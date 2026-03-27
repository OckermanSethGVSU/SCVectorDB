package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kshedden/gonpy"
	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
)

func initTracer(ctx context.Context) (func(context.Context) error, error) {
	endpoint := os.Getenv("OTLP_GRPC_ENDPOINT")
	if endpoint == "" {
		log.Fatal("OTLP_GRPC_ENDPOINT must be set")
	}

	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String("milvus-client"),
		)),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	return tp.Shutdown, nil
}

type NodeInfo struct {
	Rank int
	IP   string
	Port int
}

type SweepConfig struct {
	BatchSize  int
	ResultPath string
	Label      string
}

func getNodeByRank(filename string, targetRank int) (*NodeInfo, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.Split(line, ",")
		if len(parts) != 4 {
			continue
		}

		rank, err := strconv.Atoi(parts[0])
		if err != nil {
			continue
		}

		if rank == targetRank {
			port, err := strconv.Atoi(parts[2])
			if err != nil {
				return nil, err
			}

			return &NodeInfo{
				Rank: rank,
				IP:   parts[1],
				Port: port,
			}, nil
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return nil, fmt.Errorf("rank %d not found", targetRank)
}

// Barrier represents a reusable synchronization point for a group of goroutines.
type Barrier struct {
	n     int
	count int
	gen   int
	mu    sync.Mutex
	cond  *sync.Cond
}

func NewBarrier(n int) *Barrier {
	if n <= 0 {
		panic("barrier size must be > 0")
	}
	b := &Barrier{n: n, count: n}
	b.cond = sync.NewCond(&b.mu)
	return b
}

// Wait blocks until exactly N goroutines have called it for this generation.
// Then it releases all of them and resets for the next generation.
func (b *Barrier) Wait() {
	b.mu.Lock()
	g := b.gen
	b.count--

	if b.count == 0 {
		// Last goroutine arrives: advance generation and reset.
		b.gen++
		b.count = b.n
		b.cond.Broadcast()
		b.mu.Unlock()
		return
	}

	// Wait until generation advances (handles spurious wakeups).
	for g == b.gen {
		b.cond.Wait()
	}
	b.mu.Unlock()
}

// SharedTiming records one global window:
// loop start (first inserter enters the loop) -> searchable (global visibility reached).
type SharedTiming struct {
	loopStart      time.Time
	searchableAt   time.Time
	startOnce      sync.Once
	searchableOnce sync.Once
	searchableCh   chan struct{}
	mu             sync.Mutex
	expected       int
	ready          int
}

func NewSharedTiming(expected int) *SharedTiming {
	return &SharedTiming{
		searchableCh: make(chan struct{}),
		expected:     expected,
	}
}

func (t *SharedTiming) MarkLoopStart() {
	t.startOnce.Do(func() {
		t.loopStart = time.Now()
	})
}

func (t *SharedTiming) MarkSearchable() {
	t.searchableOnce.Do(func() {
		t.searchableAt = time.Now()
		close(t.searchableCh)
	})
}

func (t *SharedTiming) WaitSearchable() {
	<-t.searchableCh
}

func (t *SharedTiming) MarkClientReady() {
	t.mu.Lock()
	t.ready++
	reachedAll := t.ready == t.expected
	t.mu.Unlock()
	if reachedAll {
		t.MarkSearchable()
	}
}

func waitForLocalLastIDSearchable(
	ctx context.Context,
	mclient *milvusclient.Client,
	collectionName string,
	idField string,
	lastLocalID int64,
) bool {
	deadline := time.Now().Add(10 * time.Minute)
	for {
		opt := milvusclient.NewQueryOption(collectionName).
			WithIDs(column.NewColumnInt64(idField, []int64{lastLocalID})).
			WithConsistencyLevel(entity.ClStrong)
		res, err := mclient.Get(ctx, opt)
		if err == nil && res.ResultCount == 1 {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// splitRange splits [0, n) into `parts` contiguous chunks.
// Returns [start,end) for the chunk `idx`.
// Example: n=10, parts=2 => idx0 [0,5), idx1 [5,10)
func splitRange(n, parts, idx int) (start, end int) {
	if parts <= 0 {
		return 0, 0
	}
	if idx < 0 || idx >= parts {
		return 0, 0
	}
	base := n / parts
	rem := n % parts

	// First `rem` chunks get (base+1), rest get base
	if idx < rem {
		start = idx * (base + 1)
		end = start + (base + 1)
	} else {
		start = rem*(base+1) + (idx-rem)*base
		end = start + base
	}
	return start, end
}

func envEnabled(name string) bool {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return false
	}

	switch strings.ToLower(value) {
	case "0", "false", "no", "off":
		return false
	default:
		return true
	}
}

func getEnvIntDefault(defaultValue int, names ...string) int {
	for _, name := range names {
		value := strings.TrimSpace(os.Getenv(name))
		if value == "" {
			continue
		}

		parsed, err := strconv.Atoi(value)
		if err != nil || parsed <= 0 {
			log.Fatalf("invalid %s=%q", name, value)
		}
		return parsed
	}

	return defaultValue
}

func parseBatchSizes(raw string) ([]int, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, fmt.Errorf("empty batch size")
	}

	if single, err := strconv.Atoi(trimmed); err == nil {
		if single <= 0 {
			return nil, fmt.Errorf("invalid batch size %q", raw)
		}
		return []int{single}, nil
	}

	cleaned := strings.Trim(trimmed, "()[]")
	fields := strings.FieldsFunc(cleaned, func(r rune) bool {
		return r == ',' || r == ' ' || r == '\t' || r == '\n'
	})
	if len(fields) == 0 {
		return nil, fmt.Errorf("invalid batch size list %q", raw)
	}

	sizes := make([]int, 0, len(fields))
	for _, field := range fields {
		value, err := strconv.Atoi(field)
		if err != nil || value <= 0 {
			return nil, fmt.Errorf("invalid batch size entry %q in %q", field, raw)
		}
		sizes = append(sizes, value)
	}

	return sizes, nil
}

func crossedInsertMilestones(batchStart, batchEnd, interval int) []int {
	if interval <= 0 || batchEnd <= batchStart {
		return nil
	}

	first := ((batchStart / interval) + 1) * interval
	if first > batchEnd {
		return nil
	}

	milestones := make([]int, 0, 1+(batchEnd-first)/interval)
	for milestone := first; milestone <= batchEnd; milestone += interval {
		milestones = append(milestones, milestone)
	}
	return milestones
}

func clientWorker(
	wg *sync.WaitGroup,
	workerRank int,
	clientID int,
	clientsPerWorker int,
	totalRows int,
	matrix [][]float32,
	sweeps []SweepConfig,
	barriers []*Barrier,
	sharedTimings []*SharedTiming,
) {
	ctx, cancel := context.WithCancel(context.Background())

	tracing := os.Getenv("TRACING")
	tracingEnabled := strings.ToLower(tracing) == "true"
	var dialOpts []grpc.DialOption
	var span trace.Span
	if tracingEnabled {
		dialOpts = append(dialOpts, grpc.WithStatsHandler(otelgrpc.NewClientHandler()))
	}

	defer wg.Done()
	defer cancel()

	globalClientRank := workerRank*clientsPerWorker + clientID
	ldebugfEnabled := envEnabled("DEBUG")
	efSearch := getEnvIntDefault(64, "QUERY_EF_SEARCH", "EF_SEARCH")

	ACTIVE_TASK := os.Getenv("ACTIVE_TASK")
	TASK := os.Getenv("TASK")

	// We'll compute worker slice using splitRange(totalRows, nWorkers, workerRank)
	nWorkersStr := os.Getenv("NUM_PROXIES")
	nWorkers, err := strconv.Atoi(nWorkersStr)

	// ----- slice assignment: worker slice, then client slice within worker -----
	workerStart, workerEnd := splitRange(totalRows, nWorkers, workerRank)
	workerLen := workerEnd - workerStart

	clientStartOff, clientEndOff := splitRange(workerLen, clientsPerWorker, clientID)
	startIdx := workerStart + clientStartOff
	endIdx := workerStart + clientEndOff

	local := matrix[startIdx:endIdx]
	localRows := len(local)
	mcols := 0
	if localRows > 0 {
		mcols = len(local[0])
	}

	// ----- Target Milvus Proxy  -----
	balanceEnv := fmt.Sprintf("%s_BALANCE_STRATEGY", ACTIVE_TASK)
	balanceStrategy := os.Getenv(balanceEnv)
	if balanceStrategy == "" {
		log.Fatalf("invalid %s=%q", balanceEnv, balanceStrategy)
	}
	bs := strings.ToUpper(strings.TrimSpace(balanceStrategy))

	var node *NodeInfo
	var errN error

	if bs == "NONE" {
		node, errN = getNodeByRank("PROXY_registry.txt", 0)
	} else if bs == "WORKER" {
		node, errN = getNodeByRank("PROXY_registry.txt", workerRank)
	} else {
		log.Fatalf("unknown balance_strategy=%q (expected NONE or WORKER)", balanceStrategy)
	}

	if errN != nil {
		log.Fatalf("failed to get proxy node: %v", errN)
	}

	MILVUS_HOST := node.IP
	MILVUS_PORT := node.Port

	url := fmt.Sprintf("http://%s:%d", MILVUS_HOST, MILVUS_PORT)
	mclient, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address:     url,
		DialOptions: dialOpts,
	})
	if err != nil {
		log.Fatalf("failed to create Milvus client: %v", errN)
	}

	collectionName := "standalone" // TODO
	vectorField := "vector"        // TODO
	idField := "id"
	localLastID := int64(endIdx - 1)

	// sanity check ID: last ID in the whole run
	lastID := int64(totalRows - 1) // global last id
	globalOpt := milvusclient.NewQueryOption(collectionName).
		WithIDs(column.NewColumnInt64("id", []int64{lastID})).
		WithConsistencyLevel(entity.ClStrong)

	localOpt := milvusclient.NewQueryOption(collectionName).
		WithIDs(column.NewColumnInt64("id", []int64{localLastID})).
		WithConsistencyLevel(entity.ClStrong)

	if tracingEnabled && (ACTIVE_TASK == TASK) {
		tracer := otel.Tracer("milvus-client")
		ctx, span = tracer.Start(ctx, fmt.Sprintf("MilvusClient-rank-%d", globalClientRank))
		span.SetAttributes(attribute.Int("client.rank", globalClientRank))

		if globalClientRank == 0 {
			sc := span.SpanContext()
			log.Printf("client span started: trace_id=%s span_id=%s recording=%v",
				sc.TraceID().String(),
				sc.SpanID().String(),
				span.IsRecording(),
			)
		}
	}
	for sweepIdx, sweep := range sweeps {
		barrier := barriers[sweepIdx]
		sharedTiming := sharedTimings[sweepIdx]

		fmt.Printf(
			"Proxy=%d client=%d global_client=%d assigned [%d,%d) rows=%d batch=%d sweep=%s\n",
			workerRank, clientID, globalClientRank, startIdx, endIdx, localRows, sweep.BatchSize, sweep.Label,
		)

		var (
			totalDurations   []float64
			convertDurations []float64
			uploadDurations  []float64
		)

		if localRows == 0 {
			sharedTiming.MarkClientReady()
			barrier.Wait()
			barrier.Wait()
			barrier.Wait()
			sharedTiming.WaitSearchable()
			barrier.Wait()
			barrier.Wait()
			continue
		}

		// Barrier before inserting/searching for this sweep.
		barrier.Wait()

		// Preserve the existing perf marker behavior for the single-sweep case.
		if len(sweeps) == 1 && (ACTIVE_TASK == TASK) && globalClientRank == 0 {
			file, err := os.Create("./workerOut/workflow_start.txt")
			if err != nil {
				log.Fatalf("failed to create file: %v", err)
			}
			file.Close()

			time.Sleep(2 * time.Second)
		}

		barrier.Wait()

		sharedTiming.MarkLoopStart()
		startLoop := time.Now()
		for i := 0; i < localRows; i += sweep.BatchSize {
			end := i + sweep.BatchSize
			if end > localRows {
				end = localRows
			}

			startTotal := time.Now()
			batch := local[i:end]

			var milestones []int
			if ldebugfEnabled {
				if i == 0 {
					milestones = append(milestones, 0)
				}
				milestones = append(milestones, crossedInsertMilestones(i, end, 1000)...)
				for _, milestone := range milestones {
					log.Printf(
						"DEBUG: before op worker=%d client=%d global_client=%d target_proxy=%s:%d local_inserted=%d abs_row_start=%d batch_rows=%d batch_local_range=[%d,%d) sweep=%s",
						workerRank, clientID, globalClientRank, MILVUS_HOST, MILVUS_PORT, milestone, startIdx+i, len(batch), i, end, sweep.Label,
					)
				}
			}

			opCtx, opCancel := context.WithTimeout(ctx, 30*time.Minute)

			var err error
			var queryResults []milvusclient.ResultSet
			var startUpload time.Time

			if ACTIVE_TASK == "INSERT" {
				ids := make([]int64, len(batch))
				for j := range ids {
					absIdx := startIdx + i + j
					ids[j] = int64(absIdx)
				}

				startUpload = time.Now()
				_, err = mclient.Insert(
					opCtx,
					milvusclient.NewColumnBasedInsertOption(collectionName).
						WithInt64Column(idField, ids).
						WithFloatVectorColumn(vectorField, mcols, batch),
				)
			} else if ACTIVE_TASK == "QUERY" {
				vectors := make([]entity.Vector, len(batch))
				for j := range batch {
					vectors[j] = entity.FloatVector(batch[j])
				}

				searchOpt := milvusclient.NewSearchOption(collectionName, 10, vectors).
					WithANNSField(vectorField).
					WithConsistencyLevel(entity.ClBounded).
					WithSearchParam("ef", strconv.Itoa(efSearch))

				startUpload = time.Now()
				queryResults, err = mclient.Search(opCtx, searchOpt)
			} else {
				log.Fatalf("unknown ACTIVE_TASK=%s", ACTIVE_TASK)
			}

			opCancel()
			if err != nil {
				log.Fatalf("op failed worker=%d client=%d absRowStart=%d sweep=%s: %v", workerRank, clientID, startIdx+i, sweep.Label, err)
			}
			if ldebugfEnabled {
				for _, milestone := range milestones {
					log.Printf(
						"DEBUG op succeeded worker=%d client=%d global_client=%d target_proxy=%s:%d local_count=%d abs_row_start=%d batch_rows=%d batch_local_range=[%d,%d) sweep=%s",
						workerRank, clientID, globalClientRank, MILVUS_HOST, MILVUS_PORT, milestone, startIdx+i, len(batch), i, end, sweep.Label,
					)
				}
				if ACTIVE_TASK == "QUERY" && len(queryResults) > 0 {
					log.Printf(
						"DEBUG query sample worker=%d client=%d result_count=%d ids=%v scores=%v sweep=%s",
						workerRank, clientID, queryResults[0].ResultCount, queryResults[0].IDs, queryResults[0].Scores, sweep.Label,
					)
				}
			}
			endUpload := time.Now()

			convertDurations = append(convertDurations, startUpload.Sub(startTotal).Seconds())
			uploadDurations = append(uploadDurations, endUpload.Sub(startUpload).Seconds())
			totalDurations = append(totalDurations, endUpload.Sub(startTotal).Seconds())
		}
		endLoop := time.Now()
		barrier.Wait()

		sentinelID := int64(totalRows)
		if globalClientRank == 0 {
			if TASK == "INSERT" {
				vec := make([]float32, mcols)
				_, err := mclient.Insert(
					ctx,
					milvusclient.NewColumnBasedInsertOption(collectionName).
						WithInt64Column(idField, []int64{sentinelID}).
						WithFloatVectorColumn(vectorField, mcols, [][]float32{vec}),
				)
				if err != nil {
					log.Printf("sentinel insert failed: %v", err)
				}

				ok := waitForLocalLastIDSearchable(ctx, mclient, collectionName, idField, sentinelID)
				if !ok {
					log.Printf("Timed out waiting for sentinel visibility id=%d", sentinelID)
				}
			}
			sharedTiming.MarkSearchable()
		}

		sharedTiming.WaitSearchable()
		searchableAtClient := time.Now()

		if strings.ToLower(tracing) == "true" && sweepIdx == len(sweeps)-1 {
			span.End()
		}

		if len(sweeps) == 1 && ACTIVE_TASK == TASK && globalClientRank == 0 {
			file, err := os.Create("./workerOut/workflow_end.txt")
			if err != nil {
				log.Fatalf("failed to create file: %v", err)
			}
			file.Close()
		}
		barrier.Wait()

		localExists := false
		if TASK == "INSERT" && localRows > 0 {
			localRes, localErr := mclient.Get(ctx, localOpt)
			localExists = (localErr == nil && localRes.ResultCount == 1)
			if localErr != nil {
				log.Printf("Local sanity check failed worker=%d client=%d localLastID=%d: %v",
					workerRank, clientID, localLastID, localErr)
			}
		}

		globalExists := false
		if TASK == "INSERT" {
			globalRes, globalErr := mclient.Get(ctx, globalOpt)
			globalExists = (globalErr == nil && globalRes.ResultCount == 1)
			if globalErr != nil {
				log.Printf("Global sanity check failed worker=%d client=%d lastID=%d: %v",
					workerRank, clientID, lastID, globalErr)
			}
		}

		barrier.Wait()

		loopDuration := endLoop.Sub(startLoop).Seconds()
		waitDuration := searchableAtClient.Sub(endLoop).Seconds()
		clientTotalToSearchable := searchableAtClient.Sub(startLoop).Seconds()
		sharedWindow := sharedTiming.searchableAt.Sub(sharedTiming.loopStart).Seconds()

		time.Sleep(time.Second * time.Duration(globalClientRank*2))

		file, err := os.OpenFile(sweep.ResultPath+"/times.csv", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			log.Fatal(err)
		}

		writer := csv.NewWriter(file)

		if globalClientRank == 0 {
			_ = writer.Write([]string{
				"worker", "client", "global_client",
				"start_idx", "end_idx",
				"local_sanity_check", "global_sanity_check",
				"loop_duration", "wait_after_loop", "client_total_to_searchable",
				"shared_loop_start_to_searchable",
			})
		}
		_ = writer.Write([]string{
			strconv.Itoa(workerRank),
			strconv.Itoa(clientID),
			strconv.Itoa(globalClientRank),
			strconv.Itoa(startIdx),
			strconv.Itoa(endIdx),
			strconv.FormatBool(localExists),
			strconv.FormatBool(globalExists),
			strconv.FormatFloat(loopDuration, 'g', -1, 64),
			strconv.FormatFloat(waitDuration, 'g', -1, 64),
			strconv.FormatFloat(clientTotalToSearchable, 'g', -1, 64),
			strconv.FormatFloat(sharedWindow, 'g', -1, 64),
		})
		writer.Flush()
		file.Close()

		w1, _ := gonpy.NewFileWriter(fmt.Sprintf(sweep.ResultPath+"/batch_construction_times_w%d_c%d.npy", workerRank, clientID))
		_ = w1.WriteFloat64(convertDurations)

		w2, _ := gonpy.NewFileWriter(fmt.Sprintf(sweep.ResultPath+"/upload_times_w%d_c%d.npy", workerRank, clientID))
		_ = w2.WriteFloat64(uploadDurations)

		w3, _ := gonpy.NewFileWriter(fmt.Sprintf(sweep.ResultPath+"/op_times_w%d_c%d.npy", workerRank, clientID))
		_ = w3.WriteFloat64(totalDurations)
	}
}

func main() {
	nWorkersStr := os.Getenv("NUM_PROXIES")
	nWorkers, err := strconv.Atoi(nWorkersStr)
	if err != nil || nWorkers <= 0 {
		log.Fatalf("invalid NUM_PROXIES=%q", nWorkersStr)
	}

	activeTask := os.Getenv("ACTIVE_TASK")
	task := os.Getenv("TASK")

	if activeTask == "" && task == "" {
		log.Fatal("ACTIVE_TASK and TASK environment variables are not set")
	}
	if activeTask == "" {
		activeTask = task
	}

	fmt.Printf("Active task: %s (TASK=%s)\n", activeTask, task)

	clientsEnv := fmt.Sprintf("%s_CLIENTS_PER_PROXY", activeTask)
	clientsStr := os.Getenv(clientsEnv)
	clientsPerWorker, err := strconv.Atoi(clientsStr)
	if err != nil || clientsPerWorker <= 0 {
		log.Fatalf("invalid %s=%q", clientsEnv, clientsStr)
	}

	corpusEnv := fmt.Sprintf("%s_CORPUS_SIZE", activeTask)
	CORPUS_SIZE_str := os.Getenv(corpusEnv)
	CORPUS_SIZE, err := strconv.Atoi(CORPUS_SIZE_str)
	if err != nil || CORPUS_SIZE <= 0 {
		log.Fatalf("invalid %s=%q", corpusEnv, CORPUS_SIZE_str)
	}

	dataPathEnv := fmt.Sprintf("%s_DATA_FILEPATH", activeTask)
	DATA_PATH := os.Getenv(dataPathEnv)
	if DATA_PATH == "" {
		log.Fatalf("invalid %s=%q", dataPathEnv, DATA_PATH)
	}

	batchEnv := fmt.Sprintf("%s_BATCH_SIZE", activeTask)
	batchSizeStr := os.Getenv(batchEnv)
	batchSizes, err := parseBatchSizes(batchSizeStr)
	if err != nil {
		log.Fatalf("invalid %s=%q: %v", batchEnv, batchSizeStr, err)
	}

	resultPath := os.Getenv("RESULT_PATH")
	if resultPath == "" {
		log.Fatalf("invalid RESULT_PATH=%q", resultPath)
	}

	sweeps := make([]SweepConfig, 0, len(batchSizes))
	if len(batchSizes) == 1 {
		sweeps = append(sweeps, SweepConfig{
			BatchSize:  batchSizes[0],
			ResultPath: resultPath,
			Label:      fmt.Sprintf("batch_%d", batchSizes[0]),
		})
	} else {
		if strings.ToUpper(activeTask) != "QUERY" {
			log.Fatalf("%s only supports batch size lists for QUERY", batchEnv)
		}

		for idx, batchSize := range batchSizes {
			subdir := fmt.Sprintf("%s/query_batch_%d_run_%02d", resultPath, batchSize, idx)
			if err := os.MkdirAll(subdir, 0755); err != nil {
				log.Fatalf("failed to create result dir %s: %v", subdir, err)
			}
			sweeps = append(sweeps, SweepConfig{
				BatchSize:  batchSize,
				ResultPath: subdir,
				Label:      fmt.Sprintf("batch_%d_run_%02d", batchSize, idx),
			})
		}
	}

	totalClients := nWorkers * clientsPerWorker

	fmt.Printf(
		"CORPUS_SIZE=%d nProxies=%d clientsPerProxy=%d totalClients=%d DATA_FILEPATH=%s batch_sweeps=%v\n",
		CORPUS_SIZE, nWorkers, clientsPerWorker, totalClients, DATA_PATH, batchSizes,
	)

	tracing := os.Getenv("TRACING")
	if tracing == "" {
		log.Fatal("TRACING environment variable must be set")
	}

	var shutdown func(context.Context) error
	if strings.ToLower(tracing) == "true" {
		var err error
		shutdown, err = initTracer(context.Background())
		if err != nil {
			log.Fatalf("failed to init tracer: %v", err)
		}
		defer func() {
			if err := shutdown(context.Background()); err != nil {
				log.Printf("tracer shutdown failed: %v", err)
			}
		}()
	}

	r, err := gonpy.NewFileReader(DATA_PATH)
	if err != nil {
		panic(err)
	}
	shape := r.Shape
	// rows := shape[0]
	cols := shape[1]
	data, err := r.GetFloat32()
	if err != nil {
		panic(err)
	}

	// Build [][]float32 without copying
	matrix := make([][]float32, CORPUS_SIZE)
	for i := 0; i < CORPUS_SIZE; i++ {
		start := i * cols
		end := start + cols
		matrix[i] = data[start:end]
	}

	var wg sync.WaitGroup
	barriers := make([]*Barrier, len(sweeps))
	sharedTimings := make([]*SharedTiming, len(sweeps))
	for i := range sweeps {
		barriers[i] = NewBarrier(totalClients)
		sharedTimings[i] = NewSharedTiming(totalClients)
	}

	for w := 0; w < nWorkers; w++ {
		for c := 0; c < clientsPerWorker; c++ {
			wg.Add(1)
			go clientWorker(&wg, w, c, clientsPerWorker, CORPUS_SIZE, matrix, sweeps, barriers, sharedTimings)
		}
	}

	wg.Wait()
	fmt.Println("All workers finished")
}
