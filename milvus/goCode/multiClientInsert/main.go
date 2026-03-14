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
	barrier *Barrier,
	sharedTiming *SharedTiming,
	workerRank int,
	clientID int,
	clientsPerWorker int,
	totalRows int,
	matrix [][]float32,
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

	// ----- slice assignment: worker slice, then client slice within worker -----
	wStart, wEnd := splitRange(totalRows, workerRank+1, workerRank)
	_ = wStart
	_ = wEnd

	task := os.Getenv("TASK")

	// We'll compute worker slice using splitRange(totalRows, nWorkers, workerRank)
	nWorkersStr := os.Getenv("NUM_PROXIES")
	nWorkers, err := strconv.Atoi(nWorkersStr)

	RESULT_PATH := os.Getenv("RESULT_PATH")
	if RESULT_PATH == "" {
		log.Fatalf("invalid RESULT_PATH=%q", RESULT_PATH)
	}

	workerStart, workerEnd := splitRange(totalRows, nWorkers, workerRank)
	workerLen := workerEnd - workerStart

	clientStartOff, clientEndOff := splitRange(workerLen, clientsPerWorker, clientID)
	startIdx := workerStart + clientStartOff
	endIdx := workerStart + clientEndOff

	local := matrix[startIdx:endIdx]
	localRows := len(local)
	if localRows == 0 {
		sharedTiming.MarkClientReady()
		// Still participate in barriers to avoid deadlock.
		sharedTiming.WaitSearchable()
		barrier.Wait()
		barrier.Wait()
		barrier.Wait()
		return
	}

	mcols := len(local[0])

	// ----- Target Milvus Proxy  -----
	balance_strategy := os.Getenv("UPLOAD_BALANCE_STRATEGY")
	if balance_strategy == "" {
		log.Fatalf("invalid UPLOAD_BALANCE_STRATEGY=%q", balance_strategy)
	}
	bs := strings.ToUpper(strings.TrimSpace(balance_strategy))

	var node *NodeInfo
	var errN error

	if bs == "NONE" {
		node, errN = getNodeByRank("PROXY_registry.txt", 0)
	} else if bs == "WORKER" {
		node, errN = getNodeByRank("PROXY_registry.txt", workerRank)
	} else {
		log.Fatalf("unknown balance_strategy=%q (expected NONE or WORKER)", balance_strategy)
	}

	if errN != nil {
		log.Fatalf("failed to get proxy node: %v", errN)
	}

	MILVUS_HOST := node.IP
	MILVUS_PORT := node.Port

	batchSizeStr := os.Getenv("UPLOAD_BATCH_SIZE")
	BATCH_SIZE, err := strconv.Atoi(batchSizeStr)
	if err != nil || BATCH_SIZE <= 0 {
		log.Fatalf("invalid UPLOAD_BATCH_SIZE=%q", batchSizeStr)
	}

	
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

	fmt.Printf(
		"Proxy=%d client=%d global_client=%d assigned [%d,%d) rows=%d batch=%d\n",
		workerRank, clientID, globalClientRank, startIdx, endIdx, localRows, BATCH_SIZE,
	)

	var (
		totalDurations   []float64
		convertDurations []float64
		uploadDurations  []float64
	)

	if tracingEnabled {
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
	// Barrier before inserting
	barrier.Wait()

	// let perf know that it should start tracking
	if task == "insert" && globalClientRank == 0 {
		file, err := os.Create("./workerOut/workflow_start.txt")
		if err != nil {
			log.Fatalf("failed to create file: %v", err)
		}
		file.Close()

		time.Sleep(2 * time.Second) // give perf a momement to attach
	}

	barrier.Wait()

	
	sharedTiming.MarkLoopStart()
	startLoop := time.Now()
	for i := 0; i < localRows; i += BATCH_SIZE {
		end := i + BATCH_SIZE
		if end > localRows {
			end = localRows
		}

		startTotal := time.Now()
		batch := local[i:end]

		ids := make([]int64, len(batch))
		for j := range ids {
			absIdx := startIdx + i + j
			ids[j] = int64(absIdx)
		}

		startUpload := time.Now()
		var milestones []int
		if ldebugfEnabled {
			if i == 0 {
				milestones = append(milestones, 0)
			}
			milestones = append(milestones, crossedInsertMilestones(i, end, 1000)...)
			for _, milestone := range milestones {
				log.Printf(
					"DEBUG: before insert worker=%d client=%d global_client=%d target_proxy=%s:%d local_inserted=%d abs_row_start=%d batch_rows=%d batch_local_range=[%d,%d)",
					workerRank, clientID, globalClientRank, MILVUS_HOST, MILVUS_PORT, milestone, startIdx+i, len(batch), i, end,
				)
			}
		}

		// make sure RPC does not time out under load
		insertCtx, insertCancel := context.WithTimeout(ctx, 30*time.Minute)
		_, err := mclient.Insert(
			insertCtx,
			milvusclient.NewColumnBasedInsertOption(collectionName).
				WithInt64Column(idField, ids).
				WithFloatVectorColumn(vectorField, mcols, batch),
		)
		insertCancel()
		if err != nil {
			log.Fatalf("insert failed worker=%d client=%d absRowStart=%d: %v", workerRank, clientID, startIdx+i, err)
		}
		if ldebugfEnabled {
			for _, milestone := range milestones {
				log.Printf(
					"DEBUG insert succeeded worker=%d client=%d global_client=%d target_proxy=%s:%d local_inserted=%d abs_row_start=%d batch_rows=%d batch_local_range=[%d,%d)",
					workerRank, clientID, globalClientRank, MILVUS_HOST, MILVUS_PORT, milestone, startIdx+i, len(batch), i, end,
				)
			}
		}
		endUpload := time.Now()

		convertDurations = append(convertDurations, startUpload.Sub(startTotal).Seconds())
		uploadDurations = append(uploadDurations, endUpload.Sub(startUpload).Seconds())
		totalDurations = append(totalDurations, endUpload.Sub(startTotal).Seconds())
	}
	endLoop := time.Now()
	// Wait for everyone to finish inserting
	barrier.Wait()

	if tracingEnabled {
		span.End()
	}
	// Insert a final value so we can measure when it has been processed
	sentinelID := int64(totalRows) // unique
	if globalClientRank == 0 {

		if task == "insert" {
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

	// end tracing
	if strings.ToLower(tracing) == "true" {
		span.End()
	}

	// tell perf to stop tracking
	if task == "insert" && globalClientRank == 0 {
		file, err := os.Create("./workerOut/workflow_end.txt")
		if err != nil {
			log.Fatalf("failed to create file: %v", err)
		}
		file.Close()
	}
	barrier.Wait()

	// local sanity (skip if no rows)
	localExists := false
	if localRows > 0 {
		localRes, localErr := mclient.Get(ctx, localOpt)
		localExists = (localErr == nil && localRes.ResultCount == 1)
		if localErr != nil {
			log.Printf("Local sanity check failed worker=%d client=%d localLastID=%d: %v",
				workerRank, clientID, localLastID, localErr)
		}
	} else {
		localExists = false
	}

	// global sanity (spot check)
	globalRes, globalErr := mclient.Get(ctx, globalOpt)
	globalExists := (globalErr == nil && globalRes.ResultCount == 1)
	if globalErr != nil {
		log.Printf("Global sanity check failed worker=%d client=%d lastID=%d: %v",
			workerRank, clientID, lastID, globalErr)
	}

	barrier.Wait()

	loopDuration := endLoop.Sub(startLoop).Seconds()
	waitDuration := searchableAtClient.Sub(endLoop).Seconds()
	clientTotalToSearchable := searchableAtClient.Sub(startLoop).Seconds()
	sharedWindow := sharedTiming.searchableAt.Sub(sharedTiming.loopStart).Seconds()

	// stagger file writes a bit
	time.Sleep(time.Second * time.Duration(globalClientRank*2))

	file, err := os.OpenFile(RESULT_PATH+"/times.csv", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

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

	w1, _ := gonpy.NewFileWriter(fmt.Sprintf(RESULT_PATH+"/batch_construction_times_w%d_c%d.npy", workerRank, clientID))
	_ = w1.WriteFloat64(convertDurations)

	w2, _ := gonpy.NewFileWriter(fmt.Sprintf(RESULT_PATH+"/upload_times_w%d_c%d.npy", workerRank, clientID))
	_ = w2.WriteFloat64(uploadDurations)

	w3, _ := gonpy.NewFileWriter(fmt.Sprintf(RESULT_PATH+"/op_times_w%d_c%d.npy", workerRank, clientID))
	_ = w3.WriteFloat64(totalDurations)
}

func main() {
	nWorkersStr := os.Getenv("NUM_PROXIES")
	nWorkers, err := strconv.Atoi(nWorkersStr)
	if err != nil || nWorkers <= 0 {
		log.Fatalf("invalid NUM_PROXIES=%q", nWorkersStr)
	}

	clientsStr := os.Getenv("UPLOAD_CLIENTS_PER_PROXY")
	clientsPerWorker, err := strconv.Atoi(clientsStr)
	if err != nil || clientsPerWorker <= 0 {
		log.Fatalf("invalid UPLOAD_CLIENTS_PER_PROXY=%q", clientsStr)
	}
	CORPUS_SIZE_str := os.Getenv("CORPUS_SIZE")
	CORPUS_SIZE, err := strconv.Atoi(CORPUS_SIZE_str)
	if err != nil || CORPUS_SIZE <= 0 {
		log.Fatalf("invalid CORPUS_SIZE=%q", CORPUS_SIZE_str)
	}

	DATA_PATH := os.Getenv("DATA_FILEPATH")
	if DATA_PATH == "" {
		log.Fatalf("invalid DATA_FILEPATH=%q", DATA_PATH)
	}

	batchSizeStr := os.Getenv("UPLOAD_BATCH_SIZE")
	BATCH_SIZE, err := strconv.Atoi(batchSizeStr)
	if err != nil || BATCH_SIZE <= 0 {
		log.Fatalf("invalid UPLOAD_BATCH_SIZE=%q", batchSizeStr)
	}

	totalClients := nWorkers * clientsPerWorker
	fmt.Printf("CORPUS_SIZE=%d nProxies=%d clientsPerProxy=%d totalClients=%d DATA_FILEPATH=%s\n",
		CORPUS_SIZE, nWorkers, clientsPerWorker, totalClients, DATA_PATH,
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
	barrier := NewBarrier(totalClients)
	sharedTiming := NewSharedTiming(totalClients)

	for w := 0; w < nWorkers; w++ {
		for c := 0; c < clientsPerWorker; c++ {
			wg.Add(1)
			go clientWorker(&wg, barrier, sharedTiming, w, c, clientsPerWorker, CORPUS_SIZE, matrix)
		}
	}

	wg.Wait()
	fmt.Println("All workers finished")
}
