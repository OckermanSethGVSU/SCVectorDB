package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

type runMode string

const (
	modeMax  runMode = "max"
	modeRate runMode = "rate"
)

type balanceStrategy string

const (
	balanceNone      balanceStrategy = "NONE"
	balancePerClient balanceStrategy = "PER_CLIENT"
	balanceWorker    balanceStrategy = "WORKER_BALANCE"
)

type config struct {
	weaviateScheme string
	weaviateHost   string
	className      string

	dataFile      string
	outputDir     string
	registryPath  string
	corpusSize    int
	startRow      int
	vectorDim     int
	batchSize     int
	insertClients int

	mode             runMode
	insertOpsPerSec  float64
	rpcTimeout       time.Duration
	waitSec          int
	overallSec       int
	drainTimeoutSec  int
	drainPollMS      int
	drainStablePolls int

	resetClass       bool
	dynamicThreshold int64
	distanceMetric   string
	balance          balanceStrategy
}

type matrixData struct {
	rows int
	dim  int
	data []float32
}

type clientAssignment struct {
	clientID int
	startRow int
	endRow   int
}

type insertLogRecord struct {
	ClientRole      string `json:"client_role"`
	ClientID        int    `json:"client_id"`
	OpIndex         int    `json:"op_index"`
	IssuedAtNS      int64  `json:"issued_at_ns"`
	CompletedAtNS   int64  `json:"completed_at_ns"`
	DurationNS      int64  `json:"duration_ns"`
	BatchStartRow   int    `json:"batch_start_row"`
	BatchEndRow     int    `json:"batch_end_row"`
	InsertedIDStart int64  `json:"inserted_id_start"`
	InsertedIDEnd   int64  `json:"inserted_id_end_exclusive"`
	Status          string `json:"status"`
	Error           string `json:"error,omitempty"`
	TargetHost      string `json:"target_host"`
}

type summaryRecord struct {
	TimestampUTC     string  `json:"timestamp_utc"`
	Class            string  `json:"class"`
	Worker           string  `json:"worker"`
	DataFile         string  `json:"data_file"`
	VecDim           int     `json:"vec_dim"`
	MeasureVecs      int     `json:"measure_vecs"`
	StartRow         int     `json:"start_row"`
	BatchSize        int     `json:"batch_size"`
	InsertClients    int     `json:"insert_clients"`
	BalanceStrategy  string  `json:"balance_strategy"`

	BaselineObjects  int64   `json:"baseline_objects"`
	ExpectedObjects  int64   `json:"expected_objects"`
	Inserted         int     `json:"inserted"`

	SendSec          float64 `json:"send_sec"`
	DrainSec         float64 `json:"drain_sec"`
	TotalSec         float64 `json:"total_sec"`

	ThroughputSendVPS  float64 `json:"throughput_send_vps"`
	ThroughputDrainVPS float64 `json:"throughput_drain_vps"`

	PostSendObjects    int64 `json:"post_send_objects"`
	PostSendQueue      int64 `json:"post_send_queue"`
	PostSendIndexing   int64 `json:"post_send_indexing_shards"`

	FinalObjects       int64 `json:"final_objects"`
	FinalQueue         int64 `json:"final_queue"`
	FinalIndexing      int64 `json:"final_indexing_shards"`

	DrainPolls         int   `json:"drain_polls"`
	StablePollsNeeded  int   `json:"stable_polls_needed"`
	DrainPollMS        int   `json:"drain_poll_ms"`

	FinalStatus      string `json:"final_status"`
	FinalError       string `json:"final_error,omitempty"`
}

type pacer struct {
	mode      runMode
	rate      float64
	startTime time.Time
}

type workerTarget struct {
	Rank int
	IP   string
	Port int
}

type batchObject struct {
	Class      string         `json:"class"`
	Properties map[string]any `json:"properties"`
	Vector     []float32      `json:"vector"`
}

type batchObjectsRequest struct {
	Objects []batchObject `json:"objects"`
}

type classRequest struct {
	Class             string           `json:"class"`
	Vectorizer        string           `json:"vectorizer"`
	VectorIndexType   string           `json:"vectorIndexType"`
	VectorIndexConfig map[string]any   `json:"vectorIndexConfig"`
	Properties        []map[string]any `json:"properties"`
}

type nodeCollectionStats struct {
	ObjectCount    int64
	VectorQueue    int64
	IndexingShards int64
}

type drainResult struct {
	Stats nodeCollectionStats
	Polls int
}

var httpClient = &http.Client{
	Transport: &http.Transport{
		Proxy: nil,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:        128,
		MaxIdleConnsPerHost: 128,
		IdleConnTimeout:     90 * time.Second,
	},
}

func main() {
	cfg, err := parseFlags()
	if err != nil {
		log.Fatal(err)
	}

	if err := os.MkdirAll(cfg.outputDir, 0o755); err != nil {
		log.Fatalf("create output dir: %v", err)
	}

	data, err := loadNPYFloat32Matrix(cfg.dataFile)
	if err != nil {
		log.Fatalf("load vectors: %v", err)
	}

	if cfg.vectorDim > 0 && data.dim != cfg.vectorDim {
		log.Fatalf("VECTOR_DIM mismatch: env=%d actual=%d", cfg.vectorDim, data.dim)
	}

	if cfg.startRow < 0 || cfg.startRow >= data.rows {
		log.Fatalf("invalid start_row=%d for rows=%d", cfg.startRow, data.rows)
	}

	availableRows := data.rows - cfg.startRow
	if cfg.corpusSize <= 0 || cfg.corpusSize > availableRows {
		cfg.corpusSize = availableRows
	}
	data = sliceMatrixRows(data, cfg.startRow, cfg.startRow+cfg.corpusSize)

	targets, err := loadWorkerTargets(cfg.registryPath)
	if err != nil {
		log.Fatalf("load worker registry: %v", err)
	}
	if len(targets) == 0 {
		log.Fatalf("no worker targets found in %s", cfg.registryPath)
	}

	coordinatorBase := fmt.Sprintf("%s://%s", cfg.weaviateScheme, cfg.weaviateHost)

	ctx := context.Background()
	if err := waitForWeaviate(ctx, coordinatorBase, time.Duration(cfg.waitSec)*time.Second); err != nil {
		log.Fatalf("wait for weaviate: %v", err)
	}

	if cfg.resetClass {
		if err := recreateClass(ctx, cfg, coordinatorBase); err != nil {
			log.Fatalf("recreate class: %v", err)
		}
	}

	baselineStats, err := fetchCollectionStats(ctx, coordinatorBase, cfg.className)
	if err != nil {
		log.Fatalf("fetch baseline stats: %v", err)
	}
	baselineObjects := baselineStats.ObjectCount
	expectedObjects := baselineObjects + int64(data.rows)

	assignments := buildAssignments(data.rows, cfg.insertClients)

	start := time.Now()
	err = runInsertClients(ctx, cfg, data, targets, assignments)
	sendSec := time.Since(start).Seconds()

	postSendStats, statsErr := fetchCollectionStats(ctx, coordinatorBase, cfg.className)
	if statsErr != nil && err == nil {
		err = fmt.Errorf("fetch post-send stats: %w", statsErr)
	}

	var drainSec float64
	var drainPolls int
	finalStats := postSendStats

	if err == nil {
		drainCtx, cancel := context.WithTimeout(ctx, time.Duration(cfg.drainTimeoutSec)*time.Second)
		defer cancel()

		drainRes, derr := waitForDrain(
			drainCtx,
			coordinatorBase,
			cfg.className,
			expectedObjects,
			time.Duration(cfg.drainPollMS)*time.Millisecond,
			cfg.drainStablePolls,
		)
		if derr != nil {
			err = derr
		} else {
			finalStats = drainRes.Stats
			drainPolls = drainRes.Polls
			drainSec = time.Since(start).Seconds()
		}
	}

	if drainSec == 0 {
		drainSec = time.Since(start).Seconds()
	}

	inserted := data.rows

	throughputSend := 0.0
	if sendSec > 0 {
		throughputSend = float64(inserted) / sendSec
	}

	throughputDrain := 0.0
	if drainSec > 0 {
		throughputDrain = float64(inserted) / drainSec
	}

	summary := summaryRecord{
		TimestampUTC:       time.Now().UTC().Format(time.RFC3339),
		Class:              cfg.className,
		Worker:             coordinatorBase,
		DataFile:           cfg.dataFile,
		VecDim:             data.dim,
		MeasureVecs:        data.rows,
		StartRow:           cfg.startRow,
		BatchSize:          cfg.batchSize,
		InsertClients:      cfg.insertClients,
		BalanceStrategy:    string(cfg.balance),

		BaselineObjects:    baselineObjects,
		ExpectedObjects:    expectedObjects,
		Inserted:           inserted,

		SendSec:            sendSec,
		DrainSec:           drainSec,
		TotalSec:           drainSec,

		ThroughputSendVPS:  throughputSend,
		ThroughputDrainVPS: throughputDrain,

		PostSendObjects:    postSendStats.ObjectCount,
		PostSendQueue:      postSendStats.VectorQueue,
		PostSendIndexing:   postSendStats.IndexingShards,

		FinalObjects:       finalStats.ObjectCount,
		FinalQueue:         finalStats.VectorQueue,
		FinalIndexing:      finalStats.IndexingShards,

		DrainPolls:         drainPolls,
		StablePollsNeeded:  cfg.drainStablePolls,
		DrainPollMS:        cfg.drainPollMS,

		FinalStatus:        "ok",
	}
	if err != nil {
		summary.FinalStatus = "error"
		summary.FinalError = err.Error()
	}

	summaryPath := filepath.Join(cfg.outputDir, "insert_summary.json")
	if werr := writeJSON(summaryPath, summary); werr != nil {
		log.Fatalf("write summary: %v", werr)
	}

	if err != nil {
		log.Fatal(err)
	}
}

func parseFlags() (config, error) {
	var cfg config
	var mode string
	var balance string

	flag.StringVar(&cfg.weaviateScheme, "weaviate-scheme", getenvDefault("WEAVIATE_SCHEME", "http"), "Weaviate scheme")
	flag.StringVar(&cfg.weaviateHost, "weaviate-host", getenvDefault("WEAVIATE_HOST", "127.0.0.1:8080"), "Weaviate host:port")
	flag.StringVar(&cfg.className, "class-name", getenvDefault("CLASS_NAME", "PES2O"), "Weaviate class name")

	flag.StringVar(&cfg.dataFile, "data-file", getenvDefault("DATA_FILEPATH", ""), "Path to .npy embeddings")
	flag.StringVar(&cfg.outputDir, "output-dir", getenvDefault("RESULT_PATH", "."), "Output directory")
	flag.StringVar(&cfg.registryPath, "registry-path", getenvDefault("WORKER_REGISTRY_PATH", "ip_registry.txt"), "Worker registry path")
	flag.IntVar(&cfg.corpusSize, "corpus-size", getenvIntDefault("CORPUS_SIZE", 0), "How many rows to insert, 0 means all from start-row")
	flag.IntVar(&cfg.startRow, "start-row", getenvIntDefault("START_ROW", 0), "Starting row offset in .npy")
	flag.IntVar(&cfg.vectorDim, "vector-dim", getenvIntDefault("VECTOR_DIM", 0), "Expected vector dimension")
	flag.IntVar(&cfg.batchSize, "batch-size", getenvIntDefault("UPLOAD_BATCH_SIZE", 32), "Insert batch size")
	flag.IntVar(&cfg.insertClients, "insert-clients", getenvIntDefault("UPLOAD_CLIENTS_PER_WORKER", 1), "Number of insert clients")

	flag.StringVar(&mode, "mode", getenvDefault("INSERT_MODE", "max"), "Insert mode: max or rate")
	flag.Float64Var(&cfg.insertOpsPerSec, "insert-ops-per-sec", getenvFloatDefault("INSERT_OPS_PER_SEC", 0), "Global insert ops/sec across all clients")
	flag.DurationVar(&cfg.rpcTimeout, "rpc-timeout", getenvDurationDefault("RPC_TIMEOUT", 30*time.Minute), "Per-RPC timeout")
	flag.IntVar(&cfg.waitSec, "wait-sec", getenvIntDefault("WAIT_SEC", 300), "Wait for Weaviate readiness")
	flag.IntVar(&cfg.overallSec, "overall-sec", getenvIntDefault("OVERALL_SEC", 25000), "Overall time budget, informational only")

	flag.IntVar(&cfg.drainTimeoutSec, "drain-timeout-sec", getenvIntDefault("DRAIN_TIMEOUT_SEC", 5000), "Timeout for post-send drain wait")
	flag.IntVar(&cfg.drainPollMS, "drain-poll-ms", getenvIntDefault("DRAIN_POLL_MS", 1000), "Poll interval in ms for drain detection")
	flag.IntVar(&cfg.drainStablePolls, "drain-stable-polls", getenvIntDefault("DRAIN_STABLE_POLLS", 3), "Required consecutive stable polls")

	flag.BoolVar(&cfg.resetClass, "reset-class", getenvBoolDefault("RESET_CLASS", true), "Delete and recreate class before inserting")
	flag.Int64Var(&cfg.dynamicThreshold, "dynamic-threshold", getenvInt64Default("DYNAMIC_THRESHOLD", 1000000000000), "Dynamic vector index threshold")
	flag.StringVar(&cfg.distanceMetric, "distance-metric", getenvDefault("DISTANCE_METRIC", "cosine"), "Distance metric")
	flag.StringVar(&balance, "balance-strategy", getenvDefault("UPLOAD_BALANCE_STRATEGY", "WORKER_BALANCE"), "NONE | PER_CLIENT | WORKER_BALANCE")

	flag.Parse()

	cfg.mode = normalizeRunMode(mode)
	cfg.balance = normalizeBalance(balance)

	if cfg.dataFile == "" {
		return config{}, errors.New("DATA_FILEPATH / --data-file is required")
	}
	if cfg.className == "" {
		return config{}, errors.New("CLASS_NAME / --class-name is required")
	}
	if cfg.batchSize <= 0 {
		return config{}, errors.New("batch-size must be positive")
	}
	if cfg.insertClients <= 0 {
		return config{}, errors.New("insert-clients must be positive")
	}
	if cfg.mode != modeMax && cfg.mode != modeRate {
		return config{}, fmt.Errorf("unsupported mode %q", cfg.mode)
	}
	if cfg.mode == modeRate && cfg.insertOpsPerSec <= 0 {
		return config{}, errors.New("insert-ops-per-sec must be positive when mode=rate")
	}
	if cfg.drainTimeoutSec <= 0 {
		return config{}, errors.New("drain-timeout-sec must be positive")
	}
	if cfg.drainPollMS <= 0 {
		return config{}, errors.New("drain-poll-ms must be positive")
	}
	if cfg.drainStablePolls <= 0 {
		return config{}, errors.New("drain-stable-polls must be positive")
	}

	cfg.distanceMetric = normalizeDistance(cfg.distanceMetric)
	return cfg, nil
}

func normalizeRunMode(v string) runMode {
	return runMode(strings.ToLower(strings.TrimSpace(v)))
}

func normalizeBalance(v string) balanceStrategy {
	switch strings.ToUpper(strings.TrimSpace(v)) {
	case "", "NONE":
		return balanceNone
	case "PER_CLIENT":
		return balancePerClient
	case "WORKER", "WORKER_BALANCE":
		return balanceWorker
	default:
		return balanceStrategy(strings.ToUpper(strings.TrimSpace(v)))
	}
}

func normalizeDistance(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "cosine":
		return "cosine"
	case "dot":
		return "dot"
	case "l2-squared", "l2":
		return "l2-squared"
	default:
		return strings.ToLower(strings.TrimSpace(v))
	}
}

func runInsertClients(ctx context.Context, cfg config, data matrixData, targets []workerTarget, assignments []clientAssignment) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	errCh := make(chan error, len(assignments))

	for _, assignment := range assignments {
		wg.Add(1)
		go func(a clientAssignment) {
			defer wg.Done()
			if err := runInsertClient(runCtx, cfg, data, targets, a); err != nil {
				errCh <- fmt.Errorf("insert client %d: %w", a.clientID, err)
				cancel()
			}
		}(assignment)
	}

	wg.Wait()
	close(errCh)

	var joined error
	for err := range errCh {
		joined = errors.Join(joined, err)
	}
	return joined
}

func runInsertClient(ctx context.Context, cfg config, data matrixData, targets []workerTarget, assignment clientAssignment) error {
	target, err := pickTarget(cfg, targets, assignment.clientID)
	if err != nil {
		return err
	}
	baseURL := fmt.Sprintf("%s://%s:%d", cfg.weaviateScheme, target.IP, target.Port)
	log.Printf("insert client %d -> %s rows=[%d,%d)", assignment.clientID, baseURL, assignment.startRow, assignment.endRow)

	p := newPacer(cfg)
	logPath := filepath.Join(cfg.outputDir, fmt.Sprintf("insert_client_%03d.jsonl", assignment.clientID))
	records := make([]insertLogRecord, 0, max(1, assignment.endRow-assignment.startRow))

	opIndex := 0
	for row := assignment.startRow; row < assignment.endRow; {
		batchSize := cfg.batchSize
		if batchSize > assignment.endRow-row {
			batchSize = assignment.endRow - row
		}

		if err := p.Wait(ctx, opIndex); err != nil {
			return err
		}

		objects := make([]batchObject, 0, batchSize)
		insertedIDStart := int64(cfg.startRow + row)
		insertedIDEndExclusive := int64(cfg.startRow + row + batchSize)

		for i := 0; i < batchSize; i++ {
			globalRow := row + i
			docID := int64(cfg.startRow + globalRow)
			objects = append(objects, batchObject{
				Class: cfg.className,
				Properties: map[string]any{
					"doc_id": docID,
				},
				Vector: rowSlice(data, globalRow),
			})
		}

		reqBody := batchObjectsRequest{Objects: objects}
		opCtx, cancel := context.WithTimeout(ctx, cfg.rpcTimeout)
		issuedAt := time.Now()
		err := postJSON(opCtx, baseURL+"/v1/batch/objects", reqBody)
		completedAt := time.Now()
		cancel()

		record := insertLogRecord{
			ClientRole:      "insert",
			ClientID:        assignment.clientID,
			OpIndex:         opIndex,
			IssuedAtNS:      issuedAt.UnixNano(),
			CompletedAtNS:   completedAt.UnixNano(),
			DurationNS:      completedAt.Sub(issuedAt).Nanoseconds(),
			BatchStartRow:   cfg.startRow + row,
			BatchEndRow:     cfg.startRow + row + batchSize,
			InsertedIDStart: insertedIDStart,
			InsertedIDEnd:   insertedIDEndExclusive,
			Status:          "ok",
			TargetHost:      fmt.Sprintf("%s:%d", target.IP, target.Port),
		}
		if err != nil {
			record.Status = "error"
			record.Error = err.Error()
		}

		records = append(records, record)
		if err != nil {
			_ = writeJSONL(logPath, records)
			return err
		}

		row += batchSize
		opIndex++
	}

	if err := writeJSONL(logPath, records); err != nil {
		return err
	}
	return nil
}

func newPacer(cfg config) *pacer {
	p := &pacer{mode: cfg.mode, startTime: time.Now()}
	if cfg.mode == modeRate && cfg.insertClients > 0 {
		p.rate = cfg.insertOpsPerSec / float64(cfg.insertClients)
	}
	return p
}

func (p *pacer) Wait(ctx context.Context, opIndex int) error {
	if p.mode != modeRate || p.rate <= 0 {
		return nil
	}
	target := p.startTime.Add(time.Duration(float64(opIndex)/p.rate*float64(time.Second)))
	delay := time.Until(target)
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func pickTarget(cfg config, targets []workerTarget, clientID int) (workerTarget, error) {
	if len(targets) == 0 {
		return workerTarget{}, errors.New("no worker targets available")
	}
	switch cfg.balance {
	case balanceNone:
		return targets[0], nil
	case balancePerClient, balanceWorker:
		return targets[clientID%len(targets)], nil
	default:
		return workerTarget{}, fmt.Errorf("unsupported balance strategy %q", cfg.balance)
	}
}

func loadWorkerTargets(path string) ([]workerTarget, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var targets []workerTarget
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.Split(line, ",")
		if len(parts) < 4 {
			continue
		}

		rank, err := strconv.Atoi(strings.TrimSpace(parts[0]))
		if err != nil {
			continue
		}
		ip := strings.TrimSpace(parts[2])
		port, err := strconv.Atoi(strings.TrimSpace(parts[3]))
		if err != nil {
			continue
		}

		targets = append(targets, workerTarget{
			Rank: rank,
			IP:   ip,
			Port: port,
		})
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return targets, nil
}

func waitForWeaviate(ctx context.Context, baseURL string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/v1/meta", nil)
		resp, err := httpClient.Do(req)
		if err == nil && resp != nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return nil
			}
		}
		if time.Now().After(deadline) {
			if err != nil {
				return fmt.Errorf("meta endpoint not ready before timeout: %w", err)
			}
			return fmt.Errorf("meta endpoint not ready before timeout: status %v", resp.Status)
		}
		time.Sleep(2 * time.Second)
	}
}

func recreateClass(ctx context.Context, cfg config, baseURL string) error {
	_ = deleteClass(ctx, baseURL, cfg.className)

	if err := createClass(ctx, cfg, baseURL); err != nil {
		return err
	}
	return waitForClass(ctx, baseURL, cfg.className, 60*time.Second)
}

func deleteClass(ctx context.Context, baseURL, className string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, baseURL+"/v1/schema/"+className, nil)
	if err != nil {
		return err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusOK {
		return nil
	}
	return fmt.Errorf("delete class %s failed: %s", className, resp.Status)
}

func createClass(ctx context.Context, cfg config, baseURL string) error {
	body := classRequest{
		Class:           cfg.className,
		Vectorizer:      "none",
		VectorIndexType: "dynamic",
		VectorIndexConfig: map[string]any{
			"distance":  cfg.distanceMetric,
			"threshold": cfg.dynamicThreshold,
			"hnsw": map[string]any{
				"efConstruction": 100,
				"maxConnections": 16,
			},
		},
		Properties: []map[string]any{
			{
				"name":     "doc_id",
				"dataType": []string{"int"},
			},
		},
	}
	return postJSON(ctx, baseURL+"/v1/schema", body)
}

func waitForClass(ctx context.Context, baseURL, className string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/v1/schema/"+className, nil)
		resp, err := httpClient.Do(req)
		if err == nil && resp != nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return nil
			}
		}
		if time.Now().After(deadline) {
			if err != nil {
				return fmt.Errorf("class %s did not appear: %w", className, err)
			}
			return fmt.Errorf("class %s did not appear before timeout", className)
		}
		time.Sleep(1 * time.Second)
	}
}

func waitForDrain(
	ctx context.Context,
	baseURL string,
	className string,
	expectedObjects int64,
	pollInterval time.Duration,
	stablePolls int,
) (drainResult, error) {
	var stable int
	var polls int
	var last nodeCollectionStats

	for {
		select {
		case <-ctx.Done():
			return drainResult{}, fmt.Errorf(
				"drain wait timeout/cancelled: last_objects=%d expected=%d last_queue=%d last_indexing=%d: %w",
				last.ObjectCount, expectedObjects, last.VectorQueue, last.IndexingShards, ctx.Err(),
			)
		default:
		}

		stats, err := fetchCollectionStats(ctx, baseURL, className)
		if err != nil {
			return drainResult{}, err
		}
		last = stats
		polls++

		log.Printf(
			"[drain] poll=%d objects=%d expected=%d queue=%d indexing=%d stable=%d/%d",
			polls, stats.ObjectCount, expectedObjects, stats.VectorQueue, stats.IndexingShards, stable, stablePolls,
		)

		ready := stats.ObjectCount >= expectedObjects &&
			stats.VectorQueue == 0 &&
			stats.IndexingShards == 0

		if ready {
			stable++
			if stable >= stablePolls {
				return drainResult{
					Stats: stats,
					Polls: polls,
				}, nil
			}
		} else {
			stable = 0
		}

		timer := time.NewTimer(pollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return drainResult{}, fmt.Errorf(
				"drain wait timeout/cancelled: last_objects=%d expected=%d last_queue=%d last_indexing=%d: %w",
				last.ObjectCount, expectedObjects, last.VectorQueue, last.IndexingShards, ctx.Err(),
			)
		case <-timer.C:
		}
	}
}

func fetchCollectionStats(ctx context.Context, baseURL, className string) (nodeCollectionStats, error) {
	u, err := url.Parse(baseURL + "/v1/nodes")
	if err != nil {
		return nodeCollectionStats{}, err
	}
	q := u.Query()
	q.Set("output", "verbose")
	q.Set("collection", className)
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nodeCollectionStats{}, err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nodeCollectionStats{}, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nodeCollectionStats{}, fmt.Errorf("GET %s failed: %s: %s", u.String(), resp.Status, strings.TrimSpace(string(body)))
	}

	var payload any
	if err := json.Unmarshal(body, &payload); err != nil {
		return nodeCollectionStats{}, fmt.Errorf("decode nodes response: %w", err)
	}

	stats, ok := extractCollectionStats(payload, className)
	if !ok {
		return nodeCollectionStats{}, fmt.Errorf("could not find collection stats for class %q in /v1/nodes response", className)
	}
	return stats, nil
}

func extractCollectionStats(payload any, className string) (nodeCollectionStats, bool) {
	var stats nodeCollectionStats
	found := false
	target := strings.ToLower(className)

	var walk func(v any, inTargetCollection bool)
	walk = func(v any, inTargetCollection bool) {
		switch x := v.(type) {
		case map[string]any:
			nextInTarget := inTargetCollection

			for k, val := range x {
				lk := strings.ToLower(k)

				if lk == "name" || lk == "class" || lk == "collection" {
					if s, ok := val.(string); ok && strings.ToLower(s) == target {
						nextInTarget = true
						found = true
					}
				}
			}

			if nextInTarget {
				for k, val := range x {
					lk := strings.ToLower(k)

					switch lk {
					case "objectcount":
						stats.ObjectCount += toInt64(val)
					case "vectorqueuelength":
						stats.VectorQueue += toInt64(val)
					case "vectorindexingstatus":
						if isIndexingStatus(val) {
							stats.IndexingShards++
						}
					}
				}
			}

			for _, val := range x {
				walk(val, nextInTarget)
			}

		case []any:
			for _, elem := range x {
				walk(elem, inTargetCollection)
			}
		}
	}

	walk(payload, false)
	return stats, found
}

func isIndexingStatus(v any) bool {
	s, ok := v.(string)
	if !ok {
		return false
	}
	s = strings.TrimSpace(strings.ToUpper(s))
	return s == "INDEXING"
}

func toInt64(v any) int64 {
	switch x := v.(type) {
	case float64:
		return int64(x)
	case float32:
		return int64(x)
	case int:
		return int64(x)
	case int64:
		return x
	case int32:
		return int64(x)
	case int16:
		return int64(x)
	case int8:
		return int64(x)
	case uint:
		return int64(x)
	case uint64:
		return int64(x)
	case uint32:
		return int64(x)
	case uint16:
		return int64(x)
	case uint8:
		return int64(x)
	case json.Number:
		n, _ := x.Int64()
		return n
	case string:
		n, _ := strconv.ParseInt(strings.TrimSpace(x), 10, 64)
		return n
	default:
		return 0
	}
}

func postJSON(ctx context.Context, url string, body any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("POST %s failed: %s: %s", url, resp.Status, strings.TrimSpace(string(respBody)))
	}
	return nil
}

func buildAssignments(totalRows, clients int) []clientAssignment {
	assignments := make([]clientAssignment, 0, clients)
	for i := 0; i < clients; i++ {
		start, end := splitRange(totalRows, clients, i)
		assignments = append(assignments, clientAssignment{
			clientID: i,
			startRow: start,
			endRow:   end,
		})
	}
	return assignments
}

func splitRange(total, parts, idx int) (int, int) {
	base := total / parts
	rem := total % parts
	if idx < rem {
		start := idx * (base + 1)
		return start, start + base + 1
	}
	start := rem*(base+1) + (idx-rem)*base
	return start, start + base
}

func rowSlice(data matrixData, row int) []float32 {
	offset := row * data.dim
	return data.data[offset : offset+data.dim]
}

func sliceMatrixRows(data matrixData, start, end int) matrixData {
	if start == 0 && end == data.rows {
		return data
	}
	return matrixData{
		rows: end - start,
		dim:  data.dim,
		data: data.data[start*data.dim : end*data.dim],
	}
}

func writeJSONL(path string, records []insertLogRecord) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	for _, rec := range records {
		if err := enc.Encode(rec); err != nil {
			return err
		}
	}
	return nil
}

func writeJSON(path string, value any) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(value)
}

func loadNPYFloat32Matrix(path string) (matrixData, error) {
	f, err := os.Open(path)
	if err != nil {
		return matrixData{}, err
	}
	defer f.Close()

	magic := make([]byte, 6)
	if _, err := io.ReadFull(f, magic); err != nil {
		return matrixData{}, err
	}
	if !bytes.Equal(magic, []byte("\x93NUMPY")) {
		return matrixData{}, fmt.Errorf("%s is not a .npy file", path)
	}

	ver := make([]byte, 2)
	if _, err := io.ReadFull(f, ver); err != nil {
		return matrixData{}, err
	}

	var headerLen int
	switch ver[0] {
	case 1:
		var n uint16
		if err := binary.Read(f, binary.LittleEndian, &n); err != nil {
			return matrixData{}, err
		}
		headerLen = int(n)
	case 2, 3:
		var n uint32
		if err := binary.Read(f, binary.LittleEndian, &n); err != nil {
			return matrixData{}, err
		}
		headerLen = int(n)
	default:
		return matrixData{}, fmt.Errorf("unsupported npy version %d.%d", ver[0], ver[1])
	}

	headerBytes := make([]byte, headerLen)
	if _, err := io.ReadFull(f, headerBytes); err != nil {
		return matrixData{}, err
	}
	header := string(headerBytes)

	if !strings.Contains(header, "'fortran_order': False") && !strings.Contains(header, "\"fortran_order\": False") {
		return matrixData{}, errors.New("fortran-order arrays are not supported")
	}

	if !strings.Contains(header, "'descr': '<f4'") &&
		!strings.Contains(header, "\"descr\": \"<f4\"") &&
		!strings.Contains(header, "'descr': '|f4'") &&
		!strings.Contains(header, "\"descr\": \"|f4\"") {
		return matrixData{}, fmt.Errorf("only float32 npy arrays are supported, header=%q", header)
	}

	re := regexp.MustCompile(`[\(\[]\s*(\d+)\s*,\s*(\d+)\s*[,)\]]`)
	m := re.FindStringSubmatch(header)
	if len(m) != 3 {
		return matrixData{}, fmt.Errorf("could not parse 2D shape from header %q", header)
	}

	rows, err := strconv.Atoi(m[1])
	if err != nil {
		return matrixData{}, err
	}
	dim, err := strconv.Atoi(m[2])
	if err != nil {
		return matrixData{}, err
	}

	count := rows * dim
	data := make([]float32, count)
	if err := binary.Read(f, binary.LittleEndian, data); err != nil {
		return matrixData{}, err
	}

	return matrixData{
		rows: rows,
		dim:  dim,
		data: data,
	}, nil
}

func getenvDefault(name, fallback string) string {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return fallback
	}
	return v
}

func getenvIntDefault(name string, fallback int) int {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func getenvInt64Default(name string, fallback int64) int64 {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return fallback
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return fallback
	}
	return n
}

func getenvFloatDefault(name string, fallback float64) float64 {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return fallback
	}
	x, err := strconv.ParseFloat(v, 64)
	if err != nil || math.IsNaN(x) || math.IsInf(x, 0) {
		return fallback
	}
	return x
}

func getenvDurationDefault(name string, fallback time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}

func getenvBoolDefault(name string, fallback bool) bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(name)))
	if v == "" {
		return fallback
	}
	switch v {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return fallback
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}