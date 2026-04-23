package main

import (
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
	"math/rand"
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

type role string

const (
	roleInsert role = "insert"
	roleQuery  role = "query"
)

type runMode string

const (
	modeMax  runMode = "max"
	modeRate runMode = "rate"
)

type batchConfig struct {
	fixed int
	min   int
	max   int
}

type batchPicker struct {
	cfg batchConfig
	rng *rand.Rand
}

type clientAssignment struct {
	clientID int
	startRow int // absolute row in source npy
	endRow   int // exclusive, absolute row in source npy
}

type insertLogRecord struct {
	ClientRole      role   `json:"client_role"`
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
}

type queryLogRecord struct {
	ClientRole      role        `json:"client_role"`
	ClientID        int         `json:"client_id"`
	OpIndex         int         `json:"op_index"`
	IssuedAtNS      int64       `json:"issued_at_ns"`
	CompletedAtNS   int64       `json:"completed_at_ns"`
	DurationNS      int64       `json:"duration_ns"`
	QueryStartRow   int         `json:"query_start_row"`
	QueryEndRow     int         `json:"query_end_row"`
	QueryRowIndices []int       `json:"query_row_indices"`
	ResultIDs       [][]int64   `json:"result_ids"`
	ResultScores    [][]float32 `json:"result_scores"`
	Status          string      `json:"status"`
	Error           string      `json:"error,omitempty"`
}

type queryResult struct {
	IDs    []int64
	Scores []float32
}

type shardSnapshot struct {
	Shards       int `json:"shards"`
	TotalObjects int `json:"total_objects"`
	TotalQueue   int `json:"total_queue"`
	Indexing     int `json:"indexing"`
}

type nodesResp struct {
	Nodes []struct {
		Name   string `json:"name"`
		Status string `json:"status"`
		Shards []struct {
			Class                string `json:"class"`
			Name                 string `json:"name"`
			ObjectCount          int    `json:"objectCount"`
			VectorIndexingStatus string `json:"vectorIndexingStatus"`
			VectorQueueLength    int    `json:"vectorQueueLength"`
		} `json:"shards"`
	} `json:"nodes"`
}

type GraphQLResp struct {
	Data struct {
		Get map[string][]struct {
			DocID int64 `json:"doc_id"`
			Additional struct {
				Distance float64 `json:"distance"`
			} `json:"_additional"`
		} `json:"Get"`
	} `json:"data"`
	Errors []struct {
		Message string `json:"message"`
	} `json:"errors"`
}

type batchObjectsReq struct {
	Objects []batchObject `json:"objects"`
}

type batchObject struct {
	Class      string                 `json:"class"`
	ID         string                 `json:"id,omitempty"`
	Properties map[string]interface{} `json:"properties"`
	Vector     []float32              `json:"vector"`
}

type batchObjectsResp []struct {
	ID     string `json:"id"`
	Result struct {
		Status string `json:"status"`
		Errors *struct {
			Error []struct {
				Message string `json:"message"`
			} `json:"error"`
		} `json:"errors"`
	} `json:"result"`
}

type NpyInfo struct {
	Offset int64
	N      int
	D      int
}

type config struct {
	weaviateHTTPAddr string
	className        string
	distanceMetric   string

	insertFile string
	queryFile  string
	outputDir  string

	preloadTotal            int
	preloadClients          int
	preloadBatchSize        int
	preloadDynamicThreshold int

	postPreloadDynamicThreshold int

	mixedInsertStartRow int
	mixedInsertTotal    int
	mixedInsertClients  int
	mixedQueryClients   int
	mixedInsertBatch    int
	mixedQueryBatch     int

	insertMode      runMode
	queryMode       runMode
	insertOpsPerSec float64
	queryOpsPerSec  float64

	topK       int
	ef         int
	rpcTimeout time.Duration

	insertLogFile string
	queryLogFile  string
}

var httpClient = &http.Client{
	Transport: &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:        256,
		MaxIdleConnsPerHost: 256,
		IdleConnTimeout:     90 * time.Second,
	},
	Timeout: 0,
}

var (
	insertLogMu sync.Mutex
	queryLogMu  sync.Mutex
)

func main() {
	cfg, err := parseFlags()
	if err != nil {
		log.Fatal(err)
	}

	if err := os.MkdirAll(cfg.outputDir, 0o755); err != nil {
		log.Fatalf("mkdir output: %v", err)
	}

	if err := waitForWeaviate(cfg.weaviateHTTPAddr, 300); err != nil {
		log.Fatalf("weaviate not ready: %v", err)
	}

	// Fresh collection for each run.
	if err := recreateCollection(cfg); err != nil {
		log.Fatalf("recreate collection: %v", err)
	}

	// Preload phase.
	log.Printf("[MIXED] preload start: total=%d clients=%d batch=%d threshold=%d",
		cfg.preloadTotal, cfg.preloadClients, cfg.preloadBatchSize, cfg.preloadDynamicThreshold)

	if err := runInsertPhase(
		context.Background(),
		cfg,
		cfg.insertFile,
		0,
		cfg.preloadTotal,
		cfg.preloadClients,
		cfg.preloadBatchSize,
		modeMax,
		0,
		0, // client id base
	); err != nil {
		log.Fatalf("preload phase failed: %v", err)
	}

	log.Printf("[MIXED] preload inserts complete, waiting for indexing to drain")
	if err := waitForCollectionIdle(cfg.weaviateHTTPAddr, cfg.className, 7200, 1000, 5); err != nil {
		log.Fatalf("wait for preload indexing: %v", err)
	}

	// Reset threshold to default 1e12. This does not revert HNSW back to flat if the
	// collection already converted, but it restores the config value for subsequent growth.
	if err := updateDynamicThreshold(cfg.weaviateHTTPAddr, cfg.className, cfg.postPreloadDynamicThreshold); err != nil {
		log.Fatalf("reset dynamic threshold: %v", err)
	}
	log.Printf("[MIXED] dynamic threshold reset to %d", cfg.postPreloadDynamicThreshold)

	// Mixed phase.
	log.Printf("[MIXED] mixed phase start: insert_total=%d insert_clients=%d query_clients=%d insert_batch=%d query_batch=%d insert_mode=%s insert_ops_per_sec=%.3f query_mode=%s",
		cfg.mixedInsertTotal, cfg.mixedInsertClients, cfg.mixedQueryClients,
		cfg.mixedInsertBatch, cfg.mixedQueryBatch, cfg.insertMode, cfg.insertOpsPerSec, cfg.queryMode)

	if err := runMixedPhase(context.Background(), cfg); err != nil {
		log.Fatalf("mixed phase failed: %v", err)
	}

	log.Printf("[MIXED] done")
}

func parseFlags() (config, error) {
	var cfg config
	var insertMode, queryMode string

	defaultBase := strings.TrimSpace(os.Getenv("WEAVIATE_HTTP_ADDR"))
	if defaultBase == "" {
		workerIP := strings.TrimSpace(os.Getenv("WORKER_IP"))
		restPort := strings.TrimSpace(os.Getenv("REST_PORT"))
		if workerIP != "" && restPort != "" {
			defaultBase = "http://" + workerIP + ":" + restPort
		}
	}

	flag.StringVar(&cfg.weaviateHTTPAddr, "weaviate-http-addr", defaultBase, "Weaviate HTTP base address")
	flag.StringVar(&cfg.className, "class", getenvDefault("CLASS_NAME", "PES2OMixedEF64"), "Weaviate class name")
	flag.StringVar(&cfg.distanceMetric, "distance", getenvDefault("DISTANCE_METRIC", "IP"), "Distance metric (IP/cosine/l2-squared)")
	flag.StringVar(&cfg.insertFile, "insert-file", getenvDefault("INSERT_DATA_FILEPATH", ""), "Insert matrix .npy")
	flag.StringVar(&cfg.queryFile, "query-file", getenvDefault("QUERY_DATA_FILEPATH", ""), "Query matrix .npy")
	flag.StringVar(&cfg.outputDir, "output-dir", getenvDefault("RESULT_PATH", "."), "Output dir")

	flag.IntVar(&cfg.preloadTotal, "preload-total", getenvIntDefault("PRELOAD_TOTAL", 5000000), "Rows to preload from insert file starting at row 0")
	flag.IntVar(&cfg.preloadClients, "preload-clients", getenvIntDefault("PRELOAD_CLIENTS", 16), "Preload insert clients")
	flag.IntVar(&cfg.preloadBatchSize, "preload-batch-size", getenvIntDefault("PRELOAD_BATCH_SIZE", 32), "Preload batch size")
	flag.IntVar(&cfg.preloadDynamicThreshold, "preload-dynamic-threshold", getenvIntDefault("PRELOAD_DYNAMIC_THRESHOLD", 10000), "Dynamic threshold used at collection creation")

	flag.IntVar(&cfg.postPreloadDynamicThreshold, "post-preload-dynamic-threshold", getenvIntDefault("POST_PRELOAD_DYNAMIC_THRESHOLD", 1000000000000), "Threshold to set after preload")

	flag.IntVar(&cfg.mixedInsertStartRow, "mixed-insert-start-row", getenvIntDefault("MIXED_INSERT_START_ROW", 5000000), "Start row for mixed inserts")
	flag.IntVar(&cfg.mixedInsertTotal, "mixed-insert-total", getenvIntDefault("MIXED_INSERT_CORPUS_SIZE", 5000000), "Rows to insert during mixed phase")
	flag.IntVar(&cfg.mixedInsertClients, "mixed-insert-clients", getenvIntDefault("MIXED_INSERT_CLIENTS", 1), "Insert clients in mixed phase")
	flag.IntVar(&cfg.mixedQueryClients, "mixed-query-clients", getenvIntDefault("MIXED_QUERY_CLIENTS", 1), "Query clients in mixed phase")
	flag.IntVar(&cfg.mixedInsertBatch, "mixed-insert-batch", getenvIntDefault("MIXED_INSERT_BATCH_SIZE", 32), "Insert batch size in mixed phase")
	flag.IntVar(&cfg.mixedQueryBatch, "mixed-query-batch", getenvIntDefault("MIXED_QUERY_BATCH_SIZE", 32), "Query batch size in mixed phase")

	flag.StringVar(&insertMode, "insert-mode", getenvDefault("INSERT_MODE", "max"), "Insert mode: max or rate")
	flag.StringVar(&queryMode, "query-mode", getenvDefault("QUERY_MODE", "max"), "Query mode: max or rate")
	flag.Float64Var(&cfg.insertOpsPerSec, "insert-ops-per-sec", getenvFloatDefault("INSERT_OPS_PER_SEC", 0), "Insert ops/sec across all insert clients when insert-mode=rate")
	flag.Float64Var(&cfg.queryOpsPerSec, "query-ops-per-sec", getenvFloatDefault("QUERY_OPS_PER_SEC", 0), "Query ops/sec across all query clients when query-mode=rate")

	flag.IntVar(&cfg.topK, "top-k", getenvIntDefault("TOP_K", 10), "Top-k")
	flag.IntVar(&cfg.ef, "ef", getenvIntDefault("EFSearch", 64), "Query ef")
	flag.DurationVar(&cfg.rpcTimeout, "rpc-timeout", getenvDurationDefault("RPC_TIMEOUT", 10*time.Minute), "Per-operation timeout")

	flag.StringVar(&cfg.insertLogFile, "insert-log-file", getenvDefault("INSERT_LOG_FILE", filepath.Join(getenvDefault("RESULT_PATH", "."), "insert_client.jsonl")), "Insert log file")
	flag.StringVar(&cfg.queryLogFile, "query-log-file", getenvDefault("QUERY_LOG_FILE", filepath.Join(getenvDefault("RESULT_PATH", "."), "query_client.jsonl")), "Query log file")

	flag.Parse()

	cfg.insertMode = runMode(strings.ToLower(strings.TrimSpace(insertMode)))
	cfg.queryMode = runMode(strings.ToLower(strings.TrimSpace(queryMode)))

	if cfg.weaviateHTTPAddr == "" {
		return cfg, errors.New("weaviate-http-addr is required")
	}
	if cfg.className == "" {
		return cfg, errors.New("class is required")
	}
	if cfg.insertFile == "" {
		return cfg, errors.New("insert-file is required")
	}
	if cfg.queryFile == "" {
		return cfg, errors.New("query-file is required")
	}
	if cfg.preloadTotal <= 0 || cfg.preloadClients <= 0 || cfg.preloadBatchSize <= 0 {
		return cfg, errors.New("invalid preload settings")
	}
	if cfg.mixedInsertTotal <= 0 || cfg.mixedInsertClients <= 0 || cfg.mixedQueryClients <= 0 {
		return cfg, errors.New("invalid mixed settings")
	}
	if cfg.mixedInsertBatch <= 0 || cfg.mixedQueryBatch <= 0 || cfg.topK <= 0 || cfg.ef <= 0 {
		return cfg, errors.New("invalid batch/topk/ef settings")
	}
	if cfg.rpcTimeout <= 0 {
		return cfg, errors.New("rpc-timeout must be positive")
	}
	if cfg.insertMode != modeMax && cfg.insertMode != modeRate {
		return cfg, fmt.Errorf("bad insert-mode: %q", cfg.insertMode)
	}
	if cfg.queryMode != modeMax && cfg.queryMode != modeRate {
		return cfg, fmt.Errorf("bad query-mode: %q", cfg.queryMode)
	}
	if cfg.insertMode == modeRate && cfg.insertOpsPerSec <= 0 {
		return cfg, errors.New("insert-ops-per-sec must be > 0 when insert-mode=rate")
	}
	if cfg.queryMode == modeRate && cfg.queryOpsPerSec <= 0 {
		return cfg, errors.New("query-ops-per-sec must be > 0 when query-mode=rate")
	}
	return cfg, nil
}

func recreateCollection(cfg config) error {
	_ = deleteCollection(cfg.weaviateHTTPAddr, cfg.className)

	payload := map[string]interface{}{
		"class":           cfg.className,
		"vectorizer":      "none",
		"vectorIndexType": "dynamic",
		"vectorIndexConfig": map[string]interface{}{
			"distance":  normalizeDistance(cfg.distanceMetric),
			"threshold": cfg.preloadDynamicThreshold,
			"hnsw": map[string]interface{}{
				"efConstruction": 100,
				"maxConnections": 16,
			},
			"flat": map[string]interface{}{},
		},
		"properties": []map[string]interface{}{
			{
				"name":     "doc_id",
				"dataType": []string{"int"},
			},
		},
	}

	body, code, err := httpJSON(context.Background(), "POST", cfg.weaviateHTTPAddr+"/v1/schema", payload)
	if err != nil {
		return err
	}
	if code != 200 {
		return fmt.Errorf("create collection http %d: %s", code, string(body))
	}

	return waitForClass(cfg.weaviateHTTPAddr, cfg.className, 60*time.Second)
}

func deleteCollection(base, className string) error {
	req, err := http.NewRequest("DELETE", base+"/v1/schema/"+className, nil)
	if err != nil {
		return err
	}
	res, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	_, _ = io.ReadAll(res.Body)
	// 200 or 404 are both fine here.
	if res.StatusCode != 200 && res.StatusCode != 404 && res.StatusCode != 422 {
		return fmt.Errorf("delete collection status=%d", res.StatusCode)
	}
	return nil
}

func updateDynamicThreshold(base, className string, newThreshold int) error {
	// First fetch the existing schema.
	body, code, err := httpGET(base + "/v1/schema/" + className)
	if err != nil {
		return err
	}
	if code != 200 {
		return fmt.Errorf("get schema http %d: %s", code, string(body))
	}

	var cls map[string]interface{}
	if err := json.Unmarshal(body, &cls); err != nil {
		return fmt.Errorf("unmarshal schema: %w", err)
	}

	vic, ok := cls["vectorIndexConfig"].(map[string]interface{})
	if !ok {
		return errors.New("schema missing vectorIndexConfig")
	}
	vic["threshold"] = newThreshold
	cls["vectorIndexConfig"] = vic

	// Try PUT /v1/schema/{class}
	body, code, err = httpJSON(context.Background(), "PUT", base+"/v1/schema/"+className, cls)
	if err == nil && (code == 200 || code == 204) {
		return nil
	}

	// Fallback: try PUT /v1/schema/{class}/config with only mutable config.
	minPayload := map[string]interface{}{
		"vectorIndexConfig": map[string]interface{}{
			"threshold": newThreshold,
		},
	}
	body2, code2, err2 := httpJSON(context.Background(), "PUT", base+"/v1/schema/"+className+"/config", minPayload)
	if err2 == nil && (code2 == 200 || code2 == 204) {
		return nil
	}

	return fmt.Errorf("failed to update threshold, put-class status=%d body=%s ; put-config status=%d body=%s",
		code, string(body), code2, string(body2))
}

func runMixedPhase(ctx context.Context, cfg config) error {
	insertStart := cfg.mixedInsertStartRow
	insertEnd := cfg.mixedInsertStartRow + cfg.mixedInsertTotal

	insertAssignments := buildAssignmentsAbsolute(insertStart, insertEnd, cfg.mixedInsertClients)
	queryAssignments := buildAssignmentsAbsolute(0, cfg.countQueryRows(), cfg.mixedQueryClients)

	var wg sync.WaitGroup
	var readyWG sync.WaitGroup
	errCh := make(chan error, len(insertAssignments)+len(queryAssignments))
	startCh := make(chan struct{})

	readyWG.Add(len(insertAssignments) + len(queryAssignments))

	for _, a := range insertAssignments {
		wg.Add(1)
		go func(ass clientAssignment) {
			defer wg.Done()
			if err := runInsertWorker(ctx, cfg, ass, cfg.insertFile, cfg.mixedInsertBatch, cfg.insertMode, cfg.insertOpsPerSec, 0, true, &readyWG, startCh); err != nil {
				errCh <- fmt.Errorf("mixed insert client %d: %w", ass.clientID, err)
			}
		}(a)
	}

	for _, a := range queryAssignments {
		wg.Add(1)
		go func(ass clientAssignment) {
			defer wg.Done()
			if err := runQueryWorker(ctx, cfg, ass, cfg.queryFile, cfg.mixedQueryBatch, cfg.queryMode, cfg.queryOpsPerSec, 0, &readyWG, startCh); err != nil {
				errCh <- fmt.Errorf("mixed query client %d: %w", ass.clientID, err)
			}
		}(a)
	}

	readyWG.Wait()
	close(startCh)

	wg.Wait()
	close(errCh)

	var joined error
	for err := range errCh {
		joined = errors.Join(joined, err)
	}
	return joined
}

func runInsertPhase(
	ctx context.Context,
	cfg config,
	npyPath string,
	startRow int,
	totalRows int,
	clients int,
	batchSize int,
	mode runMode,
	opsPerSec float64,
	clientIDBase int,
) error {
	assignments := buildAssignmentsAbsolute(startRow, startRow+totalRows, clients)

	var wg sync.WaitGroup
	errCh := make(chan error, len(assignments))
	startCh := make(chan struct{})
	var readyWG sync.WaitGroup
	readyWG.Add(len(assignments))

	for _, a := range assignments {
		wg.Add(1)
		go func(ass clientAssignment) {
			defer wg.Done()
			if err := runInsertWorker(ctx, cfg, clientAssignment{
				clientID: clientIDBase + ass.clientID,
				startRow: ass.startRow,
				endRow:   ass.endRow,
			}, npyPath, batchSize, mode, opsPerSec, 0, false, &readyWG, startCh); err != nil {
				errCh <- err
			}
		}(a)
	}

	readyWG.Wait()
	close(startCh)

	wg.Wait()
	close(errCh)

	var joined error
	for err := range errCh {
		joined = errors.Join(joined, err)
	}
	return joined
}

func runInsertWorker(
	ctx context.Context,
	cfg config,
	assignment clientAssignment,
	npyPath string,
	batchSize int,
	mode runMode,
	opsPerSec float64,
	idBaseOffset int64,
	logEnabled bool,
	readyWG *sync.WaitGroup,
	startCh <-chan struct{},
) error {
	f, err := os.Open(npyPath)
	if err != nil {
		readyWG.Done()
		return err
	}
	defer f.Close()

	info, err := parseNpyHeader(f, 0)
	if err != nil {
		readyWG.Done()
		return err
	}

	if assignment.startRow < 0 || assignment.endRow > info.N || assignment.startRow > assignment.endRow {
		readyWG.Done()
		return fmt.Errorf("bad assignment rows=[%d,%d) fileN=%d", assignment.startRow, assignment.endRow, info.N)
	}

	readyWG.Done()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-startCh:
	}

	picker := newBatchPicker(batchConfig{fixed: batchSize}, assignment.clientID, roleInsert)
	pacer := newPacer(mode, opsPerSec, 1) // one logical client per goroutine in this worker wrapper
	opIndex := 0

	for row := assignment.startRow; row < assignment.endRow; {
		n := picker.Next()
		if n > assignment.endRow-row {
			n = assignment.endRow - row
		}
		if n <= 0 {
			break
		}

		if err := pacer.Wait(ctx, opIndex); err != nil {
			return err
		}

		vecsFlat, got, err := readVectors(f, info, row, n)
		if err != nil {
			return err
		}
		vectors := rowsToSlices(vecsFlat, got, info.D)

		ids := make([]int64, got)
		for i := 0; i < got; i++ {
			ids[i] = int64(row+i) + idBaseOffset
		}

		opCtx, cancel := context.WithTimeout(ctx, cfg.rpcTimeout)
		issuedAt := time.Now()
		err = batchInsert(opCtx, cfg.weaviateHTTPAddr, cfg.className, ids, vectors)
		completedAt := time.Now()
		cancel()

		rec := insertLogRecord{
			ClientRole:      roleInsert,
			ClientID:        assignment.clientID,
			OpIndex:         opIndex,
			IssuedAtNS:      issuedAt.UnixNano(),
			CompletedAtNS:   completedAt.UnixNano(),
			DurationNS:      completedAt.Sub(issuedAt).Nanoseconds(),
			BatchStartRow:   row,
			BatchEndRow:     row + got,
			InsertedIDStart: ids[0],
			InsertedIDEnd:   ids[len(ids)-1] + 1,
			Status:          "ok",
		}
		if err != nil {
			rec.Status = "error"
			rec.Error = err.Error()
		}

		if logEnabled {
			if err2 := appendJSONL(cfg.insertLogFile, rec, &insertLogMu); err2 != nil {
				return err2
			}
		}

		if err != nil {
			return err
		}

		row += got
		opIndex++
	}

	return nil
}

func runQueryWorker(
	ctx context.Context,
	cfg config,
	assignment clientAssignment,
	npyPath string,
	batchSize int,
	mode runMode,
	opsPerSec float64,
	clientIDBase int,
	readyWG *sync.WaitGroup,
	startCh <-chan struct{},
) error {
	f, err := os.Open(npyPath)
	if err != nil {
		readyWG.Done()
		return err
	}
	defer f.Close()

	info, err := parseNpyHeader(f, 0)
	if err != nil {
		readyWG.Done()
		return err
	}
	if assignment.startRow < 0 || assignment.endRow > info.N || assignment.startRow > assignment.endRow {
		readyWG.Done()
		return fmt.Errorf("bad query assignment rows=[%d,%d) fileN=%d", assignment.startRow, assignment.endRow, info.N)
	}

	readyWG.Done()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-startCh:
	}

	picker := newBatchPicker(batchConfig{fixed: batchSize}, assignment.clientID, roleQuery)
	pacer := newPacer(mode, opsPerSec, 1)
	opIndex := 0
	clientID := clientIDBase + assignment.clientID

	for row := assignment.startRow; row < assignment.endRow; {
		n := picker.Next()
		if n > assignment.endRow-row {
			n = assignment.endRow - row
		}
		if n <= 0 {
			break
		}

		if err := pacer.Wait(ctx, opIndex); err != nil {
			return err
		}

		vecsFlat, got, err := readVectors(f, info, row, n)
		if err != nil {
			return err
		}

		rowIndices := make([]int, got)
		results := make([][]int64, got)
		scores := make([][]float32, got)
		for i := 0; i < got; i++ {
			rowIndices[i] = row + i
		}

		issuedAt := time.Now()
		var queryErr error

		for i := 0; i < got; i++ {
			off := i * info.D
			v := vecsFlat[off : off+info.D]
			opCtx, cancel := context.WithTimeout(ctx, cfg.rpcTimeout)
			ids, dists, err := runOneQuery(opCtx, cfg.weaviateHTTPAddr, cfg.className, v, cfg.topK, cfg.ef)
			cancel()
			if err != nil {
				queryErr = err
				break
			}
			results[i] = ids
			scores[i] = dists
		}

		completedAt := time.Now()
		rec := queryLogRecord{
			ClientRole:      roleQuery,
			ClientID:        clientID,
			OpIndex:         opIndex,
			IssuedAtNS:      issuedAt.UnixNano(),
			CompletedAtNS:   completedAt.UnixNano(),
			DurationNS:      completedAt.Sub(issuedAt).Nanoseconds(),
			QueryStartRow:   row,
			QueryEndRow:     row + got,
			QueryRowIndices: rowIndices,
			ResultIDs:       results,
			ResultScores:    scores,
			Status:          "ok",
		}
		if queryErr != nil {
			rec.Status = "error"
			rec.Error = queryErr.Error()
		}

		if err2 := appendJSONL(cfg.queryLogFile, rec, &queryLogMu); err2 != nil {
			return err2
		}
		if queryErr != nil {
			return queryErr
		}

		row += got
		opIndex++
	}

	return nil
}

func batchInsert(ctx context.Context, base, className string, ids []int64, vectors [][]float32) error {
	if len(ids) != len(vectors) {
		return fmt.Errorf("ids/vectors mismatch %d vs %d", len(ids), len(vectors))
	}
	if len(ids) == 0 {
		return nil
	}

	objects := make([]batchObject, 0, len(ids))
	for i, id := range ids {
		objects = append(objects, batchObject{
			Class: className,
			Properties: map[string]interface{}{
				"doc_id": id,
			},
			Vector: vectors[i],
		})
	}

	reqBody := batchObjectsReq{Objects: objects}
	body, code, err := httpJSON(ctx, "POST", base+"/v1/batch/objects", reqBody)
	if err != nil {
		return err
	}
	if code != 200 {
		return fmt.Errorf("batch insert http %d: %s", code, string(body))
	}

	var resp batchObjectsResp
	if err := json.Unmarshal(body, &resp); err != nil {
		return fmt.Errorf("unmarshal batch insert: %w body=%s", err, string(body))
	}
	for i, obj := range resp {
		if strings.ToUpper(strings.TrimSpace(obj.Result.Status)) != "SUCCESS" {
			msg := fmt.Sprintf("object %d status=%s", i, obj.Result.Status)
			if obj.Result.Errors != nil && len(obj.Result.Errors.Error) > 0 {
				msg += " err=" + obj.Result.Errors.Error[0].Message
			}
			return errors.New(msg)
		}
	}
	return nil
}

func runOneQuery(ctx context.Context, base, className string, vec []float32, topK, ef int) ([]int64, []float32, error) {
	query := fmt.Sprintf(
		`{"query":"{ Get { %s(nearVector:{vector:%s}, limit:%d) { doc_id _additional { distance } } } }"}`,
		className, formatVector(vec), topK,
	)

	req, err := http.NewRequestWithContext(ctx, "POST", base+"/v1/graphql", bytes.NewBufferString(query))
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Weaviate-Consistency-Level", "ONE")
	req.Header.Set("X-Weaviate-Query-Ef", strconv.Itoa(ef))

	res, err := httpClient.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer res.Body.Close()

	body, _ := io.ReadAll(res.Body)
	if res.StatusCode != 200 {
		return nil, nil, fmt.Errorf("graphql status=%d body=%s", res.StatusCode, string(body))
	}

	var r GraphQLResp
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, nil, fmt.Errorf("graphql unmarshal: %w body=%s", err, string(body))
	}
	if len(r.Errors) > 0 {
		return nil, nil, fmt.Errorf("graphql errors: %s", r.Errors[0].Message)
	}

	hits, ok := r.Data.Get[className]
	if !ok {
		return nil, nil, fmt.Errorf("graphql response missing class %q", className)
	}

	ids := make([]int64, topK)
	dists := make([]float32, topK)
	for i := 0; i < topK; i++ {
		ids[i] = -1
		dists[i] = float32(math.NaN())
	}
	for i, hit := range hits {
		if i >= topK {
			break
		}
		ids[i] = hit.DocID
		dists[i] = float32(hit.Additional.Distance)
	}
	return ids, dists, nil
}

func waitForWeaviate(base string, maxSeconds int) error {
	start := time.Now()
	var lastBody []byte
	var lastCode int
	var lastErr error
	for {
		body, code, err := httpGET(base + "/v1/meta")
		lastBody, lastCode, lastErr = body, code, err
		if err == nil && code == 200 && len(body) > 0 {
			return nil
		}
		if time.Since(start) > time.Duration(maxSeconds)*time.Second {
			return fmt.Errorf("timeout waiting for /v1/meta: code=%d err=%v body=%s", lastCode, lastErr, string(lastBody))
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func waitForClass(base, className string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	u := base + "/v1/schema/" + className
	for time.Now().Before(deadline) {
		body, code, err := httpGET(u)
		if err == nil && code == 200 && len(body) > 0 {
			return nil
		}
		time.Sleep(300 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for class %s", className)
}

func waitForCollectionIdle(base, className string, maxSeconds, pollMS, stablePolls int) error {
	start := time.Now()
	stable := 0
	var last shardSnapshot
	var lastErr error

	for {
		snap, err := getShardSnapshot(base, className)
		if err == nil {
			last = snap
			if snap.Shards > 0 && snap.TotalQueue == 0 && snap.Indexing == 0 {
				stable++
				if stable >= stablePolls {
					return nil
				}
			} else {
				stable = 0
			}
		} else {
			lastErr = err
			stable = 0
		}

		if time.Since(start) > time.Duration(maxSeconds)*time.Second {
			return fmt.Errorf("timeout waiting idle: last_snapshot=%+v last_err=%v", last, lastErr)
		}
		time.Sleep(time.Duration(pollMS) * time.Millisecond)
	}
}

func getShardSnapshot(base, className string) (shardSnapshot, error) {
	u, _ := url.Parse(base + "/v1/nodes")
	q := u.Query()
	q.Set("output", "verbose")
	q.Set("collection", className)
	u.RawQuery = q.Encode()

	body, code, err := httpGET(u.String())
	if err != nil {
		return shardSnapshot{}, err
	}
	if code != 200 {
		return shardSnapshot{}, fmt.Errorf("nodes status http %d: %s", code, string(body))
	}

	var r nodesResp
	if err := json.Unmarshal(body, &r); err != nil {
		return shardSnapshot{}, fmt.Errorf("unmarshal nodes: %w body=%s", err, string(body))
	}

	s := shardSnapshot{}
	for _, n := range r.Nodes {
		for _, sh := range n.Shards {
			if sh.Class != className {
				continue
			}
			s.Shards++
			s.TotalObjects += sh.ObjectCount
			s.TotalQueue += sh.VectorQueueLength
			if strings.ToUpper(sh.VectorIndexingStatus) == "INDEXING" {
				s.Indexing++
			}
		}
	}
	return s, nil
}

func buildAssignmentsAbsolute(start, end, clients int) []clientAssignment {
	total := end - start
	out := make([]clientAssignment, 0, clients)
	base := total / clients
	rem := total % clients
	cur := start
	for i := 0; i < clients; i++ {
		n := base
		if i < rem {
			n++
		}
		out = append(out, clientAssignment{
			clientID: i,
			startRow: cur,
			endRow:   cur + n,
		})
		cur += n
	}
	return out
}

func (cfg config) countQueryRows() int {
	f, err := os.Open(cfg.queryFile)
	if err != nil {
		log.Fatalf("open query file: %v", err)
	}
	defer f.Close()
	info, err := parseNpyHeader(f, 0)
	if err != nil {
		log.Fatalf("query npy header: %v", err)
	}
	rows := info.N
	if envRows := getenvIntDefault("QUERY_CORPUS_SIZE", rows); envRows < rows {
		rows = envRows
	}
	return rows
}

func normalizeDistance(s string) string {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "ip", "dot", "dotproduct":
		return "dot"
	case "l2", "l2-squared", "l2squared":
		return "l2-squared"
	default:
		return "cosine"
	}
}

func httpGET(u string) ([]byte, int, error) {
	req, _ := http.NewRequest("GET", u, nil)
	res, err := httpClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	return body, res.StatusCode, nil
}

func httpJSON(ctx context.Context, method, u string, payload interface{}) ([]byte, int, error) {
	var bodyReader io.Reader
	if payload != nil {
		b, err := json.Marshal(payload)
		if err != nil {
			return nil, 0, err
		}
		bodyReader = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, bodyReader)
	if err != nil {
		return nil, 0, err
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := httpClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	return body, res.StatusCode, nil
}

func parseNpyHeader(f *os.File, expectedD int) (*NpyInfo, error) {
	magic := make([]byte, 6)
	if _, err := io.ReadFull(f, magic); err != nil {
		return nil, err
	}
	if !(magic[0] == 0x93 && string(magic[1:]) == "NUMPY") {
		return nil, errors.New("bad npy magic")
	}

	ver := make([]byte, 2)
	if _, err := io.ReadFull(f, ver); err != nil {
		return nil, err
	}
	major := ver[0]

	var headerLen int
	if major == 1 {
		var u16 uint16
		if err := binary.Read(f, binary.LittleEndian, &u16); err != nil {
			return nil, err
		}
		headerLen = int(u16)
	} else {
		var u32 uint32
		if err := binary.Read(f, binary.LittleEndian, &u32); err != nil {
			return nil, err
		}
		headerLen = int(u32)
	}

	header := make([]byte, headerLen)
	if _, err := io.ReadFull(f, header); err != nil {
		return nil, err
	}
	h := string(header)

	if !strings.Contains(h, "'descr': '<f4'") && !strings.Contains(h, "\"descr\": \"<f4\"") {
		return nil, fmt.Errorf("expected <f4 float32; header=%s", h)
	}

	re := regexp.MustCompile(`\(\s*(\d+)\s*,\s*(\d+)\s*\)`)
	m := re.FindStringSubmatch(h)
	if len(m) != 3 {
		return nil, fmt.Errorf("cannot parse shape from header=%s", h)
	}

	var n, d int
	fmt.Sscanf(m[1], "%d", &n)
	fmt.Sscanf(m[2], "%d", &d)

	if expectedD > 0 && d != expectedD {
		return nil, fmt.Errorf("vector dim mismatch: got %d expected %d", d, expectedD)
	}

	off, _ := f.Seek(0, io.SeekCurrent)
	return &NpyInfo{Offset: off, N: n, D: d}, nil
}

func readVectors(f *os.File, info *NpyInfo, start, count int) ([]float32, int, error) {
	if start < 0 || start >= info.N {
		return nil, 0, fmt.Errorf("start out of range: %d (N=%d)", start, info.N)
	}
	if start+count > info.N {
		count = info.N - start
	}
	if count <= 0 {
		return nil, 0, errors.New("count <= 0")
	}

	byteOff := info.Offset + int64(start*info.D*4)
	if _, err := f.Seek(byteOff, io.SeekStart); err != nil {
		return nil, 0, err
	}

	raw := make([]byte, count*info.D*4)
	if _, err := io.ReadFull(f, raw); err != nil {
		return nil, 0, err
	}

	out := make([]float32, count*info.D)
	for i := 0; i < count*info.D; i++ {
		u := binary.LittleEndian.Uint32(raw[i*4 : i*4+4])
		out[i] = math.Float32frombits(u)
	}
	return out, count, nil
}

func rowsToSlices(flat []float32, rows, dim int) [][]float32 {
	out := make([][]float32, 0, rows)
	for i := 0; i < rows; i++ {
		off := i * dim
		out = append(out, flat[off:off+dim])
	}
	return out
}

func formatVector(v []float32) string {
	var b strings.Builder
	b.Grow(len(v) * 12)
	b.WriteString("[")
	for i, x := range v {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(fmt.Sprintf("%.8f", x))
	}
	b.WriteString("]")
	return b.String()
}

type pacer struct {
	mode          runMode
	perClientRate float64
	start         time.Time
}

func newPacer(mode runMode, totalOpsPerSec float64, clients int) *pacer {
	p := &pacer{
		mode:  mode,
		start: time.Now(),
	}
	if mode == modeRate && clients > 0 && totalOpsPerSec > 0 {
		p.perClientRate = totalOpsPerSec / float64(clients)
	}
	return p
}

func (p *pacer) Wait(ctx context.Context, opIndex int) error {
	if p.mode != modeRate || p.perClientRate <= 0 {
		return nil
	}
	targetElapsed := time.Duration(float64(opIndex)/p.perClientRate*float64(time.Second))
	targetTime := p.start.Add(targetElapsed)
	delay := time.Until(targetTime)
	if delay <= 0 {
		return nil
	}
	t := time.NewTimer(delay)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func newBatchPicker(cfg batchConfig, clientID int, r role) *batchPicker {
	seed := int64(clientID+1)*1000003 + int64(len(string(r)))*7919
	return &batchPicker{
		cfg: cfg,
		rng: rand.New(rand.NewSource(seed)),
	}
}

func (b *batchPicker) Next() int {
	if b.cfg.min > 0 && b.cfg.max > 0 {
		return b.cfg.min + b.rng.Intn(b.cfg.max-b.cfg.min+1)
	}
	return b.cfg.fixed
}

func appendJSONL(path string, row interface{}, mu *sync.Mutex) error {
	mu.Lock()
	defer mu.Unlock()

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	b, err := json.Marshal(row)
	if err != nil {
		return err
	}
	_, err = f.Write(append(b, '\n'))
	return err
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
	x, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return x
}

func getenvFloatDefault(name string, fallback float64) float64 {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return fallback
	}
	x, err := strconv.ParseFloat(v, 64)
	if err != nil {
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