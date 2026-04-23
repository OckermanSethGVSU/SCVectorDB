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
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type config struct {
	weaviateScheme string
	weaviateHost   string
	className      string

	queryFile      string
	outputDir      string
	startRow       int
	queryWorkload  int
	vectorDim      int
	queryBatchSize int
	topK           int
	queryEF        int

	rpcTimeout time.Duration
	waitSec    int
	overallSec int
}

type matrixData struct {
	rows int
	dim  int
	data []float32
}

type queryLogRecord struct {
	ClientRole      string      `json:"client_role"`
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

type summaryRecord struct {
	TimestampUTC   string    `json:"timestamp_utc"`
	Worker         string    `json:"worker"`
	Class          string    `json:"class"`
	QueryFile      string    `json:"query_file"`
	VecDim         int       `json:"vec_dim"`
	QueryBatchSize int       `json:"query_batch_size"`
	MeasureQueries int       `json:"measure_queries"`
	StartRow       int       `json:"start_row"`
	TopK           int       `json:"top_k"`
	QueryEF        int       `json:"query_ef"`
	WaitSec        int       `json:"wait_sec"`
	OverallSec     int       `json:"overall_sec"`
	RPCSec         float64   `json:"rpc_sec"`

	QueriesCompleted int       `json:"queries_completed"`
	TotalTimeSec     float64   `json:"total_time_sec"`
	MeanLatencySec   float64   `json:"mean_latency_sec"`
	ThroughputQPS    float64   `json:"throughput_qps"`
	BatchLatencies   []float64 `json:"batch_latencies"`

	FinalStatus string `json:"final_status"`
	FinalError  string `json:"final_error,omitempty"`
}

type graphqlRequest struct {
	Query string `json:"query"`
}

type gqlResp struct {
	Data   map[string]map[string][]map[string]any `json:"data"`
	Errors []map[string]any                       `json:"errors,omitempty"`
}

var httpClient = &http.Client{
	Transport: &http.Transport{
		Proxy: nil,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:        256,
		MaxIdleConnsPerHost: 256,
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

	queries, err := loadNPYFloat32Matrix(cfg.queryFile)
	if err != nil {
		log.Fatalf("load queries: %v", err)
	}
	if cfg.vectorDim > 0 && queries.dim != cfg.vectorDim {
		log.Fatalf("VECTOR_DIM mismatch: env=%d actual=%d", cfg.vectorDim, queries.dim)
	}
	if cfg.startRow < 0 || cfg.startRow >= queries.rows {
		log.Fatalf("invalid start_row=%d for rows=%d", cfg.startRow, queries.rows)
	}

	available := queries.rows - cfg.startRow
	if cfg.queryWorkload <= 0 || cfg.queryWorkload > available {
		cfg.queryWorkload = available
	}
	queries = sliceMatrixRows(queries, cfg.startRow, cfg.startRow+cfg.queryWorkload)

	baseURL := fmt.Sprintf("%s://%s", cfg.weaviateScheme, cfg.weaviateHost)
	ctx := context.Background()

	log.Printf("[QUERY] target=%s class=%s", baseURL, cfg.className)
	log.Printf("[QUERY] file=%s start=%d workload=%d batch=%d topk=%d ef=%d",
		cfg.queryFile, cfg.startRow, cfg.queryWorkload, cfg.queryBatchSize, cfg.topK, cfg.queryEF)

	if err := waitForWeaviate(ctx, baseURL, time.Duration(cfg.waitSec)*time.Second); err != nil {
		log.Fatalf("wait for weaviate: %v", err)
	}
	if err := waitForClass(ctx, baseURL, cfg.className, 120*time.Second); err != nil {
		log.Fatalf("class check failed: %v", err)
	}

	queryLogPath := filepath.Join(cfg.outputDir, "query_client.jsonl")
	_ = os.Remove(queryLogPath)

	startAll := time.Now()
	var totalLatencyNS int64
	queriesCompleted := 0
	var batchLatencies []float64
	var joinedErr error

	clientID := 0
	opIndex := 0

	for row := 0; row < queries.rows; row += cfg.queryBatchSize {
		if cfg.overallSec > 0 && time.Since(startAll) > time.Duration(cfg.overallSec)*time.Second {
			joinedErr = errors.Join(joinedErr, fmt.Errorf("overall time budget exceeded"))
			break
		}

		end := row + cfg.queryBatchSize
		if end > queries.rows {
			end = queries.rows
		}

		got := end - row
		rowIndices := make([]int, got)
		results := make([][]int64, got)
		scores := make([][]float32, got)

		issuedAt := time.Now()
		var batchErr error

		for i := 0; i < got; i++ {
			rowIndices[i] = cfg.startRow + row + i
			vec := rowSlice(queries, row+i)

			lat, ids, dists, err := runOneQuery(ctx, cfg, baseURL, vec)
			if err != nil {
				batchErr = errors.Join(batchErr, err)
				results[i] = fillMissingIDs(cfg.topK)
				scores[i] = fillMissingScores(cfg.topK)
			} else {
				results[i] = ids
				scores[i] = dists
				queriesCompleted++
				totalLatencyNS += lat.Nanoseconds()
			}
		}

		completedAt := time.Now()
		batchLatency := completedAt.Sub(issuedAt).Seconds()
		batchLatencies = append(batchLatencies, batchLatency)

		rec := queryLogRecord{
			ClientRole:      "query",
			ClientID:        clientID,
			OpIndex:         opIndex,
			IssuedAtNS:      issuedAt.UnixNano(),
			CompletedAtNS:   completedAt.UnixNano(),
			DurationNS:      completedAt.Sub(issuedAt).Nanoseconds(),
			QueryStartRow:   cfg.startRow + row,
			QueryEndRow:     cfg.startRow + end,
			QueryRowIndices: rowIndices,
			ResultIDs:       results,
			ResultScores:    scores,
			Status:          "ok",
		}
		if batchErr != nil {
			rec.Status = "error"
			rec.Error = batchErr.Error()
			joinedErr = errors.Join(joinedErr, batchErr)
		}

		if err := appendJSONL(queryLogPath, rec); err != nil {
			log.Fatalf("write query log: %v", err)
		}

		log.Printf("[QUERY] batch op=%d rows=[%d,%d) completed=%d/%d batch_sec=%.3f",
			opIndex, rec.QueryStartRow, rec.QueryEndRow, queriesCompleted, queries.rows, batchLatency)

		opIndex++
	}

	totalTimeSec := time.Since(startAll).Seconds()

	meanLatencySec := 0.0
	if queriesCompleted > 0 {
		meanLatencySec = float64(totalLatencyNS) / float64(queriesCompleted) / 1e9
	}
	throughputQPS := 0.0
	if totalTimeSec > 0 {
		throughputQPS = float64(queriesCompleted) / totalTimeSec
	}

	summary := summaryRecord{
		TimestampUTC:     time.Now().UTC().Format(time.RFC3339),
		Worker:           baseURL,
		Class:            cfg.className,
		QueryFile:        cfg.queryFile,
		VecDim:           queries.dim,
		QueryBatchSize:   cfg.queryBatchSize,
		MeasureQueries:   queries.rows,
		StartRow:         cfg.startRow,
		TopK:             cfg.topK,
		QueryEF:          cfg.queryEF,
		WaitSec:          cfg.waitSec,
		OverallSec:       cfg.overallSec,
		RPCSec:           cfg.rpcTimeout.Seconds(),
		QueriesCompleted: queriesCompleted,
		TotalTimeSec:     totalTimeSec,
		MeanLatencySec:   meanLatencySec,
		ThroughputQPS:    throughputQPS,
		BatchLatencies:   batchLatencies,
		FinalStatus:      "ok",
	}
	if joinedErr != nil {
		summary.FinalStatus = "error"
		summary.FinalError = joinedErr.Error()
	}

	if err := writeJSON(filepath.Join(cfg.outputDir, "query_summary.json"), summary); err != nil {
		log.Fatalf("write summary: %v", err)
	}
	log.Printf("[QUERY] done: %d/%d queries, %.2f QPS, mean_lat=%.4fs, status=%s",
		queriesCompleted, queries.rows, throughputQPS, meanLatencySec, summary.FinalStatus)

	if joinedErr != nil {
		log.Fatal(joinedErr)
	}
}

func parseFlags() (config, error) {
	var cfg config

	flag.StringVar(&cfg.weaviateScheme, "weaviate-scheme", getenvDefault("WEAVIATE_SCHEME", "http"), "Weaviate scheme")
	flag.StringVar(&cfg.weaviateHost, "weaviate-host", getenvDefault("WEAVIATE_HOST", ""), "Weaviate host:port")
	flag.StringVar(&cfg.className, "class-name", getenvDefault("CLASS_NAME", ""), "Weaviate class name")

	flag.StringVar(&cfg.queryFile, "query-file", getenvDefault("QUERY_DATA_FILEPATH", ""), "Path to query .npy")
	flag.StringVar(&cfg.outputDir, "output-dir", getenvDefault("RESULT_PATH", "."), "Output directory")
	flag.IntVar(&cfg.startRow, "start-row", getenvIntDefault("START_ROW", 0), "Starting query row")
	flag.IntVar(&cfg.queryWorkload, "query-workload", getenvIntDefault("QUERY_CORPUS_SIZE", 0), "Queries to run")
	flag.IntVar(&cfg.vectorDim, "vector-dim", getenvIntDefault("VECTOR_DIM", 0), "Expected vector dimension")
	flag.IntVar(&cfg.queryBatchSize, "query-batch-size", getenvIntDefault("QUERY_BATCH_SIZE", 32), "Batch size")
	flag.IntVar(&cfg.topK, "top-k", getenvIntDefault("QUERY_TOPK", 10), "Top-k")
	flag.IntVar(&cfg.queryEF, "query-ef", getenvIntDefault("QUERY_EF", 64), "Query ef")

	flag.DurationVar(&cfg.rpcTimeout, "rpc-timeout", getenvDurationDefault("RPC_TIMEOUT", 30*time.Minute), "Per-query timeout")
	flag.IntVar(&cfg.waitSec, "wait-sec", getenvIntDefault("WAIT_SEC", 300), "Wait for Weaviate readiness")
	flag.IntVar(&cfg.overallSec, "overall-sec", getenvIntDefault("OVERALL_SEC", 25000), "Overall time budget")

	flag.Parse()

	if cfg.weaviateHost == "" {
		return config{}, errors.New("WEAVIATE_HOST / --weaviate-host is required")
	}
	if cfg.className == "" {
		return config{}, errors.New("CLASS_NAME / --class-name is required")
	}
	if cfg.queryFile == "" {
		return config{}, errors.New("QUERY_DATA_FILEPATH / --query-file is required")
	}
	if cfg.queryBatchSize <= 0 {
		return config{}, errors.New("QUERY_BATCH_SIZE must be positive")
	}
	if cfg.topK <= 0 {
		return config{}, errors.New("QUERY_TOPK must be positive")
	}

	return cfg, nil
}

func runOneQuery(parent context.Context, cfg config, baseURL string, vec []float32) (time.Duration, []int64, []float32, error) {
	vecStrs := make([]string, len(vec))
	for i, x := range vec {
		vecStrs[i] = strconv.FormatFloat(float64(x), 'f', -1, 32)
	}

	gql := fmt.Sprintf(
		`{Get{%s(nearVector:{vector:[%s]} limit:%d){doc_id _additional{distance}}}}`,
		cfg.className,
		strings.Join(vecStrs, ","),
		cfg.topK,
	)

	reqBody := graphqlRequest{Query: gql}
	body, err := json.Marshal(reqBody)
	if err != nil {
		return 0, nil, nil, err
	}

	ctx, cancel := context.WithTimeout(parent, cfg.rpcTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/v1/graphql", bytes.NewReader(body))
	if err != nil {
		return 0, nil, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Weaviate-Consistency-Level", "ONE")
	req.Header.Set("X-Weaviate-Query-Ef", strconv.Itoa(cfg.queryEF))

	start := time.Now()
	resp, err := httpClient.Do(req)
	lat := time.Since(start)
	if err != nil {
		return lat, nil, nil, err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return lat, nil, nil, fmt.Errorf("graphql status=%s body=%s", resp.Status, strings.TrimSpace(string(respBody)))
	}

	var parsed gqlResp
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return lat, nil, nil, fmt.Errorf("decode graphql response: %w", err)
	}

	// FIX: check for GraphQL-level errors
	if len(parsed.Errors) > 0 {
		msg := ""
		for _, e := range parsed.Errors {
			if m, ok := e["message"].(string); ok {
				msg += m + "; "
			}
		}
		return lat, nil, nil, fmt.Errorf("graphql errors: %s", msg)
	}

	getBlock, ok := parsed.Data["Get"]
	if !ok {
		return lat, nil, nil, fmt.Errorf("graphql response missing Data.Get")
	}
	hits, ok := getBlock[cfg.className]
	if !ok {
		return lat, nil, nil, fmt.Errorf("graphql response missing class %s", cfg.className)
	}

	ids := fillMissingIDs(cfg.topK)
	dists := fillMissingScores(cfg.topK)

	for i, hit := range hits {
		if i >= cfg.topK {
			break
		}

		if raw, ok := hit["doc_id"]; ok {
			switch v := raw.(type) {
			case float64:
				ids[i] = int64(v)
			case int64:
				ids[i] = v
			case int:
				ids[i] = int64(v)
			}
		}

		if add, ok := hit["_additional"].(map[string]any); ok {
			if rawd, ok := add["distance"]; ok {
				switch v := rawd.(type) {
				case float64:
					dists[i] = float32(v)
				case float32:
					dists[i] = v
				}
			}
		}
	}

	return lat, ids, dists, nil
}

func fillMissingIDs(topK int) []int64 {
	out := make([]int64, topK)
	for i := range out {
		out[i] = -1
	}
	return out
}

func fillMissingScores(topK int) []float32 {
	out := make([]float32, topK)
	return out
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
			return fmt.Errorf("meta endpoint not ready before timeout")
		}
		time.Sleep(2 * time.Second)
	}
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
				return fmt.Errorf("class %s not ready: %w", className, err)
			}
			return fmt.Errorf("class %s not ready before timeout", className)
		}
		time.Sleep(1 * time.Second)
	}
}

// =====================================================================
// npy I/O
// =====================================================================

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
		return matrixData{}, fmt.Errorf("not an npy file: %s", path)
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
		return matrixData{}, fmt.Errorf("unsupported npy version: %d.%d", ver[0], ver[1])
	}

	header := make([]byte, headerLen)
	if _, err := io.ReadFull(f, header); err != nil {
		return matrixData{}, err
	}
	h := string(header)

	if !strings.Contains(h, "'descr': '<f4'") && !strings.Contains(h, "\"descr\": \"<f4\"") {
		return matrixData{}, fmt.Errorf("only float32 little-endian npy is supported")
	}
	if strings.Contains(h, "'fortran_order': True") || strings.Contains(h, "\"fortran_order\": true") || strings.Contains(h, "\"fortran_order\": True") {
		return matrixData{}, fmt.Errorf("fortran-order npy not supported")
	}

	re := regexp.MustCompile(`[\(\[]\s*(\d+)\s*,\s*(\d+)\s*[,)\]]`)
	m := re.FindStringSubmatch(h)
	if len(m) != 3 {
		return matrixData{}, fmt.Errorf("could not parse shape from npy header: %s", h)
	}

	rows, err := strconv.Atoi(m[1])
	if err != nil {
		return matrixData{}, err
	}
	dim, err := strconv.Atoi(m[2])
	if err != nil {
		return matrixData{}, err
	}

	data := make([]float32, rows*dim)
	if err := binary.Read(f, binary.LittleEndian, data); err != nil {
		return matrixData{}, err
	}

	return matrixData{rows: rows, dim: dim, data: data}, nil
}

func sliceMatrixRows(m matrixData, start, end int) matrixData {
	if start < 0 {
		start = 0
	}
	if end > m.rows {
		end = m.rows
	}
	if start > end {
		start = end
	}
	return matrixData{
		rows: end - start,
		dim:  m.dim,
		data: m.data[start*m.dim : end*m.dim],
	}
}

func rowSlice(m matrixData, row int) []float32 {
	start := row * m.dim
	end := start + m.dim
	return m.data[start:end]
}

// =====================================================================
// I/O helpers
// =====================================================================

func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

func appendJSONL(path string, v any) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	return json.NewEncoder(f).Encode(v)
}

// =====================================================================
// env helpers
// =====================================================================

func getenvDefault(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func getenvIntDefault(key string, fallback int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	x, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return x
}

func getenvDurationDefault(key string, fallback time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err == nil {
		return d
	}
	if secs, err := strconv.Atoi(v); err == nil {
		return time.Duration(secs) * time.Second
	}
	return fallback
}
