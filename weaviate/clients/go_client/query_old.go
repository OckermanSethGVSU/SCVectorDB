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
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"
)

func hrsec() float64 { return float64(time.Now().UnixNano()) / 1e9 }

var httpClient = &http.Client{
	Transport: &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:        200,
		MaxIdleConnsPerHost: 200,
		IdleConnTimeout:     90 * time.Second,
	},
	Timeout: 0,
}

type NpyInfo struct {
	Offset int64
	N      int
	D      int
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

type shardSnapshot struct {
	Shards       int `json:"shards"`
	TotalObjects int `json:"total_objects"`
	TotalQueue   int `json:"total_queue"`
	Indexing     int `json:"indexing"`
}

type BatchLatency struct {
	BatchID         int     `json:"batch_id"`
	StartQueryRow   int     `json:"start_query_row"`
	EndQueryRow     int     `json:"end_query_row"`
	QueryCount      int     `json:"query_count"`
	BatchLatencySec float64 `json:"batch_latency_sec"`
	MeanPerQuerySec float64 `json:"mean_per_query_sec"`
	BatchQPS        float64 `json:"batch_qps"`
}

type Output struct {
	TimestampUTC string `json:"timestamp_utc"`
	Worker       string `json:"worker"`

	Class   string `json:"class"`
	TopK    int    `json:"top_k"`
	QueryEF int    `json:"query_ef"`

	QueryFile      string `json:"query_file"`
	VecDim         int    `json:"vec_dim"`
	QueryBatchSize int    `json:"query_batch_size"`
	MeasureQueries int    `json:"measure_queries"`
	StartRow       int    `json:"start_row"`

	WaitSec    int `json:"wait_sec"`
	OverallSec int `json:"overall_sec"`
	RpcSec     int `json:"rpc_sec"`

	QueriesCompleted int     `json:"queries_completed"`
	TotalTimeSec     float64 `json:"total_time_sec"`
	MeanLatencySec   float64 `json:"mean_latency_sec"`
	ThroughputQPS    float64 `json:"throughput_qps"`

	BatchLatencies []BatchLatency `json:"batch_latencies"`

	FinalStatus string `json:"final_status"`
	FinalError  string `json:"final_error,omitempty"`
}

func writeJSON(path string, out Output) {
	b, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		fmt.Println("[QUERY][ERROR] marshal json:", err)
		return
	}
	if err := os.WriteFile(path, b, 0644); err != nil {
		fmt.Println("[QUERY][ERROR] write json:", err)
		return
	}
	fmt.Println("[QUERY] wrote:", path)
}

func mustEnvString(k string) string {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		fmt.Println("[QUERY][ERROR] missing env", k)
		os.Exit(2)
	}
	return v
}

func mustEnvInt(k string) int {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		fmt.Println("[QUERY][ERROR] missing env", k)
		os.Exit(2)
	}
	var x int
	_, err := fmt.Sscanf(v, "%d", &x)
	if err != nil {
		fmt.Println("[QUERY][ERROR] bad int env", k, v)
		os.Exit(3)
	}
	return x
}

func httpGet(u string) ([]byte, int, error) {
	req, _ := http.NewRequest("GET", u, nil)
	res, err := httpClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	return body, res.StatusCode, nil
}

func waitForWeaviate(base string, maxSeconds int) error {
	meta := base + "/v1/meta"
	fmt.Println("[QUERY] Waiting for Weaviate readiness at", meta)

	start := time.Now()
	var lastBody []byte
	var lastCode int
	var lastErr error

	for {
		body, code, err := httpGet(meta)
		lastBody, lastCode, lastErr = body, code, err
		if err == nil && code == 200 && len(body) > 0 {
			fmt.Println("[QUERY] Weaviate ready.")
			return nil
		}
		if time.Since(start) > time.Duration(maxSeconds)*time.Second {
			return fmt.Errorf("timeout waiting %ds (last code=%d err=%v body=%s)",
				maxSeconds, lastCode, lastErr, string(lastBody))
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func waitForClass(base, className string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	u := base + "/v1/schema/" + className
	for time.Now().Before(deadline) {
		body, code, err := httpGet(u)
		if err == nil && code == 200 && len(body) > 0 {
			fmt.Println("[QUERY] Class is visible in schema.")
			return nil
		}
		time.Sleep(300 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for class %s to appear in schema", className)
}

func getShardSnapshot(base, className string) (shardSnapshot, error) {
	u, _ := url.Parse(base + "/v1/nodes")
	q := u.Query()
	q.Set("output", "verbose")
	q.Set("collection", className)
	u.RawQuery = q.Encode()

	body, code, err := httpGet(u.String())
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

func waitForRestoredCollection(base, className string, maxSeconds int) error {
	fmt.Printf("[QUERY] Waiting for restored collection %s to become queryable...\n", className)

	start := time.Now()
	var lastSnap shardSnapshot
	var lastErr error

	for {
		snap, err := getShardSnapshot(base, className)
		if err == nil {
			lastSnap = snap
			fmt.Printf("[QUERY] restore-check shards=%d objects=%d queue=%d indexing=%d\n",
				snap.Shards, snap.TotalObjects, snap.TotalQueue, snap.Indexing)

			if snap.Shards > 0 &&
				snap.TotalObjects > 0 &&
				snap.TotalQueue == 0 &&
				snap.Indexing == 0 {
				fmt.Println("[QUERY] Restored collection is ready.")
				return nil
			}
		} else {
			lastErr = err
		}

		if time.Since(start) > time.Duration(maxSeconds)*time.Second {
			return fmt.Errorf("timeout waiting for restored collection: last_snapshot=%+v last_err=%v", lastSnap, lastErr)
		}
		time.Sleep(2 * time.Second)
	}
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

	dim := info.D
	byteOff := info.Offset + int64(start*dim*4)
	if _, err := f.Seek(byteOff, io.SeekStart); err != nil {
		return nil, 0, err
	}

	raw := make([]byte, count*dim*4)
	if _, err := io.ReadFull(f, raw); err != nil {
		return nil, 0, err
	}

	out := make([]float32, count*dim)
	for i := 0; i < count*dim; i++ {
		u := binary.LittleEndian.Uint32(raw[i*4 : i*4+4])
		out[i] = math.Float32frombits(u)
	}
	return out, count, nil
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

func runOneQuery(ctx context.Context, base, className string, vec []float32, topK int) ([]int64, error) {
	query := fmt.Sprintf(
		`{"query":"{ Get { %s(nearVector:{vector:%s}, limit:%d) { doc_id _additional { distance } } } }"}`,
		className, formatVector(vec), topK,
	)

	req, err := http.NewRequestWithContext(ctx, "POST", base+"/v1/graphql", bytes.NewBufferString(query))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	res, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	body, _ := io.ReadAll(res.Body)
	if res.StatusCode != 200 {
		return nil, fmt.Errorf("graphql status=%d body=%s", res.StatusCode, string(body))
	}

	var r GraphQLResp
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, fmt.Errorf("graphql unmarshal: %w body=%s", err, string(body))
	}
	if len(r.Errors) > 0 {
		return nil, fmt.Errorf("graphql errors: %s", r.Errors[0].Message)
	}

	hits, ok := r.Data.Get[className]
	if !ok {
		return nil, fmt.Errorf("graphql response missing class %q", className)
	}

	ids := make([]int64, topK)
	for i := range ids {
		ids[i] = -1
	}
	for i, hit := range hits {
		if i >= topK {
			break
		}
		ids[i] = hit.DocID
	}
	return ids, nil
}

func writeInt64Npy(path string, rows [][]int64) error {
	if len(rows) == 0 {
		return fmt.Errorf("no rows to write")
	}

	n := len(rows)
	k := len(rows[0])
	for i := 1; i < n; i++ {
		if len(rows[i]) != k {
			return fmt.Errorf("ragged rows: row 0 has %d cols but row %d has %d", k, i, len(rows[i]))
		}
	}

	header := fmt.Sprintf("{'descr': '<i8', 'fortran_order': False, 'shape': (%d, %d), }", n, k)

	magic := []byte("\x93NUMPY")
	version := []byte{1, 0}

	headerBytes := []byte(header)
	baseLen := len(magic) + len(version) + 2 + len(headerBytes) + 1
	padLen := 16 - (baseLen % 16)
	if padLen == 16 {
		padLen = 0
	}
	headerBytes = append(headerBytes, bytes.Repeat([]byte(" "), padLen)...)
	headerBytes = append(headerBytes, '\n')

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := f.Write(magic); err != nil {
		return err
	}
	if _, err := f.Write(version); err != nil {
		return err
	}
	if err := binary.Write(f, binary.LittleEndian, uint16(len(headerBytes))); err != nil {
		return err
	}
	if _, err := f.Write(headerBytes); err != nil {
		return err
	}

	for i := 0; i < n; i++ {
		for j := 0; j < k; j++ {
			if err := binary.Write(f, binary.LittleEndian, rows[i][j]); err != nil {
				return err
			}
		}
	}

	fmt.Println("[QUERY] wrote:", path)
	return nil
}

func main() {
	outPath := flag.String("out", "query_yandex.json", "output json path")
	idsOutPath := flag.String("ids_out", "query_topk_ids.npy", "output npy path for retrieved top-k ids")
	className := flag.String("class", "Yandex10M", "weaviate class name")
	topK := flag.Int("topk", 10, "top-k docs to retrieve")
	queryBatchSize := flag.Int("query_batch_size", 2048, "number of queries per timing batch")
	waitSec := flag.Int("wait", 300, "seconds to wait for weaviate readiness")
	overallSec := flag.Int("overall_sec", 25000, "overall time budget seconds")
	rpcSec := flag.Int("rpc_sec", 1800, "per-query RPC timeout seconds")
	flag.Parse()

	workerIP := mustEnvString("WORKER_IP")
	restPort := mustEnvString("REST_PORT")
	queryFile := mustEnvString("QUERY_FILE")

	vecDim := mustEnvInt("VEC_DIM")
	measureQueries := mustEnvInt("MEASURE_QUERIES")
	startRow := mustEnvInt("START_ROW")
	queryEF := mustEnvInt("QUERY_EF")

	base := "http://" + workerIP + ":" + restPort

	out := Output{
		TimestampUTC:   time.Now().UTC().Format(time.RFC3339),
		Worker:         base,
		Class:          *className,
		TopK:           *topK,
		QueryEF:        queryEF,
		QueryFile:      queryFile,
		VecDim:         vecDim,
		QueryBatchSize: *queryBatchSize,
		MeasureQueries: measureQueries,
		StartRow:       startRow,
		WaitSec:        *waitSec,
		OverallSec:     *overallSec,
		RpcSec:         *rpcSec,
		BatchLatencies: []BatchLatency{},
		FinalStatus:    "ok",
	}

	fmt.Println("[QUERY] target:", base)
	fmt.Println("[QUERY] query_file:", queryFile)
	fmt.Printf("[QUERY] vec_dim=%d measure_queries=%d start_row=%d query_batch_size=%d topk=%d query_ef=%d\n",
		vecDim, measureQueries, startRow, *queryBatchSize, *topK, queryEF)

	if err := waitForWeaviate(base, *waitSec); err != nil {
		out.FinalStatus = "error"
		out.FinalError = err.Error()
		writeJSON(*outPath, out)
		os.Exit(4)
	}
	if err := waitForClass(base, *className, 60*time.Second); err != nil {
		out.FinalStatus = "error"
		out.FinalError = err.Error()
		writeJSON(*outPath, out)
		os.Exit(5)
	}
	if err := waitForRestoredCollection(base, *className, 600); err != nil {
		out.FinalStatus = "error"
		out.FinalError = err.Error()
		writeJSON(*outPath, out)
		os.Exit(6)
	}

	time.Sleep(10 * time.Second)

	f, err := os.Open(queryFile)
	if err != nil {
		out.FinalStatus = "error"
		out.FinalError = "open query file: " + err.Error()
		writeJSON(*outPath, out)
		os.Exit(7)
	}
	defer f.Close()

	info, err := parseNpyHeader(f, vecDim)
	if err != nil {
		out.FinalStatus = "error"
		out.FinalError = "npy: " + err.Error()
		writeJSON(*outPath, out)
		os.Exit(8)
	}
	fmt.Printf("[QUERY] npy: N=%d D=%d\n", info.N, info.D)

	if startRow >= info.N {
		startRow = 0
	}
	if measureQueries > info.N-startRow {
		measureQueries = info.N - startRow
		out.MeasureQueries = measureQueries
	}

	allIDs := make([][]int64, measureQueries)
	for i := 0; i < measureQueries; i++ {
		allIDs[i] = make([]int64, *topK)
		for j := 0; j < *topK; j++ {
			allIDs[i][j] = -1
		}
	}

	overallCtx, overallCancel := context.WithTimeout(context.Background(), time.Duration(*overallSec)*time.Second)
	defer overallCancel()

	cursor := startRow
	done := 0
	batchID := 0
	t0 := hrsec()
	totalLatency := 0.0

	for done < measureQueries {
		if err := overallCtx.Err(); err != nil {
			out.FinalStatus = "partial"
			out.FinalError = "overall timeout during query workload"
			break
		}

		n := *queryBatchSize
		remaining := measureQueries - done
		if n > remaining {
			n = remaining
		}
		if n <= 0 {
			break
		}

		vecs, got, err := readVectors(f, info, cursor, n)
		if err != nil {
			out.FinalStatus = "error"
			out.FinalError = "readVectors: " + err.Error()
			writeJSON(*outPath, out)
			os.Exit(9)
		}

		batchStart := hrsec()
		batchFailed := false

		for i := 0; i < got; i++ {
			if err := overallCtx.Err(); err != nil {
				out.FinalStatus = "partial"
				out.FinalError = "overall timeout during batch execution"
				batchFailed = true
				break
			}

			off := i * vecDim
			v := vecs[off : off+vecDim]

			var (
				qerr error
				ids  []int64
			)

			for attempt := 1; attempt <= 2; attempt++ {
				rpcCtx, rpcCancel := context.WithTimeout(overallCtx, time.Duration(*rpcSec)*time.Second)
				ids, qerr = runOneQuery(rpcCtx, base, *className, v, *topK)
				rpcCancel()

				if qerr == nil {
					break
				}
				fmt.Printf("[QUERY] attempt %d failed for global_query=%d batch_query=%d: %v\n",
					attempt, done+i, i, qerr)
				time.Sleep(3 * time.Second)
			}

			if qerr != nil {
				out.FinalStatus = "partial"
				out.FinalError = "query request failed: " + qerr.Error()
				out.QueriesCompleted = done
				out.TotalTimeSec = hrsec() - t0
				if out.TotalTimeSec > 0 {
					out.ThroughputQPS = float64(out.QueriesCompleted) / out.TotalTimeSec
				}
				if out.QueriesCompleted > 0 {
					out.MeanLatencySec = totalLatency / float64(out.QueriesCompleted)
				}
				writeJSON(*outPath, out)
				if done > 0 {
					_ = writeInt64Npy(*idsOutPath, allIDs[:done])
				}
				os.Exit(10)
			}

			copy(allIDs[done+i], ids)
		}

		if batchFailed {
			break
		}

		batchLatency := hrsec() - batchStart
		totalLatency += batchLatency
		done += got

		bl := BatchLatency{
			BatchID:         batchID,
			StartQueryRow:   cursor,
			EndQueryRow:     cursor + got - 1,
			QueryCount:      got,
			BatchLatencySec: batchLatency,
			MeanPerQuerySec: batchLatency / float64(got),
			BatchQPS:        float64(got) / batchLatency,
		}
		out.BatchLatencies = append(out.BatchLatencies, bl)

		cursor += got
		batchID++
	}

	out.QueriesCompleted = done
	out.TotalTimeSec = hrsec() - t0
	if out.TotalTimeSec > 0 {
		out.ThroughputQPS = float64(done) / out.TotalTimeSec
	}
	if done > 0 {
		out.MeanLatencySec = totalLatency / float64(done)
	}

	writeJSON(*outPath, out)

	if done > 0 {
		if err := writeInt64Npy(*idsOutPath, allIDs[:done]); err != nil {
			fmt.Println("[QUERY][ERROR] write ids npy:", err)
			if out.FinalStatus == "ok" {
				out.FinalStatus = "partial"
				out.FinalError = "failed to write ids npy: " + err.Error()
				writeJSON(*outPath, out)
			}
			os.Exit(11)
		}
	}

	if out.FinalStatus != "ok" {
		os.Exit(12)
	}
}
