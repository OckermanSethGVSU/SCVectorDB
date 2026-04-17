package main

import (
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

	"github.com/weaviate/weaviate-go-client/v5/weaviate"
	wgrpc "github.com/weaviate/weaviate-go-client/v5/weaviate/grpc"
	"github.com/weaviate/weaviate/entities/models"
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
	Timeout: 15 * time.Second,
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

func waitForWeaviate(base string, maxSeconds int) ([]byte, error) {
	meta := base + "/v1/meta"
	fmt.Println("[CLIENT] Waiting for Weaviate readiness at", meta)

	start := time.Now()
	var lastBody []byte
	var lastCode int
	var lastErr error

	for {
		body, code, err := httpGet(meta)
		lastBody, lastCode, lastErr = body, code, err
		if err == nil && code == 200 && len(body) > 0 {
			fmt.Println("[CLIENT] Weaviate ready.")
			return body, nil
		}
		if time.Since(start) > time.Duration(maxSeconds)*time.Second {
			return nil, fmt.Errorf("timeout waiting %ds (last code=%d err=%v body=%s)",
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
			return nil
		}
		time.Sleep(300 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for class %s to appear in schema", className)
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

type queueSnapshot struct {
	TotalQueue   int `json:"total_queue"`
	Shards       int `json:"shards"`
	Indexing     int `json:"indexing"`
	TotalObjects int `json:"total_objects"`
}

func getQueue(base, className string) (queueSnapshot, error) {
	u, _ := url.Parse(base + "/v1/nodes")
	q := u.Query()
	q.Set("output", "verbose")
	q.Set("collection", className)
	u.RawQuery = q.Encode()

	body, code, err := httpGet(u.String())
	if err != nil {
		return queueSnapshot{}, err
	}
	if code != 200 {
		return queueSnapshot{}, fmt.Errorf("nodes status http %d: %s", code, string(body))
	}

	var r nodesResp
	if err := json.Unmarshal(body, &r); err != nil {
		return queueSnapshot{}, fmt.Errorf("unmarshal nodes: %w (body=%s)", err, string(body))
	}

	snap := queueSnapshot{}
	for _, n := range r.Nodes {
		for _, s := range n.Shards {
			if s.Class != className {
				continue
			}
			snap.Shards++
			snap.TotalQueue += s.VectorQueueLength
			snap.TotalObjects += s.ObjectCount
			if strings.ToUpper(s.VectorIndexingStatus) == "INDEXING" {
				snap.Indexing++
			}
		}
	}
	return snap, nil
}

type NpyInfo struct {
	Offset int64
	N      int
	D      int
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

const idPropName = "doc_id"

func getenvDefault(key, def string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	return v
}

func mustEnvStringAny(keys ...string) string {
	for _, k := range keys {
		v := strings.TrimSpace(os.Getenv(k))
		if v != "" {
			return v
		}
	}
	fmt.Println("[CLIENT][ERROR] missing env; tried:", strings.Join(keys, ", "))
	os.Exit(2)
	return ""
}

func mustEnvIntAny(keys ...string) int {
	s := mustEnvStringAny(keys...)
	var x int
	_, err := fmt.Sscanf(s, "%d", &x)
	if err != nil {
		fmt.Println("[CLIENT][ERROR] bad int env value", s, "for keys", strings.Join(keys, ", "))
		os.Exit(3)
	}
	return x
}

func resolveBaseAndHosts() (base, hostPort, grpcHostPort string) {
	if v := strings.TrimSpace(os.Getenv("WEAVIATE_HTTP_ADDR")); v != "" {
		base = strings.TrimRight(v, "/")
		hostPort = strings.TrimPrefix(base, "http://")
		hostPort = strings.TrimPrefix(hostPort, "https://")
	} else {
		hostPort = mustEnvStringAny("WEAVIATE_HOST", "WORKER_IP")
		if !strings.Contains(hostPort, ":") {
			restPort := getenvDefault("REST_PORT", "8080")
			hostPort = hostPort + ":" + restPort
		}
		base = "http://" + hostPort
	}

	ip := strings.Split(hostPort, ":")[0]
	grpcPort := getenvDefault("GRPC_PORT", "50051")
	grpcHostPort = ip + ":" + grpcPort

	return base, hostPort, grpcHostPort
}

func makeDynamicClass(className string, dynamicThreshold int) *models.Class {
	return &models.Class{
		Class:           className,
		Vectorizer:      "none",
		VectorIndexType: "dynamic",
		VectorIndexConfig: map[string]any{
			"distance":  "dot",
			"threshold": dynamicThreshold,
			"hnsw": map[string]any{
				"ef":             64,
				"efConstruction": 100,
				"maxConnections": 16,
			},
		},
		Properties: []*models.Property{
			{
				Name:     idPropName,
				DataType: []string{"int"},
			},
		},
	}
}

func resetClass(ctx context.Context, client *weaviate.Client, base, className string, dynamicThreshold int) error {
	_ = client.Schema().ClassDeleter().WithClassName(className).Do(ctx)
	time.Sleep(1 * time.Second)

	cls := makeDynamicClass(className, dynamicThreshold)
	if err := client.Schema().ClassCreator().WithClass(cls).Do(ctx); err != nil {
		return fmt.Errorf("class create failed: %w", err)
	}
	return waitForClass(base, className, 60*time.Second)
}

type TimelinePoint struct {
	AbsSinceFirstRPCSec float64       `json:"abs_since_first_rpc_sec"`
	SinceThresholdSec   float64       `json:"since_threshold_sec"`
	Snapshot            queueSnapshot `json:"snapshot"`
}

type Output struct {
	TimestampUTC string `json:"timestamp_utc"`
	Worker       string `json:"worker"`
	DataFile     string `json:"data_file"`
	VecDim       int    `json:"vec_dim"`
	Class        string `json:"class"`
	Distance     string `json:"distance"`

	MeasureVecs      int `json:"measure_vecs"`
	StartRow         int `json:"start_row"`
	BatchSize        int `json:"batch_size"`
	DynamicThreshold int `json:"dynamic_threshold"`
	OverallSec       int `json:"overall_sec"`
	RpcSec           int `json:"rpc_sec"`
	IndexTimeoutSec  int `json:"index_timeout_sec"`
	IndexPollMS      int `json:"index_poll_ms"`
	IndexStablePolls int `json:"index_stable_polls"`

	InsertedVecs int     `json:"inserted_vecs"`
	SendSec      float64 `json:"send_sec"`

	ThresholdCrossed       bool          `json:"threshold_crossed"`
	ThresholdCrossAbsSec   float64       `json:"threshold_cross_abs_sec"`
	ThresholdCrossInserted int           `json:"threshold_cross_inserted"`
	ThresholdCrossSnapshot queueSnapshot `json:"threshold_cross_snapshot"`

	IndexComplete         bool          `json:"index_complete"`
	IndexCompleteAbsSec   float64       `json:"index_complete_abs_sec"`
	IndexCompleteSnapshot queueSnapshot `json:"index_complete_snapshot"`

	CPUIndexingSec float64 `json:"cpu_indexing_sec"`

	Timeline []TimelinePoint `json:"timeline"`

	FinalStatus string `json:"final_status"`
	FinalError  string `json:"final_error,omitempty"`
}

func writeJSON(path string, out Output) {
	b, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		fmt.Println("[CLIENT][ERROR] marshal json:", err)
		return
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		fmt.Println("[CLIENT][ERROR] write json:", err)
		return
	}
	fmt.Println("[CLIENT] wrote:", path)
}

func main() {
	outPath := flag.String("out", "index_time_yandex_ef64.json", "output json path")
	className := flag.String("class", getenvDefault("CLASS_NAME", "YANDEX"), "weaviate class name")
	batchSize := flag.Int("batch_size", 32768, "batch size")
	waitSec := flag.Int("wait", 180, "seconds to wait for weaviate readiness")
	overallSec := flag.Int("overall_sec", 25000, "overall time budget seconds")
	rpcSec := flag.Int("rpc_sec", 1800, "per-batch RPC timeout seconds")
	indexTimeoutSec := flag.Int("index_timeout_sec", 5000, "timeout after threshold crossing")
	indexPollMS := flag.Int("index_poll_ms", 1000, "poll interval after threshold crossing")
	indexStablePolls := flag.Int("index_stable_polls", 3, "require 0-queue and 0-indexing for N consecutive polls")
	flag.Parse()

	base, hostPort, grpcHostPort := resolveBaseAndHosts()
	dataFile := mustEnvStringAny("DATA_FILE", "DATA_FILEPATH")
	vecDim := mustEnvIntAny("VEC_DIM", "VECTOR_DIM")
	measureVecs := mustEnvIntAny("MEASURE_VECS", "CORPUS_SIZE")
	startRow := mustEnvIntAny("START_ROW")
	dynamicThreshold := mustEnvIntAny("DYNAMIC_THRESHOLD")

	out := Output{
		TimestampUTC:     time.Now().UTC().Format(time.RFC3339),
		Worker:           base,
		DataFile:         dataFile,
		VecDim:           vecDim,
		Class:            *className,
		Distance:         "dot",
		MeasureVecs:      measureVecs,
		StartRow:         startRow,
		BatchSize:        *batchSize,
		DynamicThreshold: dynamicThreshold,
		OverallSec:       *overallSec,
		RpcSec:           *rpcSec,
		IndexTimeoutSec:  *indexTimeoutSec,
		IndexPollMS:      *indexPollMS,
		IndexStablePolls: *indexStablePolls,
		Timeline:         []TimelinePoint{},
		FinalStatus:      "ok",
	}

	fmt.Println("[CLIENT] target:", base)
	fmt.Println("[CLIENT] grpc:", grpcHostPort)
	fmt.Println("[CLIENT] data:", dataFile)
	fmt.Printf("[CLIENT] vec_dim=%d measure=%d start_row=%d batch_size=%d threshold=%d distance=dot hnsw.ef=64 hnsw.efConstruction=100 hnsw.maxConnections=16\n",
		vecDim, measureVecs, startRow, *batchSize, dynamicThreshold)

	if _, err := waitForWeaviate(base, *waitSec); err != nil {
		out.FinalStatus = "error"
		out.FinalError = err.Error()
		writeJSON(*outPath, out)
		os.Exit(4)
	}

	client, err := weaviate.NewClient(weaviate.Config{
		Host:   hostPort,
		Scheme: "http",
		GrpcConfig: &wgrpc.Config{
			Host:    grpcHostPort,
			Secured: false,
		},
	})
	if err != nil {
		out.FinalStatus = "error"
		out.FinalError = "NewClient: " + err.Error()
		writeJSON(*outPath, out)
		os.Exit(5)
	}

	f, err := os.Open(dataFile)
	if err != nil {
		out.FinalStatus = "error"
		out.FinalError = "open: " + err.Error()
		writeJSON(*outPath, out)
		os.Exit(6)
	}
	defer f.Close()

	info, err := parseNpyHeader(f, vecDim)
	if err != nil {
		out.FinalStatus = "error"
		out.FinalError = "npy: " + err.Error()
		writeJSON(*outPath, out)
		os.Exit(7)
	}
	fmt.Printf("[CLIENT] npy: N=%d D=%d\n", info.N, info.D)

	resetCtx, cancelReset := context.WithTimeout(context.Background(), 5*time.Minute)
	err = resetClass(resetCtx, client, base, *className, dynamicThreshold)
	cancelReset()
	if err != nil {
		out.FinalStatus = "error"
		out.FinalError = "resetClass: " + err.Error()
		writeJSON(*outPath, out)
		os.Exit(8)
	}

	overallCtx, overallCancel := context.WithTimeout(context.Background(), time.Duration(*overallSec)*time.Second)
	defer overallCancel()

	cursor := startRow
	if cursor >= info.N {
		cursor = 0
	}

	inserted := 0
	started := false
	var t0, tLastRPC float64

	for inserted < measureVecs {
		if err := overallCtx.Err(); err != nil {
			out.FinalStatus = "partial"
			out.FinalError = "overall timeout before inserts finished"
			writeJSON(*outPath, out)
			os.Exit(9)
		}

		n := *batchSize
		remaining := measureVecs - inserted
		if n > remaining {
			n = remaining
		}
		if n <= 0 {
			break
		}
		if cursor+n > info.N {
			cursor = 0
		}

		vecs, got, err := readVectors(f, info, cursor, n)
		if err != nil {
			out.FinalStatus = "error"
			out.FinalError = "readVectors: " + err.Error()
			writeJSON(*outPath, out)
			os.Exit(10)
		}

		objs := make([]*models.Object, 0, got)
		for i := 0; i < got; i++ {
			off := i * vecDim
			v := make([]float32, vecDim)
			copy(v, vecs[off:off+vecDim])

			globalRow := int64(cursor + i)
			objs = append(objs, &models.Object{
				Class: *className,
				Properties: map[string]any{
					idPropName: globalRow,
				},
				Vector: v,
			})
		}

		if !started {
			t0 = hrsec()
			started = true
		}

		rpcCtx, rpcCancel := context.WithTimeout(overallCtx, time.Duration(*rpcSec)*time.Second)
		_, err = client.Batch().ObjectsBatcher().WithObjects(objs...).Do(rpcCtx)
		rpcCancel()
		if err != nil {
			out.FinalStatus = "partial"
			out.FinalError = "batch insert: " + err.Error()
			out.InsertedVecs = inserted
			writeJSON(*outPath, out)
			os.Exit(11)
		}

		inserted += got
		cursor += got
		tLastRPC = hrsec()

		if inserted%(100000) == 0 || inserted == measureVecs {
			fmt.Printf("[CLIENT] inserted %d / %d (send_elapsed=%.1fs)\n", inserted, measureVecs, tLastRPC-t0)
		}

		if !out.ThresholdCrossed {
			snap, err := getQueue(base, *className)
			if err == nil && snap.TotalObjects >= dynamicThreshold {
				out.ThresholdCrossed = true
				out.ThresholdCrossAbsSec = tLastRPC - t0
				out.ThresholdCrossInserted = inserted
				out.ThresholdCrossSnapshot = snap
				fmt.Printf("[CLIENT] threshold crossed at inserted=%d abs=%.3fs objects=%d queue=%d indexing=%d\n",
					inserted, out.ThresholdCrossAbsSec, snap.TotalObjects, snap.TotalQueue, snap.Indexing)
			}
		}
	}

	out.InsertedVecs = inserted
	if started {
		out.SendSec = tLastRPC - t0
	}

	if !out.ThresholdCrossed {
		snap, err := getQueue(base, *className)
		if err == nil && snap.TotalObjects >= dynamicThreshold {
			out.ThresholdCrossed = true
			out.ThresholdCrossAbsSec = tLastRPC - t0
			out.ThresholdCrossInserted = inserted
			out.ThresholdCrossSnapshot = snap
		}
	}

	if !out.ThresholdCrossed {
		out.FinalStatus = "error"
		out.FinalError = "dynamic threshold was never crossed"
		writeJSON(*outPath, out)
		os.Exit(12)
	}

	fmt.Println("[CLIENT] monitoring indexing completion...")
	stable := 0
	deadline := time.Now().Add(time.Duration(*indexTimeoutSec) * time.Second)
	poll := time.Duration(*indexPollMS) * time.Millisecond

	for time.Now().Before(deadline) {
		snap, err := getQueue(base, *className)
		nowAbs := hrsec() - t0

		if err == nil {
			out.Timeline = append(out.Timeline, TimelinePoint{
				AbsSinceFirstRPCSec: nowAbs,
				SinceThresholdSec:   nowAbs - out.ThresholdCrossAbsSec,
				Snapshot:            snap,
			})

			fmt.Printf("[CLIENT] poll abs=%.1fs since_threshold=%.1fs objects=%d queue=%d indexing=%d\n",
				nowAbs, nowAbs-out.ThresholdCrossAbsSec, snap.TotalObjects, snap.TotalQueue, snap.Indexing)

			if snap.TotalQueue == 0 && snap.Indexing == 0 {
				stable++
				if stable >= *indexStablePolls {
					out.IndexComplete = true
					out.IndexCompleteAbsSec = nowAbs
					out.IndexCompleteSnapshot = snap
					out.CPUIndexingSec = out.IndexCompleteAbsSec - out.ThresholdCrossAbsSec
					out.FinalStatus = "ok"
					writeJSON(*outPath, out)
					fmt.Printf("[CLIENT] indexing complete: cpu_indexing_sec=%.3f\n", out.CPUIndexingSec)
					return
				}
			} else {
				stable = 0
			}
		}

		time.Sleep(poll)
	}

	out.IndexComplete = false
	out.CPUIndexingSec = 0
	out.FinalStatus = "partial"
	out.FinalError = fmt.Sprintf("indexing did not finish within %d seconds after threshold crossing", *indexTimeoutSec)
	writeJSON(*outPath, out)
	os.Exit(13)
}
// package main

// import (
// 	"context"
// 	"encoding/binary"
// 	"encoding/json"
// 	"errors"
// 	"flag"
// 	"fmt"
// 	"io"
// 	"math"
// 	"net"
// 	"net/http"
// 	"net/url"
// 	"os"
// 	"regexp"
// 	"strings"
// 	"time"

// 	"github.com/weaviate/weaviate-go-client/v5/weaviate"
// 	wgrpc "github.com/weaviate/weaviate-go-client/v5/weaviate/grpc"
// 	"github.com/weaviate/weaviate/entities/models"
// )

// func hrsec() float64 { return float64(time.Now().UnixNano()) / 1e9 }

// var httpClient = &http.Client{
// 	Transport: &http.Transport{
// 		Proxy: http.ProxyFromEnvironment,
// 		DialContext: (&net.Dialer{
// 			Timeout:   5 * time.Second,
// 			KeepAlive: 30 * time.Second,
// 		}).DialContext,
// 		MaxIdleConns:        200,
// 		MaxIdleConnsPerHost: 200,
// 		IdleConnTimeout:     90 * time.Second,
// 	},
// 	Timeout: 15 * time.Second,
// }

// func httpGet(u string) ([]byte, int, error) {
// 	req, _ := http.NewRequest("GET", u, nil)
// 	res, err := httpClient.Do(req)
// 	if err != nil {
// 		return nil, 0, err
// 	}
// 	defer res.Body.Close()
// 	body, _ := io.ReadAll(res.Body)
// 	return body, res.StatusCode, nil
// }

// func waitForWeaviate(base string, maxSeconds int) ([]byte, error) {
// 	meta := base + "/v1/meta"
// 	fmt.Println("[CLIENT] Waiting for Weaviate readiness at", meta)

// 	start := time.Now()
// 	var lastBody []byte
// 	var lastCode int
// 	var lastErr error

// 	for {
// 		body, code, err := httpGet(meta)
// 		lastBody, lastCode, lastErr = body, code, err
// 		if err == nil && code == 200 && len(body) > 0 {
// 			fmt.Println("[CLIENT] Weaviate ready.")
// 			return body, nil
// 		}
// 		if time.Since(start) > time.Duration(maxSeconds)*time.Second {
// 			return nil, fmt.Errorf("timeout waiting %ds (last code=%d err=%v body=%s)",
// 				maxSeconds, lastCode, lastErr, string(lastBody))
// 		}
// 		time.Sleep(500 * time.Millisecond)
// 	}
// }

// func waitForClass(base, className string, timeout time.Duration) error {
// 	deadline := time.Now().Add(timeout)
// 	u := base + "/v1/schema/" + className
// 	for time.Now().Before(deadline) {
// 		body, code, err := httpGet(u)
// 		if err == nil && code == 200 && len(body) > 0 {
// 			return nil
// 		}
// 		time.Sleep(300 * time.Millisecond)
// 	}
// 	return fmt.Errorf("timeout waiting for class %s to appear in schema", className)
// }

// type nodesResp struct {
// 	Nodes []struct {
// 		Name   string `json:"name"`
// 		Status string `json:"status"`
// 		Shards []struct {
// 			Class                string `json:"class"`
// 			Name                 string `json:"name"`
// 			ObjectCount          int    `json:"objectCount"`
// 			VectorIndexingStatus string `json:"vectorIndexingStatus"`
// 			VectorQueueLength    int    `json:"vectorQueueLength"`
// 		} `json:"shards"`
// 	} `json:"nodes"`
// }

// type queueSnapshot struct {
// 	TotalQueue   int `json:"total_queue"`
// 	Shards       int `json:"shards"`
// 	Indexing     int `json:"indexing"`
// 	TotalObjects int `json:"total_objects"`
// }

// func getQueue(base, className string) (queueSnapshot, error) {
// 	u, _ := url.Parse(base + "/v1/nodes")
// 	q := u.Query()
// 	q.Set("output", "verbose")
// 	q.Set("collection", className)
// 	u.RawQuery = q.Encode()

// 	body, code, err := httpGet(u.String())
// 	if err != nil {
// 		return queueSnapshot{}, err
// 	}
// 	if code != 200 {
// 		return queueSnapshot{}, fmt.Errorf("nodes status http %d: %s", code, string(body))
// 	}

// 	var r nodesResp
// 	if err := json.Unmarshal(body, &r); err != nil {
// 		return queueSnapshot{}, fmt.Errorf("unmarshal nodes: %w (body=%s)", err, string(body))
// 	}

// 	snap := queueSnapshot{}
// 	for _, n := range r.Nodes {
// 		for _, s := range n.Shards {
// 			if s.Class != className {
// 				continue
// 			}
// 			snap.Shards++
// 			snap.TotalQueue += s.VectorQueueLength
// 			snap.TotalObjects += s.ObjectCount
// 			if strings.ToUpper(s.VectorIndexingStatus) == "INDEXING" {
// 				snap.Indexing++
// 			}
// 		}
// 	}
// 	return snap, nil
// }

// type NpyInfo struct {
// 	Offset int64
// 	N      int
// 	D      int
// }

// func parseNpyHeader(f *os.File, expectedD int) (*NpyInfo, error) {
// 	magic := make([]byte, 6)
// 	if _, err := io.ReadFull(f, magic); err != nil {
// 		return nil, err
// 	}
// 	if !(magic[0] == 0x93 && string(magic[1:]) == "NUMPY") {
// 		return nil, errors.New("bad npy magic")
// 	}

// 	ver := make([]byte, 2)
// 	if _, err := io.ReadFull(f, ver); err != nil {
// 		return nil, err
// 	}
// 	major := ver[0]

// 	var headerLen int
// 	if major == 1 {
// 		var u16 uint16
// 		if err := binary.Read(f, binary.LittleEndian, &u16); err != nil {
// 			return nil, err
// 		}
// 		headerLen = int(u16)
// 	} else {
// 		var u32 uint32
// 		if err := binary.Read(f, binary.LittleEndian, &u32); err != nil {
// 			return nil, err
// 		}
// 		headerLen = int(u32)
// 	}

// 	header := make([]byte, headerLen)
// 	if _, err := io.ReadFull(f, header); err != nil {
// 		return nil, err
// 	}
// 	h := string(header)

// 	if !strings.Contains(h, "'descr': '<f4'") && !strings.Contains(h, "\"descr\": \"<f4\"") {
// 		return nil, fmt.Errorf("expected <f4 float32; header=%s", h)
// 	}

// 	re := regexp.MustCompile(`\(\s*(\d+)\s*,\s*(\d+)\s*\)`)
// 	m := re.FindStringSubmatch(h)
// 	if len(m) != 3 {
// 		return nil, fmt.Errorf("cannot parse shape from header=%s", h)
// 	}

// 	var n, d int
// 	fmt.Sscanf(m[1], "%d", &n)
// 	fmt.Sscanf(m[2], "%d", &d)

// 	if expectedD > 0 && d != expectedD {
// 		return nil, fmt.Errorf("vector dim mismatch: got %d expected %d", d, expectedD)
// 	}

// 	off, _ := f.Seek(0, io.SeekCurrent)
// 	return &NpyInfo{Offset: off, N: n, D: d}, nil
// }

// func readVectors(f *os.File, info *NpyInfo, start, count int) ([]float32, int, error) {
// 	if start < 0 || start >= info.N {
// 		return nil, 0, fmt.Errorf("start out of range: %d (N=%d)", start, info.N)
// 	}
// 	if start+count > info.N {
// 		count = info.N - start
// 	}
// 	if count <= 0 {
// 		return nil, 0, errors.New("count <= 0")
// 	}

// 	dim := info.D
// 	byteOff := info.Offset + int64(start*dim*4)
// 	if _, err := f.Seek(byteOff, io.SeekStart); err != nil {
// 		return nil, 0, err
// 	}

// 	raw := make([]byte, count*dim*4)
// 	if _, err := io.ReadFull(f, raw); err != nil {
// 		return nil, 0, err
// 	}

// 	out := make([]float32, count*dim)
// 	for i := 0; i < count*dim; i++ {
// 		u := binary.LittleEndian.Uint32(raw[i*4 : i*4+4])
// 		out[i] = math.Float32frombits(u)
// 	}
// 	return out, count, nil
// }

// const idPropName = "doc_id"

// func mustEnvString(k string) string {
// 	v := strings.TrimSpace(os.Getenv(k))
// 	if v == "" {
// 		fmt.Println("[CLIENT][ERROR] missing env", k)
// 		os.Exit(2)
// 	}
// 	return v
// }

// func mustEnvInt(k string) int {
// 	v := strings.TrimSpace(os.Getenv(k))
// 	if v == "" {
// 		fmt.Println("[CLIENT][ERROR] missing env", k)
// 		os.Exit(2)
// 	}
// 	var x int
// 	_, err := fmt.Sscanf(v, "%d", &x)
// 	if err != nil {
// 		fmt.Println("[CLIENT][ERROR] bad int env", k, v)
// 		os.Exit(3)
// 	}
// 	return x
// }

// func makeDynamicClass(className string, dynamicThreshold int) *models.Class {
// 	return &models.Class{
// 		Class:           className,
// 		Vectorizer:      "none",
// 		VectorIndexType: "dynamic",
// 		VectorIndexConfig: map[string]any{
// 			"distance":  "dot",
// 			"threshold": dynamicThreshold,
// 			"hnsw": map[string]any{
// 				"ef":             64,
// 				"efConstruction": 100,
// 				"maxConnections": 16,
// 			},
// 		},
// 		Properties: []*models.Property{
// 			{
// 				Name:     idPropName,
// 				DataType: []string{"int"},
// 			},
// 		},
// 	}
// }

// func resetClass(ctx context.Context, client *weaviate.Client, base, className string, dynamicThreshold int) error {
// 	_ = client.Schema().ClassDeleter().WithClassName(className).Do(ctx)
// 	time.Sleep(1 * time.Second)

// 	cls := makeDynamicClass(className, dynamicThreshold)
// 	if err := client.Schema().ClassCreator().WithClass(cls).Do(ctx); err != nil {
// 		return fmt.Errorf("class create failed: %w", err)
// 	}
// 	return waitForClass(base, className, 60*time.Second)
// }

// type TimelinePoint struct {
// 	AbsSinceFirstRPCSec float64       `json:"abs_since_first_rpc_sec"`
// 	SinceThresholdSec   float64       `json:"since_threshold_sec"`
// 	Snapshot            queueSnapshot `json:"snapshot"`
// }

// type Output struct {
// 	TimestampUTC string `json:"timestamp_utc"`
// 	Worker       string `json:"worker"`
// 	DataFile     string `json:"data_file"`
// 	VecDim       int    `json:"vec_dim"`
// 	Class        string `json:"class"`
// 	Distance     string `json:"distance"`

// 	MeasureVecs      int `json:"measure_vecs"`
// 	StartRow         int `json:"start_row"`
// 	BatchSize        int `json:"batch_size"`
// 	DynamicThreshold int `json:"dynamic_threshold"`
// 	OverallSec       int `json:"overall_sec"`
// 	RpcSec           int `json:"rpc_sec"`
// 	IndexTimeoutSec  int `json:"index_timeout_sec"`
// 	IndexPollMS      int `json:"index_poll_ms"`
// 	IndexStablePolls int `json:"index_stable_polls"`

// 	InsertedVecs int     `json:"inserted_vecs"`
// 	SendSec      float64 `json:"send_sec"`

// 	ThresholdCrossed       bool          `json:"threshold_crossed"`
// 	ThresholdCrossAbsSec   float64       `json:"threshold_cross_abs_sec"`
// 	ThresholdCrossInserted int           `json:"threshold_cross_inserted"`
// 	ThresholdCrossSnapshot queueSnapshot `json:"threshold_cross_snapshot"`

// 	IndexComplete         bool          `json:"index_complete"`
// 	IndexCompleteAbsSec   float64       `json:"index_complete_abs_sec"`
// 	IndexCompleteSnapshot queueSnapshot `json:"index_complete_snapshot"`

// 	CPUIndexingSec float64 `json:"cpu_indexing_sec"`

// 	Timeline []TimelinePoint `json:"timeline"`

// 	FinalStatus string `json:"final_status"`
// 	FinalError  string `json:"final_error,omitempty"`
// }

// func writeJSON(path string, out Output) {
// 	b, err := json.MarshalIndent(out, "", "  ")
// 	if err != nil {
// 		fmt.Println("[CLIENT][ERROR] marshal json:", err)
// 		return
// 	}
// 	if err := os.WriteFile(path, b, 0644); err != nil {
// 		fmt.Println("[CLIENT][ERROR] write json:", err)
// 		return
// 	}
// 	fmt.Println("[CLIENT] wrote:", path)
// }

// func main() {
// 	outPath := flag.String("out", "index_time_yandex_ef64.json", "output json path")
// 	className := flag.String("class", "Yandex10MEF64", "weaviate class name")
// 	batchSize := flag.Int("batch_size", 32768, "batch size")
// 	waitSec := flag.Int("wait", 180, "seconds to wait for weaviate readiness")
// 	overallSec := flag.Int("overall_sec", 25000, "overall time budget seconds")
// 	rpcSec := flag.Int("rpc_sec", 1800, "per-batch RPC timeout seconds")
// 	indexTimeoutSec := flag.Int("index_timeout_sec", 5000, "timeout after threshold crossing")
// 	indexPollMS := flag.Int("index_poll_ms", 1000, "poll interval after threshold crossing")
// 	indexStablePolls := flag.Int("index_stable_polls", 3, "require 0-queue and 0-indexing for N consecutive polls")
// 	flag.Parse()

// 	workerIP := mustEnvString("WORKER_IP")
// 	restPort := mustEnvString("REST_PORT")
// 	grpcPort := mustEnvString("GRPC_PORT")
// 	dataFile := mustEnvString("DATA_FILE")

// 	vecDim := mustEnvInt("VEC_DIM")
// 	measureVecs := mustEnvInt("MEASURE_VECS")
// 	startRow := mustEnvInt("START_ROW")
// 	dynamicThreshold := mustEnvInt("DYNAMIC_THRESHOLD")

// 	base := "http://" + workerIP + ":" + restPort

// 	out := Output{
// 		TimestampUTC:     time.Now().UTC().Format(time.RFC3339),
// 		Worker:           base,
// 		DataFile:         dataFile,
// 		VecDim:           vecDim,
// 		Class:            *className,
// 		Distance:         "dot",
// 		MeasureVecs:      measureVecs,
// 		StartRow:         startRow,
// 		BatchSize:        *batchSize,
// 		DynamicThreshold: dynamicThreshold,
// 		OverallSec:       *overallSec,
// 		RpcSec:           *rpcSec,
// 		IndexTimeoutSec:  *indexTimeoutSec,
// 		IndexPollMS:      *indexPollMS,
// 		IndexStablePolls: *indexStablePolls,
// 		Timeline:         []TimelinePoint{},
// 		FinalStatus:      "ok",
// 	}

// 	fmt.Println("[CLIENT] target:", base)
// 	fmt.Println("[CLIENT] grpc:", workerIP+":"+grpcPort)
// 	fmt.Println("[CLIENT] data:", dataFile)
// 	fmt.Printf("[CLIENT] vec_dim=%d measure=%d start_row=%d batch_size=%d threshold=%d distance=dot hnsw.ef=64 hnsw.efConstruction=100 hnsw.maxConnections=16\n",
// 		vecDim, measureVecs, startRow, *batchSize, dynamicThreshold)

// 	if _, err := waitForWeaviate(base, *waitSec); err != nil {
// 		out.FinalStatus = "error"
// 		out.FinalError = err.Error()
// 		writeJSON(*outPath, out)
// 		os.Exit(4)
// 	}

// 	client, err := weaviate.NewClient(weaviate.Config{
// 		Host:   workerIP + ":" + restPort,
// 		Scheme: "http",
// 		GrpcConfig: &wgrpc.Config{
// 			Host:    workerIP + ":" + grpcPort,
// 			Secured: false,
// 		},
// 	})
// 	if err != nil {
// 		out.FinalStatus = "error"
// 		out.FinalError = "NewClient: " + err.Error()
// 		writeJSON(*outPath, out)
// 		os.Exit(5)
// 	}

// 	f, err := os.Open(dataFile)
// 	if err != nil {
// 		out.FinalStatus = "error"
// 		out.FinalError = "open: " + err.Error()
// 		writeJSON(*outPath, out)
// 		os.Exit(6)
// 	}
// 	defer f.Close()

// 	info, err := parseNpyHeader(f, vecDim)
// 	if err != nil {
// 		out.FinalStatus = "error"
// 		out.FinalError = "npy: " + err.Error()
// 		writeJSON(*outPath, out)
// 		os.Exit(7)
// 	}
// 	fmt.Printf("[CLIENT] npy: N=%d D=%d\n", info.N, info.D)

// 	resetCtx, cancelReset := context.WithTimeout(context.Background(), 5*time.Minute)
// 	err = resetClass(resetCtx, client, base, *className, dynamicThreshold)
// 	cancelReset()
// 	if err != nil {
// 		out.FinalStatus = "error"
// 		out.FinalError = "resetClass: " + err.Error()
// 		writeJSON(*outPath, out)
// 		os.Exit(8)
// 	}

// 	overallCtx, overallCancel := context.WithTimeout(context.Background(), time.Duration(*overallSec)*time.Second)
// 	defer overallCancel()

// 	cursor := startRow
// 	if cursor >= info.N {
// 		cursor = 0
// 	}

// 	inserted := 0
// 	started := false
// 	var t0, tLastRPC float64

// 	for inserted < measureVecs {
// 		if err := overallCtx.Err(); err != nil {
// 			out.FinalStatus = "partial"
// 			out.FinalError = "overall timeout before inserts finished"
// 			writeJSON(*outPath, out)
// 			os.Exit(9)
// 		}

// 		n := *batchSize
// 		remaining := measureVecs - inserted
// 		if n > remaining {
// 			n = remaining
// 		}
// 		if n <= 0 {
// 			break
// 		}
// 		if cursor+n > info.N {
// 			cursor = 0
// 		}

// 		vecs, got, err := readVectors(f, info, cursor, n)
// 		if err != nil {
// 			out.FinalStatus = "error"
// 			out.FinalError = "readVectors: " + err.Error()
// 			writeJSON(*outPath, out)
// 			os.Exit(10)
// 		}

// 		objs := make([]*models.Object, 0, got)
// 		for i := 0; i < got; i++ {
// 			off := i * vecDim
// 			v := make([]float32, vecDim)
// 			copy(v, vecs[off:off+vecDim])

// 			globalRow := int64(cursor + i)
// 			objs = append(objs, &models.Object{
// 				Class: *className,
// 				Properties: map[string]any{
// 					idPropName: globalRow,
// 				},
// 				Vector: v,
// 			})
// 		}

// 		if !started {
// 			t0 = hrsec()
// 			started = true
// 		}

// 		rpcCtx, rpcCancel := context.WithTimeout(overallCtx, time.Duration(*rpcSec)*time.Second)
// 		_, err = client.Batch().ObjectsBatcher().WithObjects(objs...).Do(rpcCtx)
// 		rpcCancel()
// 		if err != nil {
// 			out.FinalStatus = "partial"
// 			out.FinalError = "batch insert: " + err.Error()
// 			out.InsertedVecs = inserted
// 			writeJSON(*outPath, out)
// 			os.Exit(11)
// 		}

// 		inserted += got
// 		cursor += got
// 		tLastRPC = hrsec()

// 		if inserted%(100000) == 0 || inserted == measureVecs {
// 			fmt.Printf("[CLIENT] inserted %d / %d (send_elapsed=%.1fs)\n", inserted, measureVecs, tLastRPC-t0)
// 		}

// 		if !out.ThresholdCrossed {
// 			snap, err := getQueue(base, *className)
// 			if err == nil && snap.TotalObjects >= dynamicThreshold {
// 				out.ThresholdCrossed = true
// 				out.ThresholdCrossAbsSec = tLastRPC - t0
// 				out.ThresholdCrossInserted = inserted
// 				out.ThresholdCrossSnapshot = snap
// 				fmt.Printf("[CLIENT] threshold crossed at inserted=%d abs=%.3fs objects=%d queue=%d indexing=%d\n",
// 					inserted, out.ThresholdCrossAbsSec, snap.TotalObjects, snap.TotalQueue, snap.Indexing)
// 			}
// 		}
// 	}

// 	out.InsertedVecs = inserted
// 	if started {
// 		out.SendSec = tLastRPC - t0
// 	}

// 	if !out.ThresholdCrossed {
// 		snap, err := getQueue(base, *className)
// 		if err == nil && snap.TotalObjects >= dynamicThreshold {
// 			out.ThresholdCrossed = true
// 			out.ThresholdCrossAbsSec = tLastRPC - t0
// 			out.ThresholdCrossInserted = inserted
// 			out.ThresholdCrossSnapshot = snap
// 		}
// 	}

// 	if !out.ThresholdCrossed {
// 		out.FinalStatus = "error"
// 		out.FinalError = "dynamic threshold was never crossed"
// 		writeJSON(*outPath, out)
// 		os.Exit(12)
// 	}

// 	fmt.Println("[CLIENT] monitoring indexing completion...")
// 	stable := 0
// 	deadline := time.Now().Add(time.Duration(*indexTimeoutSec) * time.Second)
// 	poll := time.Duration(*indexPollMS) * time.Millisecond

// 	for time.Now().Before(deadline) {
// 		snap, err := getQueue(base, *className)
// 		nowAbs := hrsec() - t0

// 		if err == nil {
// 			out.Timeline = append(out.Timeline, TimelinePoint{
// 				AbsSinceFirstRPCSec: nowAbs,
// 				SinceThresholdSec:   nowAbs - out.ThresholdCrossAbsSec,
// 				Snapshot:            snap,
// 			})

// 			fmt.Printf("[CLIENT] poll abs=%.1fs since_threshold=%.1fs objects=%d queue=%d indexing=%d\n",
// 				nowAbs, nowAbs-out.ThresholdCrossAbsSec, snap.TotalObjects, snap.TotalQueue, snap.Indexing)

// 			if snap.TotalQueue == 0 && snap.Indexing == 0 {
// 				stable++
// 				if stable >= *indexStablePolls {
// 					out.IndexComplete = true
// 					out.IndexCompleteAbsSec = nowAbs
// 					out.IndexCompleteSnapshot = snap
// 					out.CPUIndexingSec = out.IndexCompleteAbsSec - out.ThresholdCrossAbsSec
// 					out.FinalStatus = "ok"
// 					writeJSON(*outPath, out)
// 					fmt.Printf("[CLIENT] indexing complete: cpu_indexing_sec=%.3f\n", out.CPUIndexingSec)
// 					return
// 				}
// 			} else {
// 				stable = 0
// 			}
// 		}

// 		time.Sleep(poll)
// 	}

// 	out.IndexComplete = false
// 	out.CPUIndexingSec = 0
// 	out.FinalStatus = "partial"
// 	out.FinalError = fmt.Sprintf("indexing did not finish within %d seconds after threshold crossing", *indexTimeoutSec)
// 	writeJSON(*outPath, out)
// 	os.Exit(13)
// }


// // package main

// // import (
// // 	"context"
// // 	"encoding/binary"
// // 	"encoding/json"
// // 	"errors"
// // 	"flag"
// // 	"fmt"
// // 	"io"
// // 	"math"
// // 	"net"
// // 	"net/http"
// // 	"net/url"
// // 	"os"
// // 	"regexp"
// // 	"strings"
// // 	"time"

// // 	"github.com/weaviate/weaviate-go-client/v5/weaviate"
// // 	wgrpc "github.com/weaviate/weaviate-go-client/v5/weaviate/grpc"
// // 	"github.com/weaviate/weaviate/entities/models"
// // )

// // func hrsec() float64 { return float64(time.Now().UnixNano()) / 1e9 }

// // var httpClient = &http.Client{
// // 	Transport: &http.Transport{
// // 		Proxy: http.ProxyFromEnvironment,
// // 		DialContext: (&net.Dialer{
// // 			Timeout:   5 * time.Second,
// // 			KeepAlive: 30 * time.Second,
// // 		}).DialContext,
// // 		MaxIdleConns:        200,
// // 		MaxIdleConnsPerHost: 200,
// // 		IdleConnTimeout:     90 * time.Second,
// // 	},
// // 	Timeout: 15 * time.Second,
// // }

// // func httpGet(u string) ([]byte, int, error) {
// // 	req, _ := http.NewRequest("GET", u, nil)
// // 	res, err := httpClient.Do(req)
// // 	if err != nil {
// // 		return nil, 0, err
// // 	}
// // 	defer res.Body.Close()
// // 	body, _ := io.ReadAll(res.Body)
// // 	return body, res.StatusCode, nil
// // }

// // func waitForWeaviate(base string, maxSeconds int) ([]byte, error) {
// // 	meta := base + "/v1/meta"
// // 	fmt.Println("[CLIENT] Waiting for Weaviate readiness at", meta)

// // 	start := time.Now()
// // 	var lastBody []byte
// // 	var lastCode int
// // 	var lastErr error

// // 	for {
// // 		body, code, err := httpGet(meta)
// // 		lastBody, lastCode, lastErr = body, code, err
// // 		if err == nil && code == 200 && len(body) > 0 {
// // 			fmt.Println("[CLIENT] Weaviate ready.")
// // 			return body, nil
// // 		}
// // 		if time.Since(start) > time.Duration(maxSeconds)*time.Second {
// // 			return nil, fmt.Errorf("timeout waiting %ds (last code=%d err=%v body=%s)",
// // 				maxSeconds, lastCode, lastErr, string(lastBody))
// // 		}
// // 		time.Sleep(500 * time.Millisecond)
// // 	}
// // }

// // func waitForClass(base, className string, timeout time.Duration) error {
// // 	deadline := time.Now().Add(timeout)
// // 	u := base + "/v1/schema/" + className
// // 	for time.Now().Before(deadline) {
// // 		body, code, err := httpGet(u)
// // 		if err == nil && code == 200 && len(body) > 0 {
// // 			return nil
// // 		}
// // 		time.Sleep(300 * time.Millisecond)
// // 	}
// // 	return fmt.Errorf("timeout waiting for class %s to appear in schema", className)
// // }

// // type nodesResp struct {
// // 	Nodes []struct {
// // 		Name   string `json:"name"`
// // 		Status string `json:"status"`
// // 		Shards []struct {
// // 			Class                string `json:"class"`
// // 			Name                 string `json:"name"`
// // 			ObjectCount          int    `json:"objectCount"`
// // 			VectorIndexingStatus string `json:"vectorIndexingStatus"`
// // 			VectorQueueLength    int    `json:"vectorQueueLength"`
// // 		} `json:"shards"`
// // 	} `json:"nodes"`
// // }

// // type queueSnapshot struct {
// // 	TotalQueue   int `json:"total_queue"`
// // 	Shards       int `json:"shards"`
// // 	Indexing     int `json:"indexing"`
// // 	TotalObjects int `json:"total_objects"`
// // }

// // func getQueue(base, className string) (queueSnapshot, error) {
// // 	u, _ := url.Parse(base + "/v1/nodes")
// // 	q := u.Query()
// // 	q.Set("output", "verbose")
// // 	q.Set("collection", className)
// // 	u.RawQuery = q.Encode()

// // 	body, code, err := httpGet(u.String())
// // 	if err != nil {
// // 		return queueSnapshot{}, err
// // 	}
// // 	if code != 200 {
// // 		return queueSnapshot{}, fmt.Errorf("nodes status http %d: %s", code, string(body))
// // 	}

// // 	var r nodesResp
// // 	if err := json.Unmarshal(body, &r); err != nil {
// // 		return queueSnapshot{}, fmt.Errorf("unmarshal nodes: %w (body=%s)", err, string(body))
// // 	}

// // 	snap := queueSnapshot{}
// // 	for _, n := range r.Nodes {
// // 		for _, s := range n.Shards {
// // 			if s.Class != className {
// // 				continue
// // 			}
// // 			snap.Shards++
// // 			snap.TotalQueue += s.VectorQueueLength
// // 			snap.TotalObjects += s.ObjectCount
// // 			if strings.ToUpper(s.VectorIndexingStatus) == "INDEXING" {
// // 				snap.Indexing++
// // 			}
// // 		}
// // 	}
// // 	return snap, nil
// // }

// // type NpyInfo struct {
// // 	Offset int64
// // 	N      int
// // 	D      int
// // }

// // func parseNpyHeader(f *os.File, expectedD int) (*NpyInfo, error) {
// // 	magic := make([]byte, 6)
// // 	if _, err := io.ReadFull(f, magic); err != nil {
// // 		return nil, err
// // 	}
// // 	if !(magic[0] == 0x93 && string(magic[1:]) == "NUMPY") {
// // 		return nil, errors.New("bad npy magic")
// // 	}

// // 	ver := make([]byte, 2)
// // 	if _, err := io.ReadFull(f, ver); err != nil {
// // 		return nil, err
// // 	}
// // 	major := ver[0]

// // 	var headerLen int
// // 	if major == 1 {
// // 		var u16 uint16
// // 		if err := binary.Read(f, binary.LittleEndian, &u16); err != nil {
// // 			return nil, err
// // 		}
// // 		headerLen = int(u16)
// // 	} else {
// // 		var u32 uint32
// // 		if err := binary.Read(f, binary.LittleEndian, &u32); err != nil {
// // 			return nil, err
// // 		}
// // 		headerLen = int(u32)
// // 	}

// // 	header := make([]byte, headerLen)
// // 	if _, err := io.ReadFull(f, header); err != nil {
// // 		return nil, err
// // 	}
// // 	h := string(header)

// // 	if !strings.Contains(h, "'descr': '<f4'") && !strings.Contains(h, "\"descr\": \"<f4\"") {
// // 		return nil, fmt.Errorf("expected <f4 float32; header=%s", h)
// // 	}

// // 	re := regexp.MustCompile(`\(\s*(\d+)\s*,\s*(\d+)\s*\)`)
// // 	m := re.FindStringSubmatch(h)
// // 	if len(m) != 3 {
// // 		return nil, fmt.Errorf("cannot parse shape from header=%s", h)
// // 	}

// // 	var n, d int
// // 	fmt.Sscanf(m[1], "%d", &n)
// // 	fmt.Sscanf(m[2], "%d", &d)

// // 	if expectedD > 0 && d != expectedD {
// // 		return nil, fmt.Errorf("vector dim mismatch: got %d expected %d", d, expectedD)
// // 	}

// // 	off, _ := f.Seek(0, io.SeekCurrent)
// // 	return &NpyInfo{Offset: off, N: n, D: d}, nil
// // }

// // func readVectors(f *os.File, info *NpyInfo, start, count int) ([]float32, int, error) {
// // 	if start < 0 || start >= info.N {
// // 		return nil, 0, fmt.Errorf("start out of range: %d (N=%d)", start, info.N)
// // 	}
// // 	if start+count > info.N {
// // 		count = info.N - start
// // 	}
// // 	if count <= 0 {
// // 		return nil, 0, errors.New("count <= 0")
// // 	}

// // 	dim := info.D
// // 	byteOff := info.Offset + int64(start*dim*4)
// // 	if _, err := f.Seek(byteOff, io.SeekStart); err != nil {
// // 		return nil, 0, err
// // 	}

// // 	raw := make([]byte, count*dim*4)
// // 	if _, err := io.ReadFull(f, raw); err != nil {
// // 		return nil, 0, err
// // 	}

// // 	out := make([]float32, count*dim)
// // 	for i := 0; i < count*dim; i++ {
// // 		u := binary.LittleEndian.Uint32(raw[i*4 : i*4+4])
// // 		out[i] = math.Float32frombits(u)
// // 	}
// // 	return out, count, nil
// // }

// // const idPropName = "doc_id"

// // func mustEnvString(k string) string {
// // 	v := strings.TrimSpace(os.Getenv(k))
// // 	if v == "" {
// // 		fmt.Println("[CLIENT][ERROR] missing env", k)
// // 		os.Exit(2)
// // 	}
// // 	return v
// // }

// // func mustEnvInt(k string) int {
// // 	v := strings.TrimSpace(os.Getenv(k))
// // 	if v == "" {
// // 		fmt.Println("[CLIENT][ERROR] missing env", k)
// // 		os.Exit(2)
// // 	}
// // 	var x int
// // 	_, err := fmt.Sscanf(v, "%d", &x)
// // 	if err != nil {
// // 		fmt.Println("[CLIENT][ERROR] bad int env", k, v)
// // 		os.Exit(3)
// // 	}
// // 	return x
// // }

// // func makeDynamicClass(className string, dynamicThreshold int) *models.Class {
// // 	return &models.Class{
// // 		Class:           className,
// // 		Vectorizer:      "none",
// // 		VectorIndexType: "dynamic",
// // 		VectorIndexConfig: map[string]any{
// // 			"distance":  "dot",
// // 			"threshold": dynamicThreshold,
// // 			"hnsw": map[string]any{
// // 				"ef":             64,
// // 				"efConstruction": 100,
// // 				"maxConnections": 16,
// // 			},
// // 		},
// // 		Properties: []*models.Property{
// // 			{Name: idPropName, DataType: []string{"text"}},
// // 		},
// // 	}
// // }

// // func resetClass(ctx context.Context, client *weaviate.Client, base, className string, dynamicThreshold int) error {
// // 	_ = client.Schema().ClassDeleter().WithClassName(className).Do(ctx)
// // 	time.Sleep(1 * time.Second)

// // 	cls := makeDynamicClass(className, dynamicThreshold)
// // 	if err := client.Schema().ClassCreator().WithClass(cls).Do(ctx); err != nil {
// // 		return fmt.Errorf("class create failed: %w", err)
// // 	}
// // 	return waitForClass(base, className, 60*time.Second)
// // }

// // type TimelinePoint struct {
// // 	AbsSinceFirstRPCSec float64       `json:"abs_since_first_rpc_sec"`
// // 	SinceThresholdSec   float64       `json:"since_threshold_sec"`
// // 	Snapshot            queueSnapshot `json:"snapshot"`
// // }

// // type Output struct {
// // 	TimestampUTC string `json:"timestamp_utc"`
// // 	Worker       string `json:"worker"`
// // 	DataFile     string `json:"data_file"`
// // 	VecDim       int    `json:"vec_dim"`
// // 	Class        string `json:"class"`
// // 	Distance     string `json:"distance"`

// // 	MeasureVecs      int `json:"measure_vecs"`
// // 	StartRow         int `json:"start_row"`
// // 	BatchSize        int `json:"batch_size"`
// // 	DynamicThreshold int `json:"dynamic_threshold"`
// // 	OverallSec       int `json:"overall_sec"`
// // 	RpcSec           int `json:"rpc_sec"`
// // 	IndexTimeoutSec  int `json:"index_timeout_sec"`
// // 	IndexPollMS      int `json:"index_poll_ms"`
// // 	IndexStablePolls int `json:"index_stable_polls"`

// // 	InsertedVecs int     `json:"inserted_vecs"`
// // 	SendSec      float64 `json:"send_sec"`

// // 	ThresholdCrossed       bool          `json:"threshold_crossed"`
// // 	ThresholdCrossAbsSec   float64       `json:"threshold_cross_abs_sec"`
// // 	ThresholdCrossInserted int           `json:"threshold_cross_inserted"`
// // 	ThresholdCrossSnapshot queueSnapshot `json:"threshold_cross_snapshot"`

// // 	IndexComplete         bool          `json:"index_complete"`
// // 	IndexCompleteAbsSec   float64       `json:"index_complete_abs_sec"`
// // 	IndexCompleteSnapshot queueSnapshot `json:"index_complete_snapshot"`

// // 	CPUIndexingSec float64 `json:"cpu_indexing_sec"`

// // 	Timeline []TimelinePoint `json:"timeline"`

// // 	FinalStatus string `json:"final_status"`
// // 	FinalError  string `json:"final_error,omitempty"`
// // }

// // func writeJSON(path string, out Output) {
// // 	b, err := json.MarshalIndent(out, "", "  ")
// // 	if err != nil {
// // 		fmt.Println("[CLIENT][ERROR] marshal json:", err)
// // 		return
// // 	}
// // 	if err := os.WriteFile(path, b, 0644); err != nil {
// // 		fmt.Println("[CLIENT][ERROR] write json:", err)
// // 		return
// // 	}
// // 	fmt.Println("[CLIENT] wrote:", path)
// // }

// // func main() {
// // 	outPath := flag.String("out", "index_time_yandex_ef64.json", "output json path")
// // 	className := flag.String("class", "Yandex10MEF64", "weaviate class name")
// // 	batchSize := flag.Int("batch_size", 32768, "batch size")
// // 	waitSec := flag.Int("wait", 180, "seconds to wait for weaviate readiness")
// // 	overallSec := flag.Int("overall_sec", 25000, "overall time budget seconds")
// // 	rpcSec := flag.Int("rpc_sec", 1800, "per-batch RPC timeout seconds")
// // 	indexTimeoutSec := flag.Int("index_timeout_sec", 5000, "timeout after threshold crossing")
// // 	indexPollMS := flag.Int("index_poll_ms", 1000, "poll interval after threshold crossing")
// // 	indexStablePolls := flag.Int("index_stable_polls", 3, "require 0-queue and 0-indexing for N consecutive polls")
// // 	flag.Parse()

// // 	workerIP := mustEnvString("WORKER_IP")
// // 	restPort := mustEnvString("REST_PORT")
// // 	grpcPort := mustEnvString("GRPC_PORT")
// // 	dataFile := mustEnvString("DATA_FILE")

// // 	vecDim := mustEnvInt("VEC_DIM")
// // 	measureVecs := mustEnvInt("MEASURE_VECS")
// // 	startRow := mustEnvInt("START_ROW")
// // 	dynamicThreshold := mustEnvInt("DYNAMIC_THRESHOLD")

// // 	base := "http://" + workerIP + ":" + restPort

// // 	out := Output{
// // 		TimestampUTC:     time.Now().UTC().Format(time.RFC3339),
// // 		Worker:           base,
// // 		DataFile:         dataFile,
// // 		VecDim:           vecDim,
// // 		Class:            *className,
// // 		Distance:         "dot",
// // 		MeasureVecs:      measureVecs,
// // 		StartRow:         startRow,
// // 		BatchSize:        *batchSize,
// // 		DynamicThreshold: dynamicThreshold,
// // 		OverallSec:       *overallSec,
// // 		RpcSec:           *rpcSec,
// // 		IndexTimeoutSec:  *indexTimeoutSec,
// // 		IndexPollMS:      *indexPollMS,
// // 		IndexStablePolls: *indexStablePolls,
// // 		Timeline:         []TimelinePoint{},
// // 		FinalStatus:      "ok",
// // 	}

// // 	fmt.Println("[CLIENT] target:", base)
// // 	fmt.Println("[CLIENT] grpc:", workerIP+":"+grpcPort)
// // 	fmt.Println("[CLIENT] data:", dataFile)
// // 	fmt.Printf("[CLIENT] vec_dim=%d measure=%d start_row=%d batch_size=%d threshold=%d distance=dot hnsw.ef=64 hnsw.efConstruction=100 hnsw.maxConnections=16\n",
// // 		vecDim, measureVecs, startRow, *batchSize, dynamicThreshold)

// // 	if _, err := waitForWeaviate(base, *waitSec); err != nil {
// // 		out.FinalStatus = "error"
// // 		out.FinalError = err.Error()
// // 		writeJSON(*outPath, out)
// // 		os.Exit(4)
// // 	}

// // 	client, err := weaviate.NewClient(weaviate.Config{
// // 		Host:   workerIP + ":" + restPort,
// // 		Scheme: "http",
// // 		GrpcConfig: &wgrpc.Config{
// // 			Host:    workerIP + ":" + grpcPort,
// // 			Secured: false,
// // 		},
// // 	})
// // 	if err != nil {
// // 		out.FinalStatus = "error"
// // 		out.FinalError = "NewClient: " + err.Error()
// // 		writeJSON(*outPath, out)
// // 		os.Exit(5)
// // 	}

// // 	f, err := os.Open(dataFile)
// // 	if err != nil {
// // 		out.FinalStatus = "error"
// // 		out.FinalError = "open: " + err.Error()
// // 		writeJSON(*outPath, out)
// // 		os.Exit(6)
// // 	}
// // 	defer f.Close()

// // 	info, err := parseNpyHeader(f, vecDim)
// // 	if err != nil {
// // 		out.FinalStatus = "error"
// // 		out.FinalError = "npy: " + err.Error()
// // 		writeJSON(*outPath, out)
// // 		os.Exit(7)
// // 	}
// // 	fmt.Printf("[CLIENT] npy: N=%d D=%d\n", info.N, info.D)

// // 	resetCtx, cancelReset := context.WithTimeout(context.Background(), 5*time.Minute)
// // 	err = resetClass(resetCtx, client, base, *className, dynamicThreshold)
// // 	cancelReset()
// // 	if err != nil {
// // 		out.FinalStatus = "error"
// // 		out.FinalError = "resetClass: " + err.Error()
// // 		writeJSON(*outPath, out)
// // 		os.Exit(8)
// // 	}

// // 	overallCtx, overallCancel := context.WithTimeout(context.Background(), time.Duration(*overallSec)*time.Second)
// // 	defer overallCancel()

// // 	cursor := startRow
// // 	if cursor >= info.N {
// // 		cursor = 0
// // 	}

// // 	inserted := 0
// // 	started := false
// // 	var t0, tLastRPC float64

// // 	for inserted < measureVecs {
// // 		if err := overallCtx.Err(); err != nil {
// // 			out.FinalStatus = "partial"
// // 			out.FinalError = "overall timeout before inserts finished"
// // 			writeJSON(*outPath, out)
// // 			os.Exit(9)
// // 		}

// // 		n := *batchSize
// // 		remaining := measureVecs - inserted
// // 		if n > remaining {
// // 			n = remaining
// // 		}
// // 		if n <= 0 {
// // 			break
// // 		}
// // 		if cursor+n >= info.N {
// // 			cursor = 0
// // 		}

// // 		vecs, got, err := readVectors(f, info, cursor, n)
// // 		if err != nil {
// // 			out.FinalStatus = "error"
// // 			out.FinalError = "readVectors: " + err.Error()
// // 			writeJSON(*outPath, out)
// // 			os.Exit(10)
// // 		}

// // 		objs := make([]*models.Object, 0, got)
// // 		for i := 0; i < got; i++ {
// // 			off := i * vecDim
// // 			v := make([]float32, vecDim)
// // 			copy(v, vecs[off:off+vecDim])
// // 			docID := fmt.Sprintf("row%d_i%d", cursor, i)
// // 			objs = append(objs, &models.Object{
// // 				Class: *className,
// // 				Properties: map[string]any{
// // 					idPropName: docID,
// // 				},
// // 				Vector: v,
// // 			})
// // 		}

// // 		if !started {
// // 			t0 = hrsec()
// // 			started = true
// // 		}

// // 		rpcCtx, rpcCancel := context.WithTimeout(overallCtx, time.Duration(*rpcSec)*time.Second)
// // 		_, err = client.Batch().ObjectsBatcher().WithObjects(objs...).Do(rpcCtx)
// // 		rpcCancel()
// // 		if err != nil {
// // 			out.FinalStatus = "partial"
// // 			out.FinalError = "batch insert: " + err.Error()
// // 			out.InsertedVecs = inserted
// // 			writeJSON(*outPath, out)
// // 			os.Exit(11)
// // 		}

// // 		inserted += got
// // 		cursor += got
// // 		tLastRPC = hrsec()

// // 		if inserted%(100000) == 0 || inserted == measureVecs {
// // 			fmt.Printf("[CLIENT] inserted %d / %d (send_elapsed=%.1fs)\n", inserted, measureVecs, tLastRPC-t0)
// // 		}

// // 		if !out.ThresholdCrossed {
// // 			snap, err := getQueue(base, *className)
// // 			if err == nil && snap.TotalObjects >= dynamicThreshold {
// // 				out.ThresholdCrossed = true
// // 				out.ThresholdCrossAbsSec = tLastRPC - t0
// // 				out.ThresholdCrossInserted = inserted
// // 				out.ThresholdCrossSnapshot = snap
// // 				fmt.Printf("[CLIENT] threshold crossed at inserted=%d abs=%.3fs objects=%d queue=%d indexing=%d\n",
// // 					inserted, out.ThresholdCrossAbsSec, snap.TotalObjects, snap.TotalQueue, snap.Indexing)
// // 			}
// // 		}
// // 	}

// // 	out.InsertedVecs = inserted
// // 	if started {
// // 		out.SendSec = tLastRPC - t0
// // 	}

// // 	if !out.ThresholdCrossed {
// // 		snap, err := getQueue(base, *className)
// // 		if err == nil && snap.TotalObjects >= dynamicThreshold {
// // 			out.ThresholdCrossed = true
// // 			out.ThresholdCrossAbsSec = tLastRPC - t0
// // 			out.ThresholdCrossInserted = inserted
// // 			out.ThresholdCrossSnapshot = snap
// // 		}
// // 	}

// // 	if !out.ThresholdCrossed {
// // 		out.FinalStatus = "error"
// // 		out.FinalError = "dynamic threshold was never crossed"
// // 		writeJSON(*outPath, out)
// // 		os.Exit(12)
// // 	}

// // 	fmt.Println("[CLIENT] monitoring indexing completion...")
// // 	stable := 0
// // 	deadline := time.Now().Add(time.Duration(*indexTimeoutSec) * time.Second)
// // 	poll := time.Duration(*indexPollMS) * time.Millisecond

// // 	for time.Now().Before(deadline) {
// // 		snap, err := getQueue(base, *className)
// // 		nowAbs := hrsec() - t0

// // 		if err == nil {
// // 			out.Timeline = append(out.Timeline, TimelinePoint{
// // 				AbsSinceFirstRPCSec: nowAbs,
// // 				SinceThresholdSec:   nowAbs - out.ThresholdCrossAbsSec,
// // 				Snapshot:            snap,
// // 			})

// // 			fmt.Printf("[CLIENT] poll abs=%.1fs since_threshold=%.1fs objects=%d queue=%d indexing=%d\n",
// // 				nowAbs, nowAbs-out.ThresholdCrossAbsSec, snap.TotalObjects, snap.TotalQueue, snap.Indexing)

// // 			if snap.TotalQueue == 0 && snap.Indexing == 0 {
// // 				stable++
// // 				if stable >= *indexStablePolls {
// // 					out.IndexComplete = true
// // 					out.IndexCompleteAbsSec = nowAbs
// // 					out.IndexCompleteSnapshot = snap
// // 					out.CPUIndexingSec = out.IndexCompleteAbsSec - out.ThresholdCrossAbsSec
// // 					out.FinalStatus = "ok"
// // 					writeJSON(*outPath, out)
// // 					fmt.Printf("[CLIENT] indexing complete: cpu_indexing_sec=%.3f\n", out.CPUIndexingSec)
// // 					return
// // 				}
// // 			} else {
// // 				stable = 0
// // 			}
// // 		}

// // 		time.Sleep(poll)
// // 	}

// // 	out.IndexComplete = false
// // 	out.CPUIndexingSec = 0
// // 	out.FinalStatus = "partial"
// // 	out.FinalError = fmt.Sprintf("indexing did not finish within %d seconds after threshold crossing", *indexTimeoutSec)
// // 	writeJSON(*outPath, out)
// // 	os.Exit(13)
// // }