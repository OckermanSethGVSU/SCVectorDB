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
	"runtime"
	"strings"
	"time"

	"github.com/weaviate/weaviate-go-client/v5/weaviate"
	wgrpc "github.com/weaviate/weaviate-go-client/v5/weaviate/grpc"
	"github.com/weaviate/weaviate/entities/models"
)

// clearProxy removes ALL proxy env vars so HTTP stays direct on HPC networks.
func clearProxy() {
	for _, k := range []string{
		"http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY",
		"no_proxy", "NO_PROXY",
	} {
		os.Unsetenv(k)
	}
	os.Setenv("NO_PROXY", "*")
	os.Setenv("no_proxy", "*")
}

func hrsec() float64 { return float64(time.Now().UnixNano()) / 1e9 }

// --------------- HTTP helpers (proxy disabled) ---------------

var httpClient = &http.Client{
	Transport: &http.Transport{
		Proxy: nil,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:        200,
		MaxIdleConnsPerHost: 200,
		IdleConnTimeout:     90 * time.Second,
	},
	Timeout: 30 * time.Second,
}

func httpGet(u string) ([]byte, int, error) {
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return nil, 0, err
	}
	res, err := httpClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	return body, res.StatusCode, nil
}

func httpDoJSON(method, u string, payload any) ([]byte, int, error) {
	var body io.Reader
	if payload != nil {
		b, err := json.Marshal(payload)
		if err != nil {
			return nil, 0, err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, u, body)
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
	rb, _ := io.ReadAll(res.Body)
	return rb, res.StatusCode, nil
}

func waitForWeaviate(base string, maxSeconds int) error {
	meta := base + "/v1/meta"
	start := time.Now()
	var lastCode int
	var lastErr error
	for {
		_, code, err := httpGet(meta)
		lastCode, lastErr = code, err
		if err == nil && code == 200 {
			return nil
		}
		if time.Since(start) > time.Duration(maxSeconds)*time.Second {
			return fmt.Errorf("timeout %ds (last code=%d err=%v)", maxSeconds, lastCode, lastErr)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func waitForClass(base, className string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	u := base + "/v1/schema/" + className
	for time.Now().Before(deadline) {
		_, code, err := httpGet(u)
		if err == nil && code == 200 {
			return nil
		}
		time.Sleep(300 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for class %s", className)
}

func waitForClassAbsent(base, className string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	u := base + "/v1/schema/" + className
	for time.Now().Before(deadline) {
		_, code, err := httpGet(u)
		if err != nil || code == 404 {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

// --------------- Cluster / queue status ---------------

type nodesResp struct {
	Nodes []struct {
		Shards []struct {
			Class                string `json:"class"`
			VectorIndexingStatus string `json:"vectorIndexingStatus"`
			VectorQueueLength    int    `json:"vectorQueueLength"`
		} `json:"shards"`
	} `json:"nodes"`
}

type queueSnapshot struct {
	TotalQueue int
	Shards     int
	Indexing   int
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
		return queueSnapshot{}, fmt.Errorf("nodes http %d: %s", code, string(body))
	}

	var r nodesResp
	if err := json.Unmarshal(body, &r); err != nil {
		return queueSnapshot{}, fmt.Errorf("unmarshal: %w", err)
	}

	snap := queueSnapshot{}
	for _, n := range r.Nodes {
		for _, s := range n.Shards {
			if s.Class != className {
				continue
			}
			snap.Shards++
			snap.TotalQueue += s.VectorQueueLength
			if strings.ToUpper(s.VectorIndexingStatus) == "INDEXING" {
				snap.Indexing++
			}
		}
	}
	return snap, nil
}

func waitForQueueDrain(base, className string, poll time.Duration, stablePolls int, timeout time.Duration) (queueSnapshot, int, error) {
	start := time.Now()
	stable := 0
	polls := 0
	var last queueSnapshot
	for {
		if time.Since(start) > timeout {
			return last, polls, fmt.Errorf("drain timeout %s (queue=%d indexing=%d)",
				timeout, last.TotalQueue, last.Indexing)
		}
		snap, err := getQueue(base, className)
		polls++
		if err != nil {
			time.Sleep(poll)
			continue
		}
		last = snap
		if snap.TotalQueue == 0 && snap.Indexing == 0 {
			stable++
			if stable >= stablePolls {
				return snap, polls, nil
			}
		} else {
			stable = 0
		}
		time.Sleep(poll)
	}
}

// --------------- NPY parsing ---------------

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

	var headerLen int
	if ver[0] == 1 {
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
		return nil, fmt.Errorf("expected <f4; header=%s", h)
	}

	re := regexp.MustCompile(`\(\s*(\d+)\s*,\s*(\d+)\s*\)`)
	m := re.FindStringSubmatch(h)
	if len(m) != 3 {
		return nil, fmt.Errorf("shape parse fail: %s", h)
	}

	var n, d int
	fmt.Sscanf(m[1], "%d", &n)
	fmt.Sscanf(m[2], "%d", &d)
	if expectedD > 0 && d != expectedD {
		return nil, fmt.Errorf("dim mismatch: %d vs %d", d, expectedD)
	}

	off, _ := f.Seek(0, io.SeekCurrent)
	return &NpyInfo{Offset: off, N: n, D: d}, nil
}

func readVectors(f *os.File, info *NpyInfo, start, count int) ([]float32, int, error) {
	if start < 0 {
		start = 0
	}
	if start >= info.N {
		start = start % info.N
	}
	avail := info.N - start
	if count > avail {
		count = avail
	}
	if count <= 0 {
		return nil, 0, fmt.Errorf("no vectors (start=%d N=%d)", start, info.N)
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
	for i := range out {
		u := binary.LittleEndian.Uint32(raw[i*4 : i*4+4])
		out[i] = math.Float32frombits(u)
	}
	return out, count, nil
}

// --------------- Schema ---------------

const idPropName = "doc_id"

func makeClassPayload(className, indexType string, shardCount int) map[string]any {
	cls := map[string]any{
		"class":           className,
		"vectorizer":      "none",
		"vectorIndexType": indexType,
		"properties": []map[string]any{
			{"name": idPropName, "dataType": []string{"text"}},
		},
	}
	if indexType == "hnsw" {
		cls["vectorIndexConfig"] = map[string]any{
			"distance": "cosine", "maxConnections": 16, "efConstruction": 128,
		}
	} else {
		cls["vectorIndexConfig"] = map[string]any{"distance": "cosine"}
	}
	if shardCount > 0 {
		cls["shardingConfig"] = map[string]any{"desiredCount": shardCount}
	}
	return cls
}

func resetClassREST(base, className, indexType string, shardCount int, timeout time.Duration) error {
	_, _, _ = httpDoJSON("DELETE", base+"/v1/schema/"+className, nil)
	time.Sleep(2 * time.Second)

	payload := makeClassPayload(className, indexType, shardCount)
	body, code, err := httpDoJSON("POST", base+"/v1/schema", payload)
	if err != nil {
		return fmt.Errorf("schema POST: %w", err)
	}
	if code != 200 && code != 201 {
		time.Sleep(3 * time.Second)
		_, _, _ = httpDoJSON("DELETE", base+"/v1/schema/"+className, nil)
		time.Sleep(3 * time.Second)
		body, code, err = httpDoJSON("POST", base+"/v1/schema", payload)
		if err != nil {
			return fmt.Errorf("schema POST retry: %w", err)
		}
		if code != 200 && code != 201 {
			return fmt.Errorf("schema create http %d: %s", code, string(body))
		}
	}
	return waitForClass(base, className, timeout)
}

// --------------- Env helpers ---------------

func mustEnvStr(k string) string {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		fmt.Fprintf(os.Stderr, "[CLIENT][FATAL] missing env %s\n", k)
		os.Exit(2)
	}
	return v
}

func mustEnvInt(k string) int {
	s := mustEnvStr(k)
	var x int
	if _, err := fmt.Sscanf(s, "%d", &x); err != nil {
		fmt.Fprintf(os.Stderr, "[CLIENT][FATAL] bad int env %s=%q\n", k, s)
		os.Exit(3)
	}
	return x
}

func envIntDefault(k string, def int) int {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return def
	}
	var x int
	if _, err := fmt.Sscanf(v, "%d", &x); err != nil {
		return def
	}
	return x
}

func envStrDefault(k, def string) string {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return def
	}
	return v
}

// --------------- Output ---------------

type ResultEntry struct {
	BatchSize        int     `json:"batch_size"`
	MeasureVecs      int     `json:"measure_vecs"`
	SendSec          float64 `json:"send_sec"`
	DrainSec         float64 `json:"drain_sec"`
	EndToEndSec      float64 `json:"end_to_end_sec"`
	SendVPS          float64 `json:"send_vps"`
	EndToEndVPS      float64 `json:"end_to_end_vps"`
	QueuePolls       int     `json:"queue_drain_polls,omitempty"`
	PostSendQueue    int     `json:"post_send_queue,omitempty"`
	PostSendIndexing int     `json:"post_send_indexing,omitempty"`
	PostSendShards   int     `json:"post_send_shards,omitempty"`
	FinalQueue       int     `json:"final_queue,omitempty"`
	FinalIndexing    int     `json:"final_indexing,omitempty"`
	FinalShards      int     `json:"final_shards,omitempty"`
	Status           string  `json:"status"`
	Error            string  `json:"error,omitempty"`
}

type Output struct {
	TimestampUTC    string        `json:"timestamp_utc"`
	Worker          string        `json:"worker"`
	DataFile        string        `json:"data_file"`
	VecDim          int           `json:"vec_dim"`
	Class           string        `json:"class"`
	IndexType       string        `json:"index_type"`
	ShardCount      int           `json:"shard_count"`
	MeasureVecs     int           `json:"measure_vecs"`
	StartRow        int           `json:"start_row"`
	TotalWorkers    int           `json:"total_workers"`
	ClientRank      int           `json:"client_rank"`
	ClientsN        int           `json:"clients_n"`
	MyStartRow      int           `json:"my_start_row"`
	MyVecs          int           `json:"my_vecs"`
	InsertedCount   int           `json:"inserted_count"`
	ErrorCount      int           `json:"error_count"`
	BsPow           int           `json:"bs_pow"`
	OverallSec      int           `json:"overall_sec"`
	RpcSec          int           `json:"rpc_sec"`
	QueueTimeoutSec int           `json:"queue_timeout_sec"`
	Results         []ResultEntry `json:"results"`
	FinalStatus     string        `json:"final_status"`
}

func writeJSON(path string, out Output) {
	b, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "[CLIENT][ERROR] marshal: %v\n", err)
		return
	}
	_ = os.WriteFile(path, b, 0644)
}

// --------------- main ---------------

func main() {
	clearProxy()

	// CRITICAL: Cap GOMAXPROCS to prevent thread explosion.
	// Aurora nodes have 208 cores — default GOMAXPROCS=208 means ~208 OS
	// threads PER process.  With 512 processes that's 106K threads → crash.
	// GOMAXPROCS=2 gives ~4 OS threads per process → 512*4 = 2048, safe.
	maxProcs := envIntDefault("GOMAXPROCS_OVERRIDE", 2)
	runtime.GOMAXPROCS(maxProcs)

	outPath := flag.String("out", "client.json", "output json path")
	className := flag.String("class", "PES2O", "weaviate class name")
	waitSec := flag.Int("wait", 300, "seconds to wait for readiness")
	maxRetries := flag.Int("retries", 3, "max retries per failed batch")
	flag.Parse()

	if envClass := envStrDefault("CLASS_NAME", ""); envClass != "" {
		*className = envClass
	}

	workerIP := mustEnvStr("WORKER_IP")
	restPort := mustEnvStr("REST_PORT")
	grpcPort := mustEnvStr("GRPC_PORT")
	dataFile := mustEnvStr("DATA_FILE")

	vecDim := mustEnvInt("VEC_DIM")
	measureVecsTotal := mustEnvInt("MEASURE_VECS")
	startRow := mustEnvInt("START_ROW")

	indexType := envStrDefault("INDEX_TYPE", "flat")
	shardCount := envIntDefault("SHARD_COUNT", 0)
	totalWorkers := envIntDefault("TOTAL_WORKERS", shardCount)

	clientsN := envIntDefault("CLIENTS_N", 1)
	rank := envIntDefault("CLIENT_RANK", 0)

	bsPow := envIntDefault("BS_POW", 8)
	batchSize := 1 << bsPow

	rpcSec := envIntDefault("CLIENT_RPC_TIMEOUT_SEC", 1800)
	overallSec := envIntDefault("CLIENT_OVERALL_SEC", 7200)
	queueTimeoutSec := envIntDefault("QUEUE_TIMEOUT_SEC", 7200)

	base := "http://" + workerIP + ":" + restPort

	// Compute per-rank split
	baseCount := measureVecsTotal / clientsN
	rem := measureVecsTotal % clientsN
	myCount := baseCount
	if rank < rem {
		myCount++
	}
	myStart := startRow
	for r := 0; r < rank; r++ {
		c := baseCount
		if r < rem {
			c++
		}
		myStart += c
	}

	out := Output{
		TimestampUTC:    time.Now().UTC().Format(time.RFC3339),
		Worker:          base,
		DataFile:        dataFile,
		VecDim:          vecDim,
		Class:           *className,
		IndexType:       indexType,
		ShardCount:      shardCount,
		MeasureVecs:     measureVecsTotal,
		StartRow:        startRow,
		TotalWorkers:    totalWorkers,
		ClientRank:      rank,
		ClientsN:        clientsN,
		MyStartRow:      myStart,
		MyVecs:          myCount,
		BsPow:           bsPow,
		OverallSec:      overallSec,
		RpcSec:          rpcSec,
		QueueTimeoutSec: queueTimeoutSec,
		FinalStatus:     "ok",
	}

	fmt.Printf("[CLIENT rank=%d] target=%s class=%s totalVecs=%d myVecs=%d batchSize=%d GOMAXPROCS=%d\n",
		rank, base, *className, measureVecsTotal, myCount, batchSize, maxProcs)

	// Wait for Weaviate
	if err := waitForWeaviate(base, *waitSec); err != nil {
		out.FinalStatus = "error"
		out.Results = []ResultEntry{{
			BatchSize: batchSize, MeasureVecs: measureVecsTotal,
			Status: "error", Error: err.Error(),
		}}
		writeJSON(*outPath, out)
		os.Exit(4)
	}
	fmt.Printf("[CLIENT rank=%d] weaviate reachable\n", rank)

	// Open vectors
	f, err := os.Open(dataFile)
	if err != nil {
		out.FinalStatus = "error"
		out.Results = []ResultEntry{{
			BatchSize: batchSize, MeasureVecs: measureVecsTotal,
			Status: "error", Error: "open: " + err.Error(),
		}}
		writeJSON(*outPath, out)
		os.Exit(6)
	}
	defer f.Close()

	info, err := parseNpyHeader(f, vecDim)
	if err != nil {
		out.FinalStatus = "error"
		out.Results = []ResultEntry{{
			BatchSize: batchSize, MeasureVecs: measureVecsTotal,
			Status: "error", Error: "npy: " + err.Error(),
		}}
		writeJSON(*outPath, out)
		os.Exit(7)
	}
	fmt.Printf("[CLIENT rank=%d] npy: N=%d D=%d\n", rank, info.N, info.D)

	if myStart >= info.N {
		myStart = myStart % info.N
	}
	out.MyStartRow = myStart

	// Schema reset (rank 0 only); other ranks wait for the fresh class.
	if rank == 0 {
		fmt.Printf("[CLIENT rank=0] resetting schema: class=%s index=%s shards=%d\n",
			*className, indexType, shardCount)
		if err := resetClassREST(base, *className, indexType, shardCount, 3*time.Minute); err != nil {
			out.FinalStatus = "error"
			out.Results = []ResultEntry{{
				BatchSize: batchSize, MeasureVecs: measureVecsTotal,
				Status: "error", Error: "resetClass: " + err.Error(),
			}}
			writeJSON(*outPath, out)
			os.Exit(8)
		}
		fmt.Printf("[CLIENT rank=0] schema ready\n")
	} else {
		fmt.Printf("[CLIENT rank=%d] waiting for rank-0 schema reset …\n", rank)
		waitForClassAbsent(base, *className, 30*time.Second)
		if err := waitForClass(base, *className, 5*time.Minute); err != nil {
			out.FinalStatus = "error"
			out.Results = []ResultEntry{{
				BatchSize: batchSize, MeasureVecs: measureVecsTotal,
				Status: "error", Error: "waitForClass: " + err.Error(),
			}}
			writeJSON(*outPath, out)
			os.Exit(9)
		}
		fmt.Printf("[CLIENT rank=%d] schema ready\n", rank)
	}

	// Create weaviate gRPC+REST client
	client, err := weaviate.NewClient(weaviate.Config{
		Host:   workerIP + ":" + restPort,
		Scheme: "http",
		GrpcConfig: &wgrpc.Config{
			Host:    workerIP + ":" + grpcPort,
			Secured: false,
		},
	})
	if err != nil {
		out.FinalStatus = "error"
		out.Results = []ResultEntry{{
			BatchSize: batchSize, MeasureVecs: measureVecsTotal,
			Status: "error", Error: "NewClient: " + err.Error(),
		}}
		writeJSON(*outPath, out)
		os.Exit(10)
	}

	overallCtx, cancel := context.WithTimeout(context.Background(), time.Duration(overallSec)*time.Second)
	defer cancel()

	// --------------- insertion loop ---------------
	result := ResultEntry{
		BatchSize:   batchSize,
		MeasureVecs: measureVecsTotal,
		Status:      "ok",
	}

	cursor := myStart
	inserted := 0
	errorCount := 0
	started := false
	var t0, tLastRPC float64

	for inserted < myCount {
		if err := overallCtx.Err(); err != nil {
			result.Status = "partial"
			result.Error = appendStr(result.Error, "overall timeout: "+err.Error())
			break
		}

		want := batchSize
		if remaining := myCount - inserted; want > remaining {
			want = remaining
		}

		if cursor >= info.N {
			cursor = cursor % info.N
		}
		readCount := want
		if cursor+readCount > info.N {
			readCount = info.N - cursor
		}

		vecs, got, err := readVectors(f, info, cursor, readCount)
		if err != nil {
			result.Status = "error"
			result.Error = appendStr(result.Error, "readVectors: "+err.Error())
			break
		}
		if got == 0 {
			cursor = 0
			continue
		}

		objs := make([]*models.Object, 0, got)
		for i := 0; i < got; i++ {
			off := i * vecDim
			v := make([]float32, vecDim)
			copy(v, vecs[off:off+vecDim])
			docID := fmt.Sprintf("c%d_r%d_row%d_i%d", clientsN, rank, cursor+i, inserted+i)
			objs = append(objs, &models.Object{
				Class: *className,
				Properties: map[string]any{
					idPropName: docID,
				},
				Vector: v,
			})
		}

		if !started {
			t0 = hrsec()
			started = true
		}

		var batchErr error
		for attempt := 0; attempt <= *maxRetries; attempt++ {
			rpcCtx, rpcCancel := context.WithTimeout(overallCtx, time.Duration(rpcSec)*time.Second)
			resp, err := client.Batch().ObjectsBatcher().WithObjects(objs...).Do(rpcCtx)
			rpcCancel()

			if err != nil {
				batchErr = err
				if attempt < *maxRetries {
					backoff := time.Duration(1<<uint(attempt)) * time.Second
					fmt.Printf("[CLIENT rank=%d] retry %d/%d: %v (backoff %s)\n",
						rank, attempt+1, *maxRetries, err, backoff)
					time.Sleep(backoff)
					continue
				}
				break
			}

			objErrs := 0
			if resp != nil {
				for _, r := range resp {
					if r.Result != nil && r.Result.Errors != nil {
						for _, e := range r.Result.Errors.Error {
							if e != nil {
								objErrs++
							}
						}
					}
				}
			}
			if objErrs > 0 {
				errorCount += objErrs
				fmt.Printf("[CLIENT rank=%d] %d object errors in batch\n", rank, objErrs)
			}
			batchErr = nil
			break
		}

		if batchErr != nil {
			result.Status = "partial"
			result.Error = appendStr(result.Error, fmt.Sprintf("batch at cursor=%d: %v", cursor, batchErr))
			break
		}

		inserted += got
		cursor += got
		tLastRPC = hrsec()

		if (inserted/batchSize)%10 == 0 || inserted >= myCount {
			elapsed := tLastRPC - t0
			vps := 0.0
			if elapsed > 0 {
				vps = float64(inserted) / elapsed
			}
			fmt.Printf("[CLIENT rank=%d] %d/%d (%.1f%%) %.0f vecs/s\n",
				rank, inserted, myCount, 100*float64(inserted)/float64(myCount), vps)
		}
	}

	out.InsertedCount = inserted
	out.ErrorCount = errorCount

	if !started {
		out.FinalStatus = "error"
		result.Status = "error"
		result.Error = appendStr(result.Error, "no inserts performed")
		out.Results = []ResultEntry{result}
		writeJSON(*outPath, out)
		os.Exit(11)
	}

	sendSec := tLastRPC - t0
	result.SendSec = sendSec
	if sendSec > 0 {
		result.SendVPS = float64(measureVecsTotal) / sendSec
	}

	fmt.Printf("[CLIENT rank=%d] send done: %d vecs in %.2fs\n", rank, inserted, sendSec)

	if rank == 0 {
		if snap, err := getQueue(base, *className); err == nil {
			result.PostSendQueue = snap.TotalQueue
			result.PostSendIndexing = snap.Indexing
			result.PostSendShards = snap.Shards
			fmt.Printf("[CLIENT rank=0] post-send: queue=%d indexing=%d shards=%d\n",
				snap.TotalQueue, snap.Indexing, snap.Shards)
		}

		qTimeout := time.Duration(queueTimeoutSec) * time.Second
		poll := 500 * time.Millisecond

		fmt.Printf("[CLIENT rank=0] waiting for queue drain (timeout=%s) …\n", qTimeout)
		qSnap, qPolls, qErr := waitForQueueDrain(base, *className, poll, 3, qTimeout)
		result.QueuePolls = qPolls
		result.FinalQueue = qSnap.TotalQueue
		result.FinalIndexing = qSnap.Indexing
		result.FinalShards = qSnap.Shards

		tEnd := hrsec()
		result.DrainSec = tEnd - tLastRPC
		result.EndToEndSec = tEnd - t0

		if qErr != nil {
			if result.Status == "ok" {
				result.Status = "partial"
			}
			result.Error = appendStr(result.Error, "drain: "+qErr.Error())
		}

		fmt.Printf("[CLIENT rank=0] drain=%.2fs  end_to_end=%.2fs\n", result.DrainSec, result.EndToEndSec)
	} else {
		tEnd := hrsec()
		result.EndToEndSec = tEnd - t0
	}

	if result.EndToEndSec > 0 {
		result.EndToEndVPS = float64(measureVecsTotal) / result.EndToEndSec
	}

	out.Results = []ResultEntry{result}

	if result.Status != "ok" {
		out.FinalStatus = result.Status
	}

	writeJSON(*outPath, out)
	fmt.Printf("[CLIENT rank=%d] wrote %s\n", rank, *outPath)

	if out.FinalStatus == "error" {
		os.Exit(12)
	}
	if out.FinalStatus == "partial" {
		os.Exit(13)
	}
}

func appendStr(existing, addition string) string {
	if existing == "" {
		return addition
	}
	return existing + " | " + addition
}