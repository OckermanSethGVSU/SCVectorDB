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

// -------------------- HTTP helper --------------------

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
	Timeout: 10 * time.Second,
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

// wait until /v1/schema/<class> succeeds (helps after rapid delete/create)
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

// -------------------- Nodes / queue drain --------------------

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
			return last, polls, fmt.Errorf("queue drain timeout after %s (last totalQueue=%d shards=%d indexing=%d)",
				timeout.String(), last.TotalQueue, last.Shards, last.Indexing)
		}

		snap, err := getQueue(base, className)
		polls++
		if err != nil {
			last = snap
			time.Sleep(poll)
			continue
		}
		last = snap

		// Drain definition: no queued vectors AND nothing actively indexing.
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

// -------------------- NPY parsing (streaming) --------------------

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

// -------------------- schema helpers --------------------

const idPropName = "doc_id"

func mustEnvString(k string) string {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		fmt.Println("[CLIENT][ERROR] missing env", k)
		os.Exit(2)
	}
	return v
}

func mustEnvInt(k string) int {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		fmt.Println("[CLIENT][ERROR] missing env", k)
		os.Exit(2)
	}
	var x int
	_, err := fmt.Sscanf(v, "%d", &x)
	if err != nil {
		fmt.Println("[CLIENT][ERROR] bad int env", k, v)
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
	_, err := fmt.Sscanf(v, "%d", &x)
	if err != nil {
		return def
	}
	return x
}

func makeDynamicClass(className string, dynamicThreshold int) *models.Class {
	return &models.Class{
		Class:           className,
		Vectorizer:      "none",
		VectorIndexType: "dynamic",
		VectorIndexConfig: map[string]any{
			"distance":  "cosine",
			"threshold": dynamicThreshold,
		},
		Properties: []*models.Property{
			{Name: idPropName, DataType: []string{"text"}},
		},
	}
}

func makeHNSWClass(className string) *models.Class {
	return &models.Class{
		Class:           className,
		Vectorizer:      "none",
		VectorIndexType: "hnsw",
		VectorIndexConfig: map[string]any{
			"distance":               "cosine",
			"maxConnections":         16,
			"efConstruction":         128,
			"cleanupIntervalSeconds": 300,
		},
		Properties: []*models.Property{
			{Name: idPropName, DataType: []string{"text"}},
		},
	}
}

func resetClassOnce(ctx context.Context, client *weaviate.Client, base, className string, dynamicThreshold int) error {
	_ = client.Schema().ClassDeleter().WithClassName(className).Do(ctx)

	// Try dynamic first (flat behavior with high threshold)
	cls := makeDynamicClass(className, dynamicThreshold)
	err := client.Schema().ClassCreator().WithClass(cls).Do(ctx)
	if err == nil {
		return waitForClass(base, className, 60*time.Second)
	}

	// Known failure: async indexing disabled -> dynamic rejected
	msg := err.Error()
	if strings.Contains(msg, "dynamic index can only be created when async indexing is enabled") {
		fmt.Println("[CLIENT][WARN] dynamic rejected (async indexing off?). Retrying, then fallback to HNSW.")
		time.Sleep(2 * time.Second)

		_ = client.Schema().ClassDeleter().WithClassName(className).Do(ctx)
		time.Sleep(500 * time.Millisecond)

		err2 := client.Schema().ClassCreator().WithClass(makeDynamicClass(className, dynamicThreshold)).Do(ctx)
		if err2 == nil {
			return waitForClass(base, className, 60*time.Second)
		}

		fmt.Println("[CLIENT][WARN] dynamic still failing; falling back to HNSW:", err2.Error())
		_ = client.Schema().ClassDeleter().WithClassName(className).Do(ctx)
		time.Sleep(500 * time.Millisecond)
		if err3 := client.Schema().ClassCreator().WithClass(makeHNSWClass(className)).Do(ctx); err3 != nil {
			return fmt.Errorf("dynamic failed (%v); fallback hnsw failed (%v)", err2, err3)
		}
		return waitForClass(base, className, 60*time.Second)
	}

	// Other errors: delete+recreate once
	_ = client.Schema().ClassDeleter().WithClassName(className).Do(ctx)
	time.Sleep(500 * time.Millisecond)
	if err4 := client.Schema().ClassCreator().WithClass(cls).Do(ctx); err4 != nil {
		return err4
	}
	return waitForClass(base, className, 60*time.Second)
}

func resetClassWithRetry(client *weaviate.Client, base, className string, dynamicThreshold int, timeout time.Duration) error {
	backoff := []time.Duration{1 * time.Second, 2 * time.Second, 5 * time.Second, 10 * time.Second}
	var lastErr error
	for attempt := 0; attempt < len(backoff)+1; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		err := resetClassOnce(ctx, client, base, className, dynamicThreshold)
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		if attempt < len(backoff) {
			time.Sleep(backoff[attempt])
		}
	}
	return lastErr
}

// -------------------- results --------------------

type ResultRow struct {
	BatchSize   int `json:"batch_size"`
	MeasureVecs int `json:"measure_vecs"`

	SendSec     float64 `json:"send_sec"`
	DrainSec    float64 `json:"drain_sec"`
	EndToEndSec float64 `json:"end_to_end_sec"`

	SendVPS     float64 `json:"send_vps"`
	EndToEndVPS float64 `json:"end_to_end_vps"`

	QueueDrainPolls int `json:"queue_drain_polls"`

	PostSendQueue    int `json:"post_send_queue"`
	PostSendIndexing int `json:"post_send_indexing"`
	PostSendShards   int `json:"post_send_shards"`

	FinalQueue    int `json:"final_queue"`
	FinalIndexing int `json:"final_indexing"`
	FinalShards   int `json:"final_shards"`

	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

type Output struct {
	TimestampUTC string `json:"timestamp_utc"`
	Worker       string `json:"worker"`
	DataFile     string `json:"data_file"`
	VecDim       int    `json:"vec_dim"`

	MinPow int    `json:"minpow"`
	MaxPow int    `json:"maxpow"`
	Class  string `json:"class"`

	MeasureVecs int `json:"measure_vecs"`
	StartRow    int `json:"start_row"`

	PropertyKey string `json:"property_key"`

	DynamicThreshold int `json:"dynamic_threshold"`

	OverallSec int `json:"overall_sec"`
	RpcSec     int `json:"rpc_sec"`

	QueueTimeoutSec  int `json:"queue_timeout_sec"`
	QueuePollMS      int `json:"queue_poll_ms"`
	QueueStablePolls int `json:"queue_stable_polls"`

	Results []ResultRow `json:"results"`

	FinalStatus string `json:"final_status"`
	FinalError  string `json:"final_error,omitempty"`
}

func writeJSON(path string, out Output) {
	b, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		fmt.Println("[CLIENT][ERROR] marshal json:", err)
		return
	}
	if err := os.WriteFile(path, b, 0644); err != nil {
		fmt.Println("[CLIENT][ERROR] write json:", err)
		return
	}
	fmt.Println("[CLIENT] wrote:", path)
}

// -------------------- main --------------------

func main() {
	outPath := flag.String("out", "bs_sweep.json", "output json path")
	minPow := flag.Int("minpow", 2, "min batch size power-of-two (2^minpow)")
	maxPow := flag.Int("maxpow", 15, "max batch size power-of-two (2^maxpow)")
	className := flag.String("class", "PES2O", "weaviate class name")
	waitSec := flag.Int("wait", 180, "seconds to wait for weaviate readiness")

	overallSec := flag.Int("overall_sec", 20000, "overall time budget seconds")
	rpcSec := flag.Int("rpc_sec", 1800, "per-batch RPC timeout seconds")

	queueTimeoutSec := flag.Int("queue_timeout_sec", 20000, "per-batchsize queue drain timeout seconds")
	queuePollMS := flag.Int("queue_poll_ms", 500, "poll interval for /v1/nodes in milliseconds")
	queueStablePolls := flag.Int("queue_stable_polls", 3, "require 0-queue for N consecutive polls")

	flag.Parse()

	workerIP := mustEnvString("WORKER_IP")
	restPort := mustEnvString("REST_PORT")
	grpcPort := mustEnvString("GRPC_PORT")
	dataFile := mustEnvString("DATA_FILE")

	vecDim := mustEnvInt("VEC_DIM")
	measureVecs := mustEnvInt("MEASURE_VECS")
	startRow := mustEnvInt("START_ROW")

	dynamicThreshold := envIntDefault("DYNAMIC_THRESHOLD", 10001000)

	base := "http://" + workerIP + ":" + restPort

	out := Output{
		TimestampUTC:     time.Now().UTC().Format(time.RFC3339),
		Worker:           base,
		DataFile:         dataFile,
		VecDim:           vecDim,
		MinPow:           *minPow,
		MaxPow:           *maxPow,
		Class:            *className,
		MeasureVecs:      measureVecs,
		StartRow:         startRow,
		PropertyKey:      idPropName,
		DynamicThreshold: dynamicThreshold,
		OverallSec:       *overallSec,
		RpcSec:           *rpcSec,
		QueueTimeoutSec:  *queueTimeoutSec,
		QueuePollMS:      *queuePollMS,
		QueueStablePolls: *queueStablePolls,
		Results:          []ResultRow{},
		FinalStatus:      "ok",
	}

	fmt.Println("[CLIENT] target:", base)
	fmt.Println("[CLIENT] grpc:", workerIP+":"+grpcPort)
	fmt.Println("[CLIENT] data:", dataFile)
	fmt.Printf("[CLIENT] vec_dim=%d measure=%d start_row=%d\n", vecDim, measureVecs, startRow)
	fmt.Printf("[CLIENT] dynamic_threshold=%d\n", dynamicThreshold)

	if _, err := waitForWeaviate(base, *waitSec); err != nil {
		out.FinalStatus = "error"
		out.FinalError = err.Error()
		writeJSON(*outPath, out)
		os.Exit(4)
	}

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

	overallCtx, overallCancel := context.WithTimeout(context.Background(), time.Duration(*overallSec)*time.Second)
	defer overallCancel()

	batchSizes := make([]int, 0, (*maxPow-*minPow)+1)
	for p := *minPow; p <= *maxPow; p++ {
		batchSizes = append(batchSizes, 1<<p)
	}

	anyPartial := false

	for _, bs := range batchSizes {
		if err := overallCtx.Err(); err != nil {
			anyPartial = true
			out.FinalStatus = "partial"
			out.FinalError = "overall time budget exhausted"
			writeJSON(*outPath, out)
			return
		}

		row := ResultRow{BatchSize: bs, Status: "ok"}
		fmt.Println("[CLIENT] ==============================")
		fmt.Printf("[CLIENT] batch_size=%d  measure_vecs=%d\n", bs, measureVecs)
		fmt.Println("[CLIENT] resetting class:", *className)

		if err := resetClassWithRetry(client, base, *className, dynamicThreshold, 5*time.Minute); err != nil {
			row.Status = "partial"
			row.Error = "resetClass failed: " + err.Error()
			out.Results = append(out.Results, row)
			out.FinalStatus = "partial"
			out.FinalError = row.Error
			writeJSON(*outPath, out)
			return
		}

		cursor := startRow
		if cursor >= info.N {
			cursor = 0
		}

		inserted := 0
		started := false

		var t0, tLastRPC float64

		// --------------- INSERT LOOP ---------------
		for inserted < measureVecs {
			if err := overallCtx.Err(); err != nil {
				row.Status = "partial"
				row.Error = "overall timeout: " + err.Error()
				anyPartial = true
				break
			}

			n := bs
			remaining := measureVecs - inserted
			if n > remaining {
				n = remaining
			}
			if n <= 0 {
				break
			}
			// Wrap around if we run past end of file
			if cursor+n >= info.N {
				cursor = 0
			}

			vecs, got, err := readVectors(f, info, cursor, n)
			if err != nil {
				row.Status = "error"
				row.Error = "readVectors: " + err.Error()
				out.Results = append(out.Results, row)
				out.FinalStatus = "error"
				out.FinalError = row.Error
				writeJSON(*outPath, out)
				os.Exit(9)
			}

			objs := make([]*models.Object, 0, got)
			for i := 0; i < got; i++ {
				off := i * vecDim
				v := make([]float32, vecDim)
				copy(v, vecs[off:off+vecDim])
				docID := fmt.Sprintf("bs%d_row%d_i%d", bs, cursor, i)
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

			rpcCtx, rpcCancel := context.WithTimeout(overallCtx, time.Duration(*rpcSec)*time.Second)
			_, err = client.Batch().ObjectsBatcher().WithObjects(objs...).Do(rpcCtx)
			rpcCancel()

			if err != nil {
				row.Status = "partial"
				row.Error = "batch insert: " + err.Error()
				anyPartial = true
				break
			}

			inserted += got
			cursor += got
			tLastRPC = hrsec()

			if inserted%(100000) == 0 || inserted == measureVecs {
				fmt.Printf("[CLIENT]   inserted %d / %d  (send_elapsed=%.1fs)\n",
					inserted, measureVecs, tLastRPC-t0)
			}
		}

		row.MeasureVecs = inserted

		if !started {
			row.Status = "error"
			row.Error = "no inserts performed (started==false)"
			out.Results = append(out.Results, row)
			out.FinalStatus = "error"
			out.FinalError = row.Error
			writeJSON(*outPath, out)
			os.Exit(10)
		}

		// SEND metrics: first RPC start -> last successful RPC end
		row.SendSec = tLastRPC - t0
		if row.SendSec > 0 {
			row.SendVPS = float64(inserted) / row.SendSec
		}

		// Snapshot right after last successful RPC
		if snap, err := getQueue(base, *className); err == nil {
			row.PostSendQueue = snap.TotalQueue
			row.PostSendIndexing = snap.Indexing
			row.PostSendShards = snap.Shards
			fmt.Printf("[CLIENT]   post-send queue=%d indexing=%d shards=%d\n",
				snap.TotalQueue, snap.Indexing, snap.Shards)
		}

		// --------------- DRAIN LOOP ---------------
		qTimeout := time.Duration(*queueTimeoutSec) * time.Second
		poll := time.Duration(*queuePollMS) * time.Millisecond

		fmt.Println("[CLIENT]   waiting for queue drain...")
		qSnap, qPolls, qErr := waitForQueueDrain(base, *className, poll, *queueStablePolls, qTimeout)
		row.QueueDrainPolls = qPolls
		row.FinalQueue = qSnap.TotalQueue
		row.FinalIndexing = qSnap.Indexing
		row.FinalShards = qSnap.Shards

		tEnd := hrsec()

		if qErr != nil {
			row.Status = "partial"
			if row.Error != "" {
				row.Error += " | "
			}
			row.Error += "queue drain: " + qErr.Error()
			anyPartial = true
		}

		// DRAIN time = last-RPC-end -> drain-complete (or timeout)
		row.DrainSec = tEnd - tLastRPC

		// END-TO-END = first-RPC-start -> drain-complete (or timeout)
		row.EndToEndSec = tEnd - t0
		if row.EndToEndSec > 0 {
			row.EndToEndVPS = float64(inserted) / row.EndToEndSec
		}

		fmt.Printf("[CLIENT]   send=%.1fs  drain=%.1fs  e2e=%.1fs  send_vps=%.0f  e2e_vps=%.0f\n",
			row.SendSec, row.DrainSec, row.EndToEndSec, row.SendVPS, row.EndToEndVPS)

		out.Results = append(out.Results, row)
		writeJSON(*outPath, out)
	}

	if anyPartial {
		out.FinalStatus = "partial"
		out.FinalError = "one or more batch sizes did not fully complete"
	} else {
		out.FinalStatus = "ok"
		out.FinalError = ""
	}
	writeJSON(*outPath, out)
	fmt.Println("[CLIENT] Done.")
}