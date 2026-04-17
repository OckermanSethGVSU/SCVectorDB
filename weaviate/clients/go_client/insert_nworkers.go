package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
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
		MaxIdleConns:        400,
		MaxIdleConnsPerHost: 400,
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

func waitForWeaviate(base string, maxSeconds int) error {
	meta := base + "/v1/meta"
	start := time.Now()
	var lastBody []byte
	var lastCode int
	var lastErr error
	for {
		body, code, err := httpGet(meta)
		lastBody, lastCode, lastErr = body, code, err
		if err == nil && code == 200 && len(body) > 0 {
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
			return nil
		}
		time.Sleep(300 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for class %s to appear in schema", className)
}

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

func makeFlatClass(className string, shardCount int) *models.Class {
	return &models.Class{
		Class:           className,
		Vectorizer:      "none",
		VectorIndexType: "flat",
		VectorIndexConfig: map[string]any{
			"distance": "cosine",
		},
		ShardingConfig: map[string]any{
			"desiredCount":       shardCount,
			"virtualPerPhysical": 128,
			"strategy":           "hash",
			"key":                "_id",
			"function":           "murmur3",
		},
		Properties: []*models.Property{
			{Name: idPropName, DataType: []string{"text"}},
		},
	}
}

func resetClassFlat(ctx context.Context, client *weaviate.Client, base, className string, shardCount int) error {
	_ = client.Schema().ClassDeleter().WithClassName(className).Do(ctx)
	cls := makeFlatClass(className, shardCount)
	if err := client.Schema().ClassCreator().WithClass(cls).Do(ctx); err != nil {
		time.Sleep(1 * time.Second)
		_ = client.Schema().ClassDeleter().WithClassName(className).Do(ctx)
		time.Sleep(500 * time.Millisecond)
		if err2 := client.Schema().ClassCreator().WithClass(cls).Do(ctx); err2 != nil {
			return err2
		}
	}
	return waitForClass(base, className, 60*time.Second)
}

type ResultRow struct {
	BatchSize   int `json:"batch_size"`
	MeasureVecs int `json:"measure_vecs"`

	SendSec     float64 `json:"send_sec"`
	DrainSec    float64 `json:"drain_sec"`
	EndToEndSec float64 `json:"end_to_end_sec"`

	SendVPS     float64 `json:"send_vps"`
	EndToEndVPS float64 `json:"end_to_end_vps"`

	QueueDrainPolls int `json:"queue_drain_polls"`
	FinalQueue      int `json:"final_queue"`
	FinalIndexing   int `json:"final_indexing"`
	FinalShards     int `json:"final_shards"`

	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

type Output struct {
	TimestampUTC string `json:"timestamp_utc"`
	Worker       string `json:"worker"`
	Class        string `json:"class"`
	IndexMode    string `json:"index_mode"`
	Storage      string `json:"storage"`
	VecDim       int    `json:"vec_dim"`
	DataFile     string `json:"data_file"`

	BatchSize    int `json:"batch_size"`
	MeasureTotal int `json:"measure_total"`
	MeasureVecs  int `json:"measure_vecs"`
	StartRow     int `json:"start_row"`

	Rank             int `json:"client_rank"`
	NClients         int `json:"n_clients"`
	TotalWorkers     int `json:"total_workers"`
	ClientsPerWorker int `json:"clients_per_worker"`

	RpcSec          int `json:"rpc_sec"`
	OverallSec      int `json:"overall_sec"`
	QueueTimeoutSec int `json:"queue_timeout_sec"`

	Results     []ResultRow `json:"results"`
	FinalStatus string      `json:"final_status"`
	FinalError  string      `json:"final_error,omitempty"`
}

func writeJSON(path string, out Output) {
	b, _ := json.MarshalIndent(out, "", "  ")
	_ = os.WriteFile(path, b, 0644)
	fmt.Println("[CLIENT] wrote:", path)
}

func main() {
	workerIP := mustEnvString("WORKER_IP")
	restPort := mustEnvString("REST_PORT")
	grpcPort := mustEnvString("GRPC_PORT")
	dataFile := mustEnvString("DATA_FILE")
	vecDim := mustEnvInt("VEC_DIM")
	className := mustEnvString("CLASS_NAME")

	measureTotal := mustEnvInt("MEASURE_TOTAL")
	startRow := mustEnvInt("START_ROW")

	nClients := mustEnvInt("CLIENTS_N")
	rank := mustEnvInt("CLIENT_RANK")
	outPath := mustEnvString("OUT_JSON")

	totalWorkers := mustEnvInt("TOTAL_WORKERS")
	clientsPerWorker := mustEnvInt("CLIENTS_PER_WORKER")

	bsPow := envIntDefault("BS_POW", 11)
	batchSize := 1 << bsPow

	overallSec := envIntDefault("CLIENT_OVERALL_SEC", 20000)
	rpcSec := envIntDefault("CLIENT_RPC_TIMEOUT_SEC", 1800)
	queueTimeoutSec := envIntDefault("QUEUE_TIMEOUT_SEC", 20000)

	base := "http://" + workerIP + ":" + restPort

	out := Output{
		TimestampUTC:     time.Now().UTC().Format(time.RFC3339),
		Worker:           base,
		Class:            className,
		IndexMode:        "flat",
		Storage:          "memory_tmpfs",
		VecDim:           vecDim,
		DataFile:         dataFile,
		BatchSize:        batchSize,
		MeasureTotal:     measureTotal,
		MeasureVecs:      0,
		StartRow:         startRow,
		Rank:             rank,
		NClients:         nClients,
		TotalWorkers:     totalWorkers,
		ClientsPerWorker: clientsPerWorker,
		RpcSec:           rpcSec,
		OverallSec:       overallSec,
		QueueTimeoutSec:  queueTimeoutSec,
		Results:          []ResultRow{},
		FinalStatus:      "ok",
	}

	fmt.Printf("[CLIENT] rank=%d/%d target=%s flat bs=%d total=%d start=%d workers=%d cpw=%d\n",
		rank, nClients, base, batchSize, measureTotal, startRow, totalWorkers, clientsPerWorker)

	if err := waitForWeaviate(base, 240); err != nil {
		out.FinalStatus = "error"
		out.FinalError = err.Error()
		writeJSON(outPath, out)
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
		writeJSON(outPath, out)
		os.Exit(5)
	}

	if rank == 0 {
		fmt.Println("[CLIENT] rank0 resetting schema (flat) with desired shards:", totalWorkers)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		err := resetClassFlat(ctx, client, base, className, totalWorkers)
		cancel()
		if err != nil {
			out.FinalStatus = "error"
			out.FinalError = "resetClassFlat: " + err.Error()
			writeJSON(outPath, out)
			os.Exit(6)
		}
	} else {
		if err := waitForClass(base, className, 10*time.Minute); err != nil {
			out.FinalStatus = "error"
			out.FinalError = "waitForClass: " + err.Error()
			writeJSON(outPath, out)
			os.Exit(7)
		}
	}

	f, err := os.Open(dataFile)
	if err != nil {
		out.FinalStatus = "error"
		out.FinalError = "open: " + err.Error()
		writeJSON(outPath, out)
		os.Exit(8)
	}
	defer f.Close()

	info, err := parseNpyHeader(f, vecDim)
	if err != nil {
		out.FinalStatus = "error"
		out.FinalError = "npy: " + err.Error()
		writeJSON(outPath, out)
		os.Exit(9)
	}

	baseCount := measureTotal / nClients
	rem := measureTotal % nClients
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
	if myStart >= info.N {
		myStart = myStart % info.N
	}

	overallCtx, cancel := context.WithTimeout(context.Background(), time.Duration(overallSec)*time.Second)
	defer cancel()

	row := ResultRow{BatchSize: batchSize, Status: "ok"}
	cursor := myStart
	inserted := 0
	started := false
	var t0, tLastRPC float64

	for inserted < myCount {
		if err := overallCtx.Err(); err != nil {
			row.Status = "partial"
			row.Error = "overall timeout: " + err.Error()
			break
		}
		n := batchSize
		remaining := myCount - inserted
		if n > remaining {
			n = remaining
		}
		if cursor+n >= info.N {
			cursor = 0
		}

		vecs, got, err := readVectors(f, info, cursor, n)
		if err != nil {
			row.Status = "error"
			row.Error = "readVectors: " + err.Error()
			break
		}

		objs := make([]*models.Object, 0, got)
		for i := 0; i < got; i++ {
			off := i * vecDim
			v := make([]float32, vecDim)
			copy(v, vecs[off:off+vecDim])
			docID := fmt.Sprintf("nw_w%d_cpw%d_nc%d_r%d_row%d_i%d",
				totalWorkers, clientsPerWorker, nClients, rank, cursor, i)
			objs = append(objs, &models.Object{
				Class: className,
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

		rpcCtx, rpcCancel := context.WithTimeout(overallCtx, time.Duration(rpcSec)*time.Second)
		_, err = client.Batch().ObjectsBatcher().WithObjects(objs...).Do(rpcCtx)
		rpcCancel()
		if err != nil {
			row.Status = "partial"
			row.Error = "batch insert: " + err.Error()
			break
		}

		inserted += got
		cursor += got
		tLastRPC = hrsec()
	}

	row.MeasureVecs = inserted
	if !started {
		row.Status = "error"
		row.Error = "no inserts performed"
		out.Results = append(out.Results, row)
		out.FinalStatus = "error"
		out.FinalError = row.Error
		writeJSON(outPath, out)
		os.Exit(10)
	}

	row.SendSec = tLastRPC - t0
	if row.SendSec > 0 {
		row.SendVPS = float64(inserted) / row.SendSec
	}

	if rank == 0 {
		qTimeout := time.Duration(queueTimeoutSec) * time.Second
		poll := 500 * time.Millisecond
		qSnap, qPolls, qErr := waitForQueueDrain(base, className, poll, 3, qTimeout)
		row.QueueDrainPolls = qPolls
		row.FinalQueue = qSnap.TotalQueue
		row.FinalIndexing = qSnap.Indexing
		row.FinalShards = qSnap.Shards
		tEnd := hrsec()

		if qErr != nil {
			row.Status = "partial"
			row.Error = "queue drain: " + qErr.Error()
		}
		row.DrainSec = tEnd - tLastRPC
		row.EndToEndSec = tEnd - t0
		if row.EndToEndSec > 0 {
			row.EndToEndVPS = float64(inserted) / row.EndToEndSec
		}
	} else {
		tEnd := hrsec()
		row.EndToEndSec = tEnd - t0
		if row.EndToEndSec > 0 {
			row.EndToEndVPS = float64(inserted) / row.EndToEndSec
		}
	}

	out.MeasureVecs = inserted
	out.Results = append(out.Results, row)
	if row.Status != "ok" {
		out.FinalStatus = row.Status
		out.FinalError = row.Error
	}
	writeJSON(outPath, out)

	if out.FinalStatus == "error" {
		os.Exit(11)
	}
	if out.FinalStatus == "partial" {
		os.Exit(12)
	}
}