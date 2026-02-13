package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/kshedden/gonpy"
	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

// Barrier represents a synchronization point for a group of goroutines.
type Barrier struct {
	count  int
	mu     sync.Mutex
	notify chan struct{}
}

func NewBarrier(count int) *Barrier {
	return &Barrier{
		count:  count,
		notify: make(chan struct{}),
	}
}

func (b *Barrier) Wait() {
	b.mu.Lock()
	b.count--
	if b.count == 0 {
		close(b.notify)
	}
	b.mu.Unlock()
	<-b.notify
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

func clientWorker(
	wg *sync.WaitGroup,
	barrier *Barrier,
	workerRank int,
	clientID int,
	clientsPerWorker int,
	totalRows int,
	matrix [][]float32,
) {
	ctx, cancel := context.WithCancel(context.Background())
	defer wg.Done()
	defer cancel()

	// ----- slice assignment: worker slice, then client slice within worker -----
	wStart, wEnd := splitRange(totalRows, workerRank+1, workerRank)
	_ = wStart
	_ = wEnd


	// We'll compute worker slice using splitRange(totalRows, nWorkers, workerRank)
	nWorkersStr := os.Getenv("N_WORKERS")
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
		// Still participate in barriers to avoid deadlock.
		barrier.Wait()
		barrier.Wait()
		barrier.Wait()
		return
	}

	mcols := len(local[0])

	// ----- Milvus client -----
	MILVUS_HOST := os.Getenv("MILVUS_HOST")
	batchSizeStr := os.Getenv("UPLOAD_BATCH_SIZE")
	BATCH_SIZE, err := strconv.Atoi(batchSizeStr)


	url := fmt.Sprintf("http://%s:19530", MILVUS_HOST)
	mclient, err := milvusclient.New(context.Background(), &milvusclient.ClientConfig{
		Address: url,
	})
	if err != nil {
		log.Fatalf("failed to create Milvus client: %v", err)
	}

	collectionName := "standalone" // TODO
	vectorField := "vector"        // TODO
	idField := "id"

	// sanity check ID: last ID in the whole run
	lastID := int64(totalRows - 1)
	option := milvusclient.NewQueryOption(collectionName).
		WithIDs(column.NewColumnInt64("id", []int64{lastID})).
		WithConsistencyLevel(entity.ClStrong)

	globalClientRank := workerRank*clientsPerWorker + clientID

	fmt.Printf(
		"worker=%d client=%d global_client=%d assigned [%d,%d) rows=%d batch=%d\n",
		workerRank, clientID, globalClientRank, startIdx, endIdx, localRows, BATCH_SIZE,
	)

	// Barrier before inserting
	barrier.Wait()

	var (
		totalDurations   []float64
		convertDurations []float64
		uploadDurations  []float64
	)

	startLoop := time.Now()
	for i := 0; i < localRows; i += BATCH_SIZE {
		end := i + BATCH_SIZE
		if end > localRows {
			end = localRows
		}

		startTotal := time.Now()
		batch := local[i:end]

		// IDs: absolute row indices
		ids := make([]int64, len(batch))
		for j := range ids {
			absIdx := startIdx + i + j
			ids[j] = int64(absIdx)
		}

		startUpload := time.Now()
		_, err := mclient.Insert(
			ctx,
			milvusclient.NewColumnBasedInsertOption(collectionName).
				WithInt64Column(idField, ids).
				WithFloatVectorColumn(vectorField, mcols, batch),
		)
		if err != nil {
			log.Fatalf("insert failed worker=%d client=%d absRowStart=%d: %v", workerRank, clientID, startIdx+i, err)
		}
		endUpload := time.Now()

		convertDurations = append(convertDurations, startUpload.Sub(startTotal).Seconds())
		uploadDurations = append(uploadDurations, endUpload.Sub(startUpload).Seconds())
		totalDurations = append(totalDurations, endUpload.Sub(startTotal).Seconds())
	}
	endLoop := time.Now()

	// Wait for everyone to finish inserting
	barrier.Wait()

	// Only ONE goroutine does the "wait until searchable" poke
	if globalClientRank == 0 {
		_, _ = mclient.Get(ctx, option)
	}
	barrier.Wait()

	searchable := time.Now()

	// sanity check for everyone
	result, err := mclient.Get(ctx, option)
	if err != nil {
		log.Printf("Get sanity check failed worker=%d client=%d: %v", workerRank, clientID, err)
	}

	existsStr := "false"
	if result.ResultCount == 1 {
		existsStr = "true"
	}

	loopDuration := endLoop.Sub(startLoop).Seconds()
	waitPeriod := searchable.Sub(endLoop).Seconds()
	total := searchable.Sub(startLoop).Seconds()

	// stagger file writes a bit
	time.Sleep(time.Second * time.Duration(globalClientRank*2))

	file, err := os.OpenFile(RESULT_PATH + "/times.csv", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
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
			"sanity_check", "loop_duration", "wait_period", "total",
		})
	}
	_ = writer.Write([]string{
		strconv.Itoa(workerRank),
		strconv.Itoa(clientID),
		strconv.Itoa(globalClientRank),
		strconv.Itoa(startIdx),
		strconv.Itoa(endIdx),
		existsStr,
		strconv.FormatFloat(loopDuration, 'g', -1, 64),
		strconv.FormatFloat(waitPeriod, 'g', -1, 64),
		strconv.FormatFloat(total, 'g', -1, 64),
	})

	w1, _ := gonpy.NewFileWriter(fmt.Sprintf(RESULT_PATH + "/batch_construction_times_w%d_c%d.npy", workerRank, clientID))
	_ = w1.WriteFloat64(convertDurations)

	w2, _ := gonpy.NewFileWriter(fmt.Sprintf(RESULT_PATH + "/upload_times_w%d_c%d.npy", workerRank, clientID))
	_ = w2.WriteFloat64(uploadDurations)

	w3, _ := gonpy.NewFileWriter(fmt.Sprintf(RESULT_PATH + "/op_times_w%d_c%d.npy", workerRank, clientID))
	_ = w3.WriteFloat64(totalDurations)
}



func main() {
	nWorkersStr := os.Getenv("N_WORKERS")
	nWorkers, err := strconv.Atoi(nWorkersStr)
	if err != nil || nWorkers <= 0 {
		log.Fatalf("invalid N_WORKERS=%q", nWorkersStr)
	}

	clientsStr := os.Getenv("UPLOAD_CLIENTS_PER_WORKER")
	clientsPerWorker, err := strconv.Atoi(clientsStr)
	if err != nil || clientsPerWorker <= 0 {
		log.Fatalf("invalid UPLOAD_CLIENTS_PER_WORKER=%q", clientsStr)
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
	fmt.Printf("CORPUS_SIZE=%d nWorkers=%d clientsPerWorker=%d totalClients=%d DATA_FILEPATH=%s\n",
		CORPUS_SIZE, nWorkers, clientsPerWorker, totalClients, DATA_PATH,
	)


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

	for w := 0; w < nWorkers; w++ {
		for c := 0; c < clientsPerWorker; c++ {
			wg.Add(1)
			go clientWorker(&wg, barrier, w, c, clientsPerWorker, CORPUS_SIZE, matrix)
		}
	}

	wg.Wait()
	fmt.Println("All workers finished")
}
