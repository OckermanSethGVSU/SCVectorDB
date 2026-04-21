import os, time
import csv
import numpy as np
from pymilvus import MilvusClient
import multiprocessing as mp

# ---------- Helpers ----------
def file_line_count(path):
    """Check if a file exists, and if so, return its line count."""
    if not os.path.isfile(path):
        print(f"File not found: {path}")
        return 0

    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        count = sum(1 for _ in f)
    return count - 1

def load_ip_from_file(filepath):
    with open(filepath, "r") as f:
        rank, ip, port = f.readline().strip().split(",")
    return ip, int(port)

def read_ip_from_file(path):
    with open(path, 'r', encoding='utf-8') as f:
        return f.read().strip()

def load_all_ips(filepath):
    ips = []
    with open(filepath, "r") as f:
        for line in f:
            rank, ip, port = line.strip().split(",")
            ips.append((int(rank), ip, int(port)))
    return ips

def get_line(path, line_no):
    with open(path) as f:
        for i, line in enumerate(f, start=0):
            if i == line_no:
                return line.strip()

def split_counts(n, k):
    """Split n items into k parts, remainder goes to earlier parts."""
    base, rem = divmod(n, k)
    return [base + (1 if i < rem else 0) for i in range(k)]


# ---------- Config ----------
N_WORKERS = int(os.environ.get("N_WORKERS"))
CLIENTS_PER_WORKER = int(os.environ.get("UPLOAD_CLIENTS_PER_WORKER"))
N_PROCS = N_WORKERS * CLIENTS_PER_WORKER

collection_name = "standalone"


def worker(global_rank, n_workers, clients_per_worker, barrier):
    """
    Flat spawn: total processes = n_workers * clients_per_worker.
    Each process is a "client" mapped to (worker_id, client_id).
    Data partitioning:
      1) Split CORPUS_SIZE points across workers (worker 0 gets first chunk, etc.)
      2) Split each worker's chunk across that worker's clients
    Example:
      10 pts, 2 workers, 2 clients/worker => worker chunks [0..4],[5..9]
      then each chunk split [3,2] => 3,2,3,2 overall
    """
    worker_id = global_rank // clients_per_worker
    client_id = global_rank % clients_per_worker

    # Milvus client
    runtime_state_dir = os.getenv("RUNTIME_STATE_DIR", "./runtime_state")
    MILVUS_HOST = read_ip_from_file(os.path.join(runtime_state_dir, "worker.ip"))
    MILVUS_GRPC_PORT = int(os.getenv("MILVUS_GRPC_PORT", "19530"))
    MILVUS_TOKEN = os.getenv("MILVUS_TOKEN", "root:Milvus")
    client = MilvusClient(f"http://{MILVUS_HOST}:{MILVUS_GRPC_PORT}", token=MILVUS_TOKEN)

    BATCH_SIZE = int(os.getenv("UPLOAD_BATCH_SIZE"))
    CORPUS_SIZE = int(os.getenv("CORPUS_SIZE"))

    client.load_collection(collection_name)
    time.sleep(3)

    # Load embeddings
    DATA_FILEPATH = os.getenv("DATA_FILEPATH")
    data = np.load(DATA_FILEPATH, mmap_mode="r")[:CORPUS_SIZE]
    n_total = int(data.shape[0])

    # ---- partition across workers first ----
    worker_counts = split_counts(n_total, n_workers)
    worker_starts = [0]
    for c in worker_counts[:-1]:
        worker_starts.append(worker_starts[-1] + c)

    
    w_start = worker_starts[worker_id]
    w_n = worker_counts[worker_id]
    w_end = w_start + w_n

    # ---- partition each worker chunk across its clients ----
    client_counts = split_counts(w_n, clients_per_worker)
    client_starts = [w_start]
    for c in client_counts[:-1]:
        client_starts.append(client_starts[-1] + c)

    my_start = client_starts[client_id]
    my_n = client_counts[client_id]
    my_end = my_start + my_n

    print(
        f"[proc {global_rank}] worker={worker_id} client={client_id} "
        f"worker_range=[{w_start},{w_end}) client_range=[{my_start},{my_end}) ",
        flush=True
    )
    
    barrier.wait()
    if global_rank == 0:
        open("data_end.event", "w").close()
        # write header once
        with open("Utimes.csv", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["global_rank", "worker_id", "client_id", "sanity_check", "loop_duration", "wait_period", "total"])
    barrier.wait()

    setup_times = []
    upload_times = []
    op_times = []

    start_loop = time.time()

    # If this client has no work (possible if very small n_total), just skip cleanly
    if my_n > 0:
        for i in range(my_start, my_end, BATCH_SIZE):
            s1 = time.time()
            end = min(i + BATCH_SIZE, my_end)

            # Unique/global IDs: use global index as id
            batch = [{"id": idx, "vector": np.array(data[idx], copy=True)} for idx in range(i, end)]
            s2 = time.time()

            u1 = time.time()
            client.upsert(collection_name, batch)
            u2 = time.time()

            setup_times.append(s2 - s1)
            upload_times.append(u2 - u1)
            op_times.append(u2 - s1)

    barrier.wait()
    end_loop = time.time()

    # Global "last id" strong check once (optional)
    if global_rank == 0 and n_total > 0:
        _ = client.get(collection_name, n_total - 1, consistency_level="Strong")

    barrier.wait()
    searchable = time.time()

    # Per-client sanity check: last id in this client's slice
    if my_n > 0:
        resp = client.get(collection_name, my_end - 1)
        sanity_check = bool(resp)
    else:
        sanity_check = True  # nothing to upload

    barrier.wait()
    time.sleep(1 + 0.2 * global_rank)

    # Append row
    with open("Utimes.csv", "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            global_rank,
            worker_id,
            client_id,
            sanity_check,
            end_loop - start_loop,
            searchable - end_loop,
            searchable - start_loop
        ])


if __name__ == "__main__":
    procs = []
    barrier = mp.Barrier(N_PROCS)

    for r in range(N_PROCS):
        p = mp.Process(target=worker, args=(r, N_WORKERS, CLIENTS_PER_WORKER, barrier))
        p.start()
        procs.append(p)

    for p in procs:
        p.join()
