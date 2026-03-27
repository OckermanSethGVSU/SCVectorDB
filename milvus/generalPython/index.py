import os
import json
import time
import requests
from pymilvus import MilvusClient

"""
IMPORTANT NOTE: if the collection was initialized with a FLAT index and this script
drops that index to rebuild HNSW or GPU_CAGRA on the same vector field, Milvus 2.6.6
hits a bug that massively degrades query performance. As of March 27th, 2026, that
bug is not resolved. This workaround is still required for our indexing experiments.
"""



def create_index_with_fallback_poll(
    client,
    collection_name,
    index_params,
    timeout_s=18000,
    poll_interval_s=0.5,
):
    """
    Try synchronous index creation first.
    If sync=True fails, fall back to polling index state until finished.

    Returns the final index description/info when finished.
    Raises on timeout or terminal failure.
    """

    try:
        resp = client.create_index(collection_name, index_params, sync=True)
        return 
    except Exception as e:
        sync_error = e
        print(f"sync create_index failed, falling back to manual polling", flush=True)

    # Fallback: poll until the index state says finished
    last_print = 0
    start = time.time()
    while True:
        elapsed = time.time() - start
        if elapsed > timeout_s:
            raise TimeoutError(
                f"Timed out after {timeout_s}s waiting for index build. "
                f"Original sync error: {sync_error}"
            )

        try:
            info = client.describe_index(collection_name, "vector")

            state = info["state"]
            indexed_rows = info["indexed_rows"]
            total_rows = info["total_rows"]
            pending_index_rows = info["pending_index_rows"]

            now = time.time()
            if now - last_print >= 600:  # 10 minutes
                print(
                    f"index state={state} indexed_rows={indexed_rows}/{total_rows} "
                    f"pending_index_rows={pending_index_rows}",
                    flush=True,
                )
                last_print = now

            state_str = state.strip().lower()

            if state_str == "finished":
                return

            if state_str == "failed":
                raise RuntimeError(f"Index build entered failure state: {info}")

        except Exception as e:
            # Transient polling failures can happen; keep going until timeout
            print(f"transient polling error: {e}", flush=True)
            time.sleep(3)

        time.sleep(poll_interval_s)



def wait_for_milvus(host, port):
    print(f"Waiting for Milvus at {host}:{port}...")
    for _ in range(60):
        try:
            r = requests.get(f"http://{host}:{port}/healthz", timeout=2)
            if r.status_code == 200:
                print("Milvus is good.")
                return
        except Exception:
            pass
        time.sleep(1)
    raise RuntimeError("Milvus did not respond in time")

def read_ip_from_file(path):
    with open(path, 'r', encoding='utf-8') as f:
        return f.read().strip()


# MILVUS_HOST = os.getenv("MILVUS_HOST", "localhost")
MILVUS_HOST = read_ip_from_file("worker.ip")
MILVUS_GRPC_PORT = int(os.getenv("MILVUS_GRPC_PORT", "20001"))
CORPUS_SIZE = int(os.getenv("CORPUS_SIZE", "10000000"))

MILVUS_TOKEN = os.getenv("MILVUS_TOKEN", "root:Milvus")


client = MilvusClient(f"http://{MILVUS_HOST}:{MILVUS_GRPC_PORT}", token=MILVUS_TOKEN)
collection_name = "standalone"

INIT_FLAT_INDEX = os.getenv("INIT_FLAT_INDEX", "TRUE").strip().lower() == "true"

if INIT_FLAT_INDEX:
    print(
        "Collection was initialized with a FLAT index; dropping it before building the target index. "
        "WARNING: this sequence triggers a Milvus bug (as of 03/27/26 and v.2.6.6) that massively degrades query performance.",
        flush=True,
    )
    print(f"Indexes for {collection_name}: ", client.list_indexes(collection_name), flush=True)
    print(client.describe_index(collection_name, "vector"), flush=True)

    client.release_collection(collection_name=collection_name)
    client.drop_index(collection_name, "vector")

    while True:
        idxs = client.list_indexes(collection_name=collection_name)
        if not idxs:
            break
        time.sleep(0.5)

    print(f"Indexes for {collection_name}: ", client.list_indexes(collection_name), flush=True)
    time.sleep(60)
else:
    print(
        "Collection was initialized without a FLAT index; building the target index directly.",
        flush=True,
    )



index_params = client.prepare_index_params()
distance_metric = os.environ["DISTANCE_METRIC"].strip().lower()
match distance_metric:
    case "dot" | "ip" | "innerproduct":
        metric = "IP"
    case "cosine":
        metric = "COSINE"
    case "euclidan" | "l2":
        metric = "L2"
    case _:
        raise ValueError(f"Unknown distance metric: {distance_metric}")

GPU_INDEX = os.environ["GPU_INDEX"].strip().lower() == "true"
if GPU_INDEX:
    index_params.add_index(
    field_name="vector",      
    index_type="GPU_CAGRA",        
    metric_type=metric,
    params={
        "intermediate_graph_degree": 64,
        "graph_degree": 16,
    },
    mmap_enabled=False,
    )
else: 
    index_params.add_index(
        field_name="vector",      # your vector_field_name
        index_type="HNSW",        # <- FLAT index
        metric_type=metric,     # or "L2", "IP", etc.
        params={
            "M":16,
            "efConstruction": 100
            },                # FLAT doesn't need extra params
        mmap_enabled=False,
    )

client.flush(collection_name)
print("Data flushed", flush=True)

t1 = time.time()
resp = create_index_with_fallback_poll(client, collection_name, index_params)
t2 = time.time()



client.load_collection("standalone")

while True:
    state = client.get_load_state("standalone")
    if str(state['state']) == "Loaded":
        break
    time.sleep(0.5)
t3 = time.time()

with open(f"index_time.txt", "w") as f:
        f.write(str(t2 - t1))
with open(f"collection_time.txt", "w") as f:
        f.write(str(t3- t2))



EXPECTED_CORPUS_SIZE = int(os.getenv("INSERT_CORPUS_SIZE", "10000000"))

start = time.time()
last_print = 0
while True:
    try:
        index_status = client.describe_index(collection_name, 'vector')
        index_current_count = int(index_status['total_rows'])

        stats = client.get_collection_stats(collection_name)
        collection_current_count = int(stats["row_count"])
    except Exception as e:
        if time.time() - start > 60 * 10:
            raise TimeoutError(f"Timed out while polling Milvus state: {e}", flush=True)
        print(f"[warn] polling failed: {e}", flush=True)
        time.sleep(10)
        continue


    now = time.time()
    if now - last_print > 60:
        print(f"[wait] rows={collection_current_count}/{EXPECTED_CORPUS_SIZE}", flush=True)
        last_print = now
    
    if collection_current_count == EXPECTED_CORPUS_SIZE and index_current_count == collection_current_count:
        break
    
    elif time.time() - start > (60 * 10):
        raise TimeoutError("Expected row count not detected in 10 minutes ")
    else:
        time.sleep(10)

while True:
    res = client.get_load_state(collection_name=collection_name)
    if "Loaded" in str(res.get("state", "")):
        break
    time.sleep(5)


res = client.describe_collection(collection_name="standalone")
index_status = client.describe_index(collection_name, 'vector')

print(res,flush=True)
print(index_status, flush=True)
