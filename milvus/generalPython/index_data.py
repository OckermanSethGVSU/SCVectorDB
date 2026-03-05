import os
import json
import time
import requests
from pymilvus import MilvusClient

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

MILVUS_TOKEN = os.getenv("MILVUS_TOKEN", "root:Milvus")


client = MilvusClient(f"http://{MILVUS_HOST}:{MILVUS_GRPC_PORT}", token=MILVUS_TOKEN)
collection_name = "standalone"

client.flush(collection_name)

print(f"Indexes for {collection_name}: ", client.list_indexes(collection_name), flush=True)
print(client.describe_index(collection_name,'vector'), flush=True)

client.release_collection(collection_name=collection_name)
client.drop_index(collection_name, "vector")
# time.sleep(30)

while True:
    idxs = client.list_indexes(collection_name=collection_name)
    # idxs format varies by version; simplest is: "no indexes at all" OR "no index for that field"
    if not idxs:
        break
    # if your list includes field info, filter it:
    # if not any(i.get("field_name") == field for i in idxs): break
    time.sleep(0.5)

print(f"Indexes for {collection_name}: ", client.list_indexes(collection_name), flush=True)
time.sleep(60)

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
    }
    )
else: 
    index_params.add_index(
        field_name="vector",      # your vector_field_name
        index_type="HNSW",        # <- FLAT index
        metric_type=metric,     # or "L2", "IP", etc.
        params={
            "M":16,
            "efConstruction": 100
            }                 # FLAT doesn't need extra params
    )

# client.flush(collection_name)
t1 = time.time()
resp = client.create_index(collection_name, index_params, sync=True)
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

print(f"Indexes for {collection_name}: ", client.list_indexes(collection_name), flush=True)
print(client.describe_index(collection_name,'vector'), flush=True)