import os
import json
import time
import requests
from pymilvus import MilvusClient
from pymilvus import FieldSchema, CollectionSchema, DataType



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


# MILVUS_HOST = os.getenv("MILVUS_HOST", "localhost")
def read_ip_from_file(path):
    with open(path, 'r', encoding='utf-8') as f:
        return f.read().strip()


# MILVUS_HOST = os.getenv("MILVUS_HOST", "localhost")
MILVUS_HOST = read_ip_from_file("worker.ip")


MILVUS_HEALTH_PORT = int(os.getenv("MILVUS_HEALTH_PORT", "9091"))
MILVUS_GRPC_PORT = int(os.getenv("MILVUS_GRPC_PORT", "19530"))
MILVUS_TOKEN = os.getenv("MILVUS_TOKEN", "root:Milvus")

wait_for_milvus(MILVUS_HOST, MILVUS_HEALTH_PORT)
time.sleep(2)


client = MilvusClient(f"http://{MILVUS_HOST}:{MILVUS_GRPC_PORT}", token=MILVUS_TOKEN)
collection_name = "standalone"

if client.has_collection(collection_name):
    client.drop_collection(collection_name)
    print(f"Dropped existing collection '{collection_name}' to avoid schema conflict.")
else:
    print(f"No existing collection named '{collection_name}' found.")


# Define fields
id_field = FieldSchema(
    name="id",
    dtype=DataType.INT64,
    is_primary=True,
    auto_id=False,
)

vector_field = FieldSchema(
    name="vector",
    dtype=DataType.FLOAT_VECTOR,
    dim=2560
)

# index_params = {
#     "metric_type": "IP",
#     "index_type": "FLAT",
#     "params": {}   # FLAT doesn't need params
# }

index_params = {
    "metric_type": "COSINE",
    "index_type": "FLAT",
    "params": {}   # FLAT doesn't need params
}
schema = CollectionSchema(
    fields=[id_field, vector_field],
    description="collection with flat index",
    
)


index_params = client.prepare_index_params()

# Step 2: add a FLAT index definition for your vector field
# index_params.add_index(
#     field_name="vector",      # your vector_field_name
#     index_type="FLAT",        # <- FLAT index
#     metric_type="IP",     # or "L2", "IP", etc.
#     params={}                 # FLAT doesn't need extra params
# )
index_params.add_index(
    field_name="vector",      # your vector_field_name
    index_type="FLAT",        # <- FLAT index
    metric_type="COSINE",     # or "L2", "IP", etc.
    params={}                 # FLAT doesn't need extra params
)
# index_params.add_index(
#     field_name="vector",      # your vector_field_name
#     index_type="GPU_IVF_PQ",        # <- FLAT index
#     metric_type="IP",     # or "L2", "IP", etc.
#     params={}                 # FLAT doesn't need extra params
# )

# index_params.add_index(
#     field_name="vector",
#     index_type="GPU_CAGRA",
#     metric_type="IP",
#     params={
#         "intermediate_graph_degree": 64,   # keep >= graph_degree
#         "graph_degree": 32,
#         # strongly recommended to be explicit:
#         "build_algo": "IVF_PQ",            # or whatever is supported in your version
#         "cache_dataset_on_device": "false",# start with false to avoid GPU memory stalls
#         "adapt_for_cpu": "false",
#     }
# )

client.create_collection(
    collection_name=collection_name,
    schema=schema,
    index_params=index_params
)


print(f"Indexes for {collection_name}: ", client.list_indexes(collection_name), flush=True)

print(client.describe_index(collection_name,'vector'), flush=True)