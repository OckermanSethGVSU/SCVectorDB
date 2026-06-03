from pymilvus import MilvusClient
import os 
import time

def read_ip_from_file(path):
    with open(path, 'r', encoding='utf-8') as f:
        return f.read().strip()
RUNTIME_STATE_DIR = os.getenv("RUNTIME_STATE_DIR", "./runtime_state")
MILVUS_HOST = read_ip_from_file(os.path.join(RUNTIME_STATE_DIR, "worker.ip"))
# Connect to Milvus / Zilliz Cloud
MILVUS_HOST = read_ip_from_file(os.path.join(RUNTIME_STATE_DIR, "worker.ip"))


MILVUS_HEALTH_PORT = int(os.getenv("MILVUS_HEALTH_PORT", "9091"))
MILVUS_GRPC_PORT = int(os.getenv("MILVUS_GRPC_PORT", "19530"))
MILVUS_TOKEN = os.getenv("MILVUS_TOKEN", "root:Milvus")

# wait_for_milvus(MILVUS_HOST, MILVUS_HEALTH_PORT)
time.sleep(2)


client = MilvusClient(f"http://{MILVUS_HOST}:{MILVUS_GRPC_PORT}", token=MILVUS_TOKEN)

collection_name = "standalone"

client.flush(collection_name)

# Get the number of entities (vectors)
stats = client.get_collection_stats(collection_name)
print(stats)

results = client.query(
    collection_name="standalone",
    filter="id >= 0",
    output_fields=["id"],
    limit=10000,  # must specify a limit
)
print(results)
# num_vectors = stats["row_count"]

# print(f"Number of vectors in '{collection_name}': {num_vectors}")
