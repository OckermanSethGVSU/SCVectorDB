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


MILVUS_HOST = os.getenv("MILVUS_HOST", "localhost")
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
client.load_collection("standalone")




print(f"Indexes for {collection_name}: ", client.list_indexes(collection_name), flush=True)
print(client.describe_index(collection_name,'vector'), flush=True)

res = client.describe_collection(
    collection_name="standalone"
)

print(res,flush=True)