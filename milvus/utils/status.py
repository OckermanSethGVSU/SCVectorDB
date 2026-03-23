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
MILVUS_GRPC_PORT = int(os.getenv("MILVUS_GRPC_PORT", "20001"))
MILVUS_TOKEN = os.getenv("MILVUS_TOKEN", "root:Milvus")

wait_for_milvus(MILVUS_HOST, MILVUS_HEALTH_PORT)
time.sleep(2)

client = MilvusClient(f"http://{MILVUS_HOST}:{MILVUS_GRPC_PORT}", token=MILVUS_TOKEN)
collection_name = "standalone"
client.load_collection("standalone")




print(f"Indexes for {collection_name}: ", client.list_indexes(collection_name), flush=True)

EXPECTED_CORPUS_SIZE = int(os.getenv("EXPECTED_CORPUS_SIZE", "10000000"))

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




client.load_collection("standalone")

while True:
    res = client.get_load_state(collection_name=collection_name)
    if "Loaded" in str(res.get("state", "")):
        break
    time.sleep(5)


res = client.describe_collection(
    collection_name="standalone"
)

print(res,flush=True)
index_status = client.describe_index(collection_name, 'vector')
print(index_status, flush=True)