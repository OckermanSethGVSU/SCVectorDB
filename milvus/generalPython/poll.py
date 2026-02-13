import os
import json
import time
import requests
from pymilvus import MilvusClient


def read_ip_from_file(path):
    with open(path, 'r', encoding='utf-8') as f:
        return f.read().strip()

def wait_for_milvus(host, port):
    print(f"Waiting for Milvus at {host}:{port}...")
    for _ in range(120):
        try:
            r = requests.get(f"http://{host}:{port}/healthz", timeout=2)
            if r.status_code == 200:
                print("Milvus Online", flush=True)
                return
        except Exception:
            pass
        time.sleep(1)
    raise RuntimeError("Milvus did not respond in time", flush=True)


while not os.path.exists("worker.ip"):
        time.sleep(1)


MILVUS_HOST = read_ip_from_file("worker.ip")
MILVUS_HEALTH_PORT = int(os.getenv("MILVUS_HEALTH_PORT", "9091"))

wait_for_milvus(MILVUS_HOST, MILVUS_HEALTH_PORT)

