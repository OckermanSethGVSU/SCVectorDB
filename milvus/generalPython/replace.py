#!/usr/bin/env python3

import argparse
from pathlib import Path
import time
import os

def get_ip_by_rank(filename: str, target_rank: int, timeout_s: float = 60.0,) -> str:
    """
    Reads a file formatted as:
        rank,ip,port
    and returns the IP associated with target_rank.
    """
    deadline = time.time() + timeout_s
    target_rank_str = str(target_rank)

    # Wait for the file to appear
    while not os.path.exists(filename):
        if time.time() >= deadline:
            raise TimeoutError(f"Timed out after {timeout_s}s waiting for file: {filename}")
        time.sleep(1)

    target_rank = str(target_rank)
    deadline = time.time() + timeout_s
    while True:
        with open(filename) as f:
            for line in f:
                parts = line.strip().split(",")
                
                if len(parts) < 2:
                    continue
                rank = parts[0]
                ip = parts[1]
                if rank == target_rank:
                    return ip
        if time.time() >= deadline:
            break
        
        time.sleep(1)

    raise ValueError(f"Rank {target_rank} not found in {filename}")

def get_etcd_mode() -> str:
    # Default to replicated if not set
    mode = os.environ.get("ETCD_MODE", "replicated").strip().lower()
    return mode


parser = argparse.ArgumentParser()
parser.add_argument("--mode", required=True, help="Required mode argument")
parser.add_argument("--rank", required=False, help="rank of component", default=-1,)
parser.add_argument("--wal", required=False, help="Which MQ to use for WAL", default="default",)
args = parser.parse_args()

mode = args.mode
rank = args.rank
wal = args.wal

if mode == "standalone":
    milvus_path = Path("configs/milvus.yaml")
    worker_ip_path = Path("worker.ip")

    # Read replacement value (strip to avoid accidental newlines)
    replacement = worker_ip_path.read_text().strip()

    # Read milvus config
    text = milvus_path.read_text()

    # Replace all occurrences
    text = text.replace("<HNS0>", replacement)

    # Write back in place
    milvus_path.write_text(text)
elif mode == "distributed":
    dist_milvus_path = Path("configs/distributed_milvus.yaml")
    text = dist_milvus_path.read_text()

    minio_ip = get_ip_by_rank("minio_registry.txt",0)
    text = text.replace("<MINIO>",minio_ip)

    ETCD_MODE = get_etcd_mode()
    text = text.replace("<WAL>",wal)

    if ETCD_MODE == "single":
        etcd0 = get_ip_by_rank("etcd_registry.txt",0)
        text = text.replace("<ETCD0>",etcd0)
        text = text.replace(",<ETCD1>:2379","")
        text = text.replace(",<ETCD2>:2379","")

    else:
        etcd0 = get_ip_by_rank("etcd_registry.txt",0)
        etcd1 = get_ip_by_rank("etcd_registry.txt",1)
        etcd2 = get_ip_by_rank("etcd_registry.txt",2)
        text = text.replace("<ETCD0>:2379",f"{etcd0}:2379")
        text = text.replace("<ETCD1>:2379",f"{etcd1}:2479")
        text = text.replace("<ETCD1>:2379",f"{etcd2}:2579")
    
    dist_milvus_path.write_text(text)

elif mode in ["COORDINATOR", "STREAMING","QUERY","PROXY", "DATA"]:
    dist_milvus_path = Path("configs/distributed_milvus.yaml")
    text = dist_milvus_path.read_text()
    ip = get_ip_by_rank(f"{mode}_registry.txt",rank)
    text = text.replace(f"<{mode}>",ip)

    for m in ["COORDINATOR", "STREAMING","QUERY","PROXY", "DATA"]:
        if m == mode:
            continue
        text = text.replace(f"<{m}>","")
        
    milvus_path = Path(f"configs/{mode}{rank}.yaml")
    milvus_path.write_text(text)

else:
    print("Unkown component type passed in", flush=True)

