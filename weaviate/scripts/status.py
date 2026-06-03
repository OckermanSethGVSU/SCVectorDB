#!/usr/bin/env python3

import argparse
import csv
from pathlib import Path

import weaviate


def read_rank0(registry_path: Path):
    with registry_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row or len(row) < 5:
                continue
            rank = int(row[0].strip())
            if rank == 0:
                return {
                    "node": row[1].strip(),
                    "ip": row[2].strip(),
                    "http_port": int(row[3].strip()),
                    "grpc_port": int(row[4].strip()),
                }
    raise ValueError(f"rank 0 not found in {registry_path}")


def read_env_value(env_path: Path, key: str):
    with env_path.open("r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            name, value = line.split("=", 1)
            if name.strip() == key:
                value = value.strip()
                return value if value != "" else None
    return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--registry", default="ip_registry.txt")
    parser.add_argument("--env-file", default="run_config.env")
    parser.add_argument("--collection")
    args = parser.parse_args()

    collection_name = args.collection
    if collection_name is None:
        collection_name = read_env_value(Path(args.env_file), "COLLECTION_NAME")
    if not collection_name:
        raise ValueError("COLLECTION_NAME was not provided and could not be read from the env file")

    node = read_rank0(Path(args.registry))
    client = weaviate.connect_to_custom(
        http_host=node["ip"],
        http_port=node["http_port"],
        http_secure=False,
        grpc_host=node["ip"],
        grpc_port=node["grpc_port"],
        grpc_secure=False,
    )

    try:
        print(f"collection={collection_name}")
        exists = client.collections.exists(collection_name)
        print(f"exists={exists}")
        if not exists:
            return

        collection = client.collections.use(collection_name)
        config = collection.config.get()
        print("config:")
        print(config)

        agg = collection.aggregate.over_all(total_count=True)
        print(f"total_count={agg.total_count}")
    finally:
        client.close()


if __name__ == "__main__":
    main()
