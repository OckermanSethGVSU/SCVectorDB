#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import pathlib
import sys
import urllib.error
import urllib.request

import numpy as np


def http_request(method: str, url: str, payload: dict | None = None) -> bytes:
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read()


def request_json(method: str, url: str, payload: dict | None = None) -> dict:
    body = http_request(method, url, payload)
    if not body:
        return {}
    return json.loads(body.decode("utf-8"))


def recreate_collection(base_url: str, collection_name: str, vector_dim: int, distance: str) -> None:
    delete_url = f"{base_url}/collections/{collection_name}"
    create_url = delete_url
    try:
        request_json("DELETE", delete_url)
    except urllib.error.HTTPError as exc:
        if exc.code != 404:
            raise

    payload = {
        "vectors": {
            "size": vector_dim,
            "distance": distance,
        },
        "shard_number": 1,
        "replication_factor": 1,
        "optimizers_config": {
            "indexing_threshold": 0,
        },
    }
    request_json("PUT", create_url, payload)


def generate_arrays(insert_count: int, query_count: int, vector_dim: int, seed: int) -> tuple[np.ndarray, np.ndarray]:
    rng = np.random.default_rng(seed)
    insert_vectors = rng.random((insert_count, vector_dim), dtype=np.float32)

    take = min(query_count, insert_count)
    base_queries = insert_vectors[:take].copy()
    noise = rng.normal(loc=0.0, scale=0.0005, size=base_queries.shape).astype(np.float32)
    base_queries += noise

    if query_count > take:
        extra = rng.random((query_count - take, vector_dim), dtype=np.float32)
        query_vectors = np.vstack([base_queries, extra]).astype(np.float32, copy=False)
    else:
        query_vectors = base_queries

    return insert_vectors, query_vectors


def main() -> int:
    root_dir = pathlib.Path(__file__).resolve().parents[1]

    parser = argparse.ArgumentParser(description="Create a local Qdrant collection and sample .npy files.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6333)
    parser.add_argument("--collection", default="singleShard")
    parser.add_argument("--distance", default="Cosine")
    parser.add_argument("--vector-dim", type=int, default=128)
    parser.add_argument("--insert-count", type=int, default=1000)
    parser.add_argument("--query-count", type=int, default=100)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output-dir", default=str(root_dir / "local_test_data"))
    parser.add_argument("--insert-file", default="insert_vectors.npy")
    parser.add_argument("--query-file", default="query_vectors.npy")
    args = parser.parse_args()

    output_dir = pathlib.Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    base_url = f"http://{args.host}:{args.port}"
    try:
        health_body = http_request("GET", f"{base_url}/healthz").decode("utf-8", errors="replace").strip()
    except Exception as exc:
        print(f"Failed to reach Qdrant at {base_url}: {exc}", file=sys.stderr)
        return 1

    insert_vectors, query_vectors = generate_arrays(
        insert_count=args.insert_count,
        query_count=args.query_count,
        vector_dim=args.vector_dim,
        seed=args.seed,
    )

    try:
        recreate_collection(base_url, args.collection, args.vector_dim, args.distance)
    except Exception as exc:
        print(f"Failed to recreate collection {args.collection!r}: {exc}", file=sys.stderr)
        return 1

    insert_path = output_dir / args.insert_file
    query_path = output_dir / args.query_file
    np.save(insert_path, insert_vectors)
    np.save(query_path, query_vectors)

    summary = {
        "qdrant_url": base_url,
        "health": health_body,
        "collection": args.collection,
        "distance": args.distance,
        "vector_dim": args.vector_dim,
        "insert_count": int(insert_vectors.shape[0]),
        "query_count": int(query_vectors.shape[0]),
        "insert_filepath": str(insert_path),
        "query_filepath": str(query_path),
    }
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
