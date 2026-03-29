#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List
from urllib import error, request

import numpy as np


def rest_json(method: str, url: str, payload: dict | None = None) -> dict:
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = request.Request(url, data=data, headers=headers, method=method)
    with request.urlopen(req) as resp:
        return json.loads(resp.read().decode("utf-8"))


def wait_for_count(base_url: str, collection: str, expected: int, timeout_s: float) -> None:
    import time

    deadline = time.time() + timeout_s
    url = f"{base_url}/collections/{collection}/points/count"
    payload = {"exact": True}

    while time.time() < deadline:
        body = rest_json("POST", url, payload)
        count = body["result"]["count"]
        if count == expected:
            return
        time.sleep(0.2)

    raise RuntimeError(f"Timed out waiting for count {expected} in collection {collection}")


def fetch_all_points(base_url: str, collection: str) -> List[dict]:
    url = f"{base_url}/collections/{collection}/points/scroll"
    payload = {
        "limit": 1024,
        "with_payload": True,
        "with_vector": True,
    }
    points: List[dict] = []
    offset = None

    while True:
        if offset is not None:
            payload["offset"] = offset
        body = rest_json("POST", url, payload)
        batch = body["result"]["points"]
        points.extend(batch)
        offset = body["result"].get("next_page_offset")
        if offset is None:
            break

    return points


def normalize_id(point_id) -> int:
    if isinstance(point_id, int):
        return point_id
    if isinstance(point_id, str):
        return int(point_id)
    if isinstance(point_id, dict) and "num" in point_id:
        return int(point_id["num"])
    raise TypeError(f"Unsupported point id shape: {point_id!r}")


def compare_vectors(expected: np.ndarray, actual_points: List[dict]) -> None:
    actual_by_id: Dict[int, dict] = {normalize_id(point["id"]): point for point in actual_points}

    expected_ids = set(range(expected.shape[0]))
    actual_ids = set(actual_by_id)
    if expected_ids != actual_ids:
        missing = sorted(expected_ids - actual_ids)
        extra = sorted(actual_ids - expected_ids)
        raise AssertionError(f"ID mismatch. Missing={missing}, extra={extra}")

    for idx in range(expected.shape[0]):
        actual_vec = np.asarray(actual_by_id[idx]["vector"], dtype=np.float32)
        if actual_vec.shape != expected[idx].shape:
            raise AssertionError(
                f"Vector shape mismatch for id {idx}: expected {expected[idx].shape}, got {actual_vec.shape}"
            )
        if not np.allclose(actual_vec, expected[idx], atol=1e-6, rtol=0.0):
            raise AssertionError(f"Vector mismatch for id {idx}: expected {expected[idx]}, got {actual_vec}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify a Qdrant collection matches a local .npy file.")
    parser.add_argument("--npy", required=True, help="Expected matrix .npy path")
    parser.add_argument("--base-url", default="http://127.0.0.1:6333", help="Qdrant REST base URL")
    parser.add_argument("--collection", default="singleShard", help="Collection name")
    parser.add_argument("--timeout-seconds", type=float, default=20.0, help="Count wait timeout")
    args = parser.parse_args()

    expected = np.load(Path(args.npy))
    wait_for_count(args.base_url, args.collection, int(expected.shape[0]), args.timeout_seconds)
    points = fetch_all_points(args.base_url, args.collection)
    compare_vectors(expected, points)
    print(f"Verified {expected.shape[0]} points in {args.collection}")


if __name__ == "__main__":
    main()
