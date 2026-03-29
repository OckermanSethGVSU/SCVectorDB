#!/usr/bin/env python3

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np


def compute_scores(queries: np.ndarray, base: np.ndarray, metric: str) -> np.ndarray:
    metric = metric.lower()
    if metric == "dot":
        return queries @ base.T
    if metric == "cosine":
        query_norms = np.linalg.norm(queries, axis=1, keepdims=True)
        base_norms = np.linalg.norm(base, axis=1, keepdims=True).T
        denom = np.clip(query_norms * base_norms, 1e-12, None)
        return (queries @ base.T) / denom
    raise ValueError(f"Unsupported metric for verifier: {metric}")


def expected_top_k_ids(base: np.ndarray, queries: np.ndarray, metric: str, top_k: int) -> np.ndarray:
    scores = compute_scores(queries, base, metric)
    order = np.argsort(-scores, axis=1, kind="stable")
    return order[:, :top_k].astype(np.int64)


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify query_result_ids.npy against brute-force expected ids.")
    parser.add_argument("--base-npy", required=True, help="Inserted base matrix")
    parser.add_argument("--query-npy", required=True, help="Query matrix")
    parser.add_argument("--result-npy", required=True, help="Path to query_result_ids.npy")
    parser.add_argument("--metric", default="Dot", help="Distance metric used by the collection")
    parser.add_argument("--top-k", type=int, required=True, help="Top-k width")
    args = parser.parse_args()

    base = np.load(Path(args.base_npy)).astype(np.float32, copy=False)
    queries = np.load(Path(args.query_npy)).astype(np.float32, copy=False)
    actual = np.load(Path(args.result_npy)).astype(np.int64, copy=False)
    expected = expected_top_k_ids(base, queries, args.metric, args.top_k)

    if actual.shape != expected.shape:
        raise AssertionError(f"Result shape mismatch: expected {expected.shape}, got {actual.shape}")

    if not np.array_equal(actual, expected):
        for row_idx in range(expected.shape[0]):
            if not np.array_equal(actual[row_idx], expected[row_idx]):
                raise AssertionError(
                    f"Top-k mismatch for query row {row_idx}: expected {expected[row_idx]}, got {actual[row_idx]}"
                )
        raise AssertionError("query_result_ids.npy did not match expected top-k ids")

    print(f"Verified query_result_ids.npy with shape {actual.shape}")


if __name__ == "__main__":
    main()
