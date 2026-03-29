#!/usr/bin/env python3

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np


def build_matrix(rows: int, dim: int) -> np.ndarray:
    # Make every row easy to recognize when validating values in Qdrant.
    values = np.arange(rows * dim, dtype=np.float32)
    return values.reshape(rows, dim)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate a deterministic float32 .npy test matrix.")
    parser.add_argument("--output", required=True, help="Destination .npy path")
    parser.add_argument("--rows", type=int, default=8, help="Number of rows to generate")
    parser.add_argument("--dim", type=int, default=4, help="Vector dimension")
    args = parser.parse_args()

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)

    matrix = build_matrix(args.rows, args.dim)
    np.save(output, matrix)
    print(f"Wrote {output} with shape {matrix.shape} and dtype {matrix.dtype}")


if __name__ == "__main__":
    main()
