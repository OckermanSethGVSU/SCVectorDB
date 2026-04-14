#!/usr/bin/env python3

import numpy as np


def make_matrix(rows: int, dim: int, seed: int) -> np.ndarray:
    rng = np.random.default_rng(seed)
    return rng.standard_normal((rows, dim), dtype=np.float32)


def main() -> None:
    insert = make_matrix(1000, 200, seed=7)
    query = make_matrix(100, 200, seed=11)

    np.save("qdrant_base.npy", insert)
    np.save("qdrant_query.npy", query)

    print("wrote qdrant_base.npy with shape", insert.shape)
    print("wrote qdrant_query.npy with shape", query.shape)


if __name__ == "__main__":
    main()
