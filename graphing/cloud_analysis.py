import numpy as np
from collections import defaultdict

# Raw benchmark data from your table
# Times are in seconds; latency in ms.
data = {
    ("Milvus", "GIST"): {
        "cloud": {"upload_s": 84.968,  "index_s": 487.061, "qps": 281.53,  "lat_ms": 322.63},
        "hpc":   {"upload_s": 197.29,  "index_s": 84.50,   "qps": 1470.29, "lat_ms": 23.85},
    },
    ("Milvus", "dbpedia-openai"): {
        "cloud": {"upload_s": 176.66,  "index_s": 886.152, "qps": 154.06,  "lat_ms": 582.51},
        "hpc":   {"upload_s": 129.03,  "index_s": 205.41,  "qps": 799.50,  "lat_ms": 46.87},
    },
    ("Qdrant", "GIST"): {
        "cloud": {"upload_s": 146.29,  "index_s": 2014.81, "qps": 433.95,  "lat_ms": 207.28},
        "hpc":   {"upload_s": 45.16,   "index_s": 215.22,  "qps": 1373.26, "lat_ms": 58.31},
    },
    ("Qdrant", "dbpedia-openai"): {
        "cloud": {"upload_s": 238.42,  "index_s": 671.83,  "qps": 1260.53, "lat_ms": 3.26},
        "hpc":   {"upload_s": 62.41,   "index_s": 85.12,   "qps": 1738.55, "lat_ms": 3.24},
    },
    ("Weaviate", "GIST"): {
        "cloud": {"upload_s": 836.96,  "index_s": None,    "qps": 496.17,  "lat_ms": 184.37},
        "hpc":   {"upload_s": 281.84,  "index_s": None,    "qps": 2481.86, "lat_ms": 30.72},
    },
    ("Weaviate", "dbpedia-openai"): {
        "cloud": {"upload_s": 836.96,  "index_s": None,    "qps": 1142.13, "lat_ms": 4.99},
        "hpc":   {"upload_s": 395.53,  "index_s": None,    "qps": 1518.28, "lat_ms": 4.87},
    },
}


def speedup(cloud_val, hpc_val):
    """
    Returns cloud/hpc speedup (>1 means HPC is faster).
    """
    if cloud_val is None or hpc_val is None:
        return None
    return cloud_val / hpc_val


def summarize(arr):
    arr = np.array(arr, dtype=float)
    return {
        "n": int(arr.size),
        "min": float(arr.min()),
        "mean": float(arr.mean()),
        "max": float(arr.max()),
        "gmean": float(np.exp(np.mean(np.log(arr)))),  # geometric mean (nice for ratios)
    }


# --- Per-row speedups (Cloud -> HPC) ---
rows = []
for (system, dataset), vals in data.items():
    up = speedup(vals["cloud"]["upload_s"], vals["hpc"]["upload_s"])
    ix = speedup(vals["cloud"]["index_s"],  vals["hpc"]["index_s"])
    rows.append((system, dataset, up, ix))

print("Per (system, dataset) speedups where >1 means HPC is faster:\n")
for system, dataset, up, ix in rows:
    up_s = "N/A" if up is None else f"{up:.2f}x"
    ix_s = "N/A" if ix is None else f"{ix:.2f}x"
    print(f"{system:8s} | {dataset:14s} | upload: {up_s:>7s} | index: {ix_s:>7s}")


# --- Overall ranges (upload + index) ---
upload_speedups = [up for _, _, up, _ in rows if up is not None]
index_speedups  = [ix for _, _, _, ix in rows if ix is not None]

print("\nOverall summary:")
print("Upload speedup (cloud/hpc):", summarize(upload_speedups))
print("Index  speedup (cloud/hpc):", summarize(index_speedups))


# --- Grouped by dataset ---
by_dataset_upload = defaultdict(list)
by_dataset_index  = defaultdict(list)

for system, dataset, up, ix in rows:
    if up is not None:
        by_dataset_upload[dataset].append(up)
    if ix is not None:
        by_dataset_index[dataset].append(ix)

print("\nBy-dataset summary:")
for dataset in sorted(set(d for _, d in data.keys())):
    print(f"\nDataset: {dataset}")
    if by_dataset_upload[dataset]:
        print("  Upload speedup:", summarize(by_dataset_upload[dataset]))
    else:
        print("  Upload speedup: N/A")
    if by_dataset_index[dataset]:
        print("  Index  speedup:", summarize(by_dataset_index[dataset]))
    else:
        print("  Index  speedup: N/A")


# --- Grouped by system ---
by_system_upload = defaultdict(list)
by_system_index  = defaultdict(list)

for system, dataset, up, ix in rows:
    if up is not None:
        by_system_upload[system].append(up)
    if ix is not None:
        by_system_index[system].append(ix)

print("\nBy-system summary:")
for system in sorted(by_system_upload.keys()):
    print(f"\nSystem: {system}")
    print("  Upload speedup:", summarize(by_system_upload[system]))
    if by_system_index[system]:
        print("  Index  speedup:", summarize(by_system_index[system]))
    else:
        print("  Index  speedup: N/A (missing index times)")