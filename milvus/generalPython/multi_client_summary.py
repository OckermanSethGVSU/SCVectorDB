import csv
import os
from pathlib import Path

import numpy as np


SUMMARY_HEADER = [
    "run",
    "batch_size",
    "rank",
    "operation",
    "total",
    "mean",
    "std",
    "p99",
    "rank_op/s",
    "rank_v/s",
]


def parse_batch_sizes(raw_value):
    value = raw_value.strip()
    if not value:
        raise ValueError("empty batch size")

    try:
        batch_size = int(value)
        if batch_size <= 0:
            raise ValueError(f"invalid batch size {raw_value!r}")
        return [batch_size]
    except ValueError:
        pass

    cleaned = value.strip("()[]")
    fields = [field for field in cleaned.replace(",", " ").split() if field]
    if not fields:
        raise ValueError(f"invalid batch size list {raw_value!r}")

    batch_sizes = []
    for field in fields:
        batch_size = int(field)
        if batch_size <= 0:
            raise ValueError(f"invalid batch size entry {field!r} in {raw_value!r}")
        batch_sizes.append(batch_size)

    return batch_sizes


def summarize_npy(path, run_label, rank, name, batch_size):
    arr = np.load(path) * 1000
    total_ms = np.sum(arr)
    total_s = total_ms / 1000
    return [
        run_label,
        batch_size,
        rank,
        name,
        total_ms,
        np.mean(arr),
        np.std(arr),
        np.percentile(arr, 99),
        len(arr) / total_s,
        (batch_size * len(arr)) / total_s,
    ], arr


def ag_stats(run_label, batch_size, rank, name, arr, total_time, corpus_size):
    return [
        run_label,
        batch_size,
        rank,
        name,
        np.sum(arr),
        np.mean(arr),
        np.std(arr),
        np.percentile(arr, 99),
        len(arr) / total_time,
        corpus_size / total_time,
    ]


def extract_time(times_path, rank):
    target_row = None
    with times_path.open(newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) <= 10:
                continue
            if row[0] == str(rank):
                target_row = row
                break

    if target_row is None:
        raise ValueError(f"could not find rank {rank} in {times_path}")

    return float(target_row[10])


def discover_runs(base_dir, active_task, batch_sizes):
    if len(batch_sizes) == 1:
        return [(base_dir, batch_sizes[0], f"batch_{batch_sizes[0]}")]

    if active_task != "QUERY":
        raise ValueError("batch size sweeps are only supported for QUERY")

    runs = []
    for idx, batch_size in enumerate(batch_sizes):
        run_dir = base_dir / f"query_batch_{batch_size}_run_{idx:02d}"
        if not run_dir.is_dir():
            raise FileNotFoundError(f"missing sweep directory: {run_dir}")
        runs.append((run_dir, batch_size, run_dir.name))

    return runs


def summarize_run(run_dir, batch_size, run_label, clients, clients_per_proxy, corpus_size):
    summary_rows = []
    all_prep = []
    all_upload = []
    all_op = []

    for worker in range(clients):
        for client in range(clients_per_proxy):
            prep, prep_arr = summarize_npy(
                run_dir / f"batch_construction_times_w{worker}_c{client}.npy",
                run_label,
                worker,
                "prep",
                batch_size,
            )
            upload, upload_arr = summarize_npy(
                run_dir / f"upload_times_w{worker}_c{client}.npy",
                run_label,
                worker,
                "upload",
                batch_size,
            )
            op, op_arr = summarize_npy(
                run_dir / f"op_times_w{worker}_c{client}.npy",
                run_label,
                worker,
                "op",
                batch_size,
            )
            summary_rows.extend([prep, upload, op])
            all_prep.append(prep_arr)
            all_upload.append(upload_arr)
            all_op.append(op_arr)

    stacked_prep = np.concatenate(all_prep)
    stacked_upload = np.concatenate(all_upload)
    stacked_op = np.concatenate(all_op)
    aggregate_time = extract_time(run_dir / "times.csv", 0)

    summary_rows.extend(
        [
            ag_stats(run_label, batch_size, "all", "prep", stacked_prep, aggregate_time, corpus_size),
            ag_stats(run_label, batch_size, "all", "upload", stacked_upload, aggregate_time, corpus_size),
            ag_stats(run_label, batch_size, "all", "op", stacked_op, aggregate_time, corpus_size),
        ]
    )

    with (run_dir / "summary.csv").open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(SUMMARY_HEADER)
        writer.writerows(summary_rows)

    return summary_rows


def main():
    active_task = os.getenv("ACTIVE_TASK", "").strip().upper()
    if not active_task:
        raise ValueError("ACTIVE_TASK is required")

    corpus_size = int(os.getenv(f"{active_task}_CORPUS_SIZE"))
    clients_per_proxy = int(os.getenv(f"{active_task}_CLIENTS_PER_PROXY"))
    clients = int(os.getenv("NUM_PROXIES"))
    batch_sizes = parse_batch_sizes(os.getenv(f"{active_task}_BATCH_SIZE", ""))

    base_dir = Path(".")
    runs = discover_runs(base_dir, active_task, batch_sizes)

    combined_rows = []
    for run_dir, batch_size, run_label in runs:
        combined_rows.extend(
            summarize_run(run_dir, batch_size, run_label, clients, clients_per_proxy, corpus_size)
        )

    with (base_dir / "summary.csv").open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(SUMMARY_HEADER)
        writer.writerows(combined_rows)


if __name__ == "__main__":
    main()
