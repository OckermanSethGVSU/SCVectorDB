import csv
import os
from pathlib import Path

import numpy as np


def env_required(name: str) -> str:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        raise RuntimeError(f"missing required environment variable: {name}")
    return value.strip()


def get_active_task() -> str:
    raw = os.getenv("ACTIVE_TASK") or os.getenv("TASK")
    if raw is None:
        raise RuntimeError("missing ACTIVE_TASK/TASK")
    task = raw.strip().upper()
    if task not in {"INSERT", "QUERY"}:
        raise RuntimeError(f"unsupported ACTIVE_TASK/TASK: {raw}")
    return task


def summarize_npy(path: Path, rank: int, name: str, batch_size: int):
    arr = np.load(path) * 1000.0
    total_ms = float(np.sum(arr))
    total_sec = total_ms / 1000.0
    op_count = len(arr)
    ops_per_sec = op_count / total_sec if total_sec else 0.0
    vecs_per_sec = (batch_size * op_count) / total_sec if total_sec else 0.0
    return [rank, name, total_ms, float(np.mean(arr)), float(np.std(arr)), float(np.percentile(arr, 99)), ops_per_sec, vecs_per_sec], arr


def aggregate_stats(name: str, arr: np.ndarray, total_time_sec: float, corpus_size: int):
    total_ms = float(np.sum(arr))
    op_count = len(arr)
    ops_per_sec = op_count / total_time_sec if total_time_sec else 0.0
    vecs_per_sec = corpus_size / total_time_sec if total_time_sec else 0.0
    return ["all", name, total_ms, float(np.mean(arr)), float(np.std(arr)), float(np.percentile(arr, 99)), ops_per_sec, vecs_per_sec]


def extract_total_time(csv_path: Path) -> float:
    with csv_path.open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("rank") == "0":
                return float(row["total"])
    raise RuntimeError(f"rank 0 total not found in {csv_path}")


def main():
    task = get_active_task()
    prefix = task.lower()
    batch_size = int(env_required(f"{task}_BATCH_SIZE"))
    corpus_size = int(env_required(f"{task}_CORPUS_SIZE"))
    n_workers = int(env_required("N_WORKERS"))
    clients_per_worker = int(env_required(f"{task}_CLIENTS_PER_WORKER"))
    clients = n_workers * clients_per_worker

    if task == "INSERT":
        main_name = "insert"
        main_prefix = "upload_times"
    else:
        main_name = "query"
        main_prefix = "query_times"

    summary_path = Path(f"{prefix}_summary.csv")
    rank_summary_path = Path(f"{prefix}_rank_summary.csv")
    times_csv = Path(f"{prefix}_times.csv")

    with rank_summary_path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["rank", "operation", "total", "mean", "std", "p99", "rank_op/s", "rank_v/s"])

    all_prep = []
    all_main = []
    all_op = []

    for rank in range(clients):
        prep, prep_arr = summarize_npy(Path(f"{prefix}_batch_construction_times_rank_{rank}.npy"), rank, "prep", batch_size)
        main_stats, main_arr = summarize_npy(Path(f"{prefix}_{main_prefix}_rank_{rank}.npy"), rank, main_name, batch_size)
        op, op_arr = summarize_npy(Path(f"{prefix}_op_times_rank_{rank}.npy"), rank, "op", batch_size)

        with rank_summary_path.open("a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(prep)
            writer.writerow(main_stats)
            writer.writerow(op)

        all_prep.append(prep_arr)
        all_main.append(main_arr)
        all_op.append(op_arr)

    total_time_sec = extract_total_time(times_csv)
    stacked_prep = np.concatenate(all_prep)
    stacked_main = np.concatenate(all_main)
    stacked_op = np.concatenate(all_op)

    with summary_path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["rank", "operation", "total", "mean", "std", "p99", "rank_op/s", "rank_v/s"])
        writer.writerow(aggregate_stats("prep", stacked_prep, total_time_sec, corpus_size))
        writer.writerow(aggregate_stats(main_name, stacked_main, total_time_sec, corpus_size))
        writer.writerow(aggregate_stats("op", stacked_op, total_time_sec, corpus_size))


if __name__ == "__main__":
    main()
