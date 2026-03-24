#!/usr/bin/env python3

import argparse
import csv
import re
import sys
from collections import defaultdict
from pathlib import Path


BATCH_RE = re.compile(r"_N1_(\d+)_")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Print rank 0 upload mean latency for each batch size."
    )
    parser.add_argument(
        "root",
        nargs="?",
        default="aurora/intialBatchSweep",
        help="Root directory containing query batch sweep runs.",
    )
    parser.add_argument(
        "--show-run",
        action="store_true",
        help="Include the run directory in the output.",
    )
    return parser.parse_args()


def extract_batch_size(run_dir: Path) -> int:
    match = BATCH_RE.search(run_dir.name)
    if not match:
        raise ValueError(f"Could not extract batch size from {run_dir}")
    return int(match.group(1))


def read_summary_values(summary_path: Path) -> tuple[float, float]:
    rank0_upload_mean = None
    all_op_rank_vs = None

    with summary_path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if row["rank"] == "0" and row["operation"] == "upload":
                rank0_upload_mean = float(row["mean"])
            if row["rank"] == "all" and row["operation"] == "op":
                all_op_rank_vs = float(row["rank_v/s"])

    if rank0_upload_mean is None:
        raise ValueError(f"Missing rank 0 upload row in {summary_path}")
    if all_op_rank_vs is None:
        raise ValueError(f"Missing all op row in {summary_path}")

    return rank0_upload_mean, all_op_rank_vs


def main() -> int:
    args = parse_args()
    root = Path(args.root).resolve()
    if not root.exists():
        print(f"Root directory does not exist: {root}", file=sys.stderr)
        return 1

    results: dict[str, list[tuple[int, float, float, Path]]] = defaultdict(list)

    for summary_path in sorted(root.rglob("query_summary.txt")):
        dataset = summary_path.relative_to(root).parts[0]
        batch_size = extract_batch_size(summary_path.parent)
        rank0_upload_mean, all_op_rank_vs = read_summary_values(summary_path)
        results[dataset].append((batch_size, rank0_upload_mean, all_op_rank_vs, summary_path.parent))

    if not results:
        print("No query_summary.txt files found.", file=sys.stderr)
        return 1

    for dataset in sorted(results):
        print(dataset)
        for batch_size, mean, all_op_rank_vs, run_dir in sorted(results[dataset], key=lambda item: (item[0], str(item[3]))):
            line = (
                f"  batch_size={batch_size:<5d} "
                f"rank0_upload_mean={mean:.12f} "
                f"all_op_rank_v/s={all_op_rank_vs:.12f}"
            )
            if args.show_run:
                line += f" run={run_dir}"
            print(line)
        print()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
