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
        description=(
            "Find the best query batch size per dataset using the "
            "'all,op,...,rank_v/s' row in query_summary.txt files."
        )
    )
    parser.add_argument(
        "root",
        nargs="?",
        default="aurora/intialBatchSweep",
        help="Root directory containing dataset subdirectories (default: aurora/intialBatchSweep)",
    )
    return parser.parse_args()


def extract_batch_size(path: Path) -> int:
    match = BATCH_RE.search(path.as_posix())
    if not match:
        raise ValueError(f"Could not extract batch size from path: {path}")
    return int(match.group(1))


def extract_dataset(root: Path, summary_path: Path) -> str:
    rel_parts = summary_path.relative_to(root).parts
    if not rel_parts:
        raise ValueError(f"Could not determine dataset for path: {summary_path}")
    return rel_parts[0]


def parse_summary(summary_path: Path) -> float:
    with summary_path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if row["rank"] == "all" and row["operation"] == "op":
                return float(row["rank_v/s"])
    raise ValueError(f"Missing all/op row in {summary_path}")


def collect_results(root: Path) -> dict[str, list[dict[str, object]]]:
    results: dict[str, list[dict[str, object]]] = defaultdict(list)

    for summary_path in sorted(root.rglob("query_summary.txt")):
        dataset = extract_dataset(root, summary_path)
        batch_size = extract_batch_size(summary_path.parent)
        rank_vs = parse_summary(summary_path)
        results[dataset].append(
            {
                "batch_size": batch_size,
                "rank_vs": rank_vs,
                "summary_path": summary_path,
                "run_dir": summary_path.parent,
            }
        )

    return results


def print_results(results: dict[str, list[dict[str, object]]]) -> None:
    if not results:
        print("No query_summary.txt files found.")
        return

    print("Best batch size per dataset using all/op rank_v/s")
    print("=" * 48)
    for dataset in sorted(results):
        entries = sorted(
            results[dataset],
            key=lambda item: (-float(item["rank_vs"]), int(item["batch_size"]), str(item["run_dir"])),
        )
        best = entries[0]
        print(
            f"{dataset}: batch_size={best['batch_size']} "
            f"rank_v/s={best['rank_vs']:.12f} "
            f"run={best['run_dir']}"
        )

    print()
    print("All runs by dataset")
    print("=" * 19)
    for dataset in sorted(results):
        print(dataset)
        entries = sorted(
            results[dataset],
            key=lambda item: (int(item["batch_size"]), -float(item["rank_vs"]), str(item["run_dir"])),
        )
        for entry in entries:
            print(
                f"  batch_size={entry['batch_size']:<5d} "
                f"rank_v/s={entry['rank_vs']:.12f} "
                f"run={entry['run_dir']}"
            )
        print()


def main() -> int:
    args = parse_args()
    root = Path(args.root).resolve()

    if not root.exists():
        print(f"Root directory does not exist: {root}", file=sys.stderr)
        return 1

    results = collect_results(root)
    print_results(results)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
