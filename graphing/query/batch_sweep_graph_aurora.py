from __future__ import annotations

from pathlib import Path
import csv
import re

import matplotlib.pyplot as plt
import numpy as np

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR / "aurora"
SWEEP_DIR_CANDIDATES = ["initalBatchSweep", "intialBatchSweep"]
DATASETS = ["pes2o", "yandex"]
SERIES = [
    ("Milvus", "milvus"),
    ("Qdrant", "qdrant"),
]
DATASET_LABELS = {
    "pes2o": "HPC-Pes2o",
    "yandex": "Yandex-Text-to-Image",
}


def _extract_batch_size(run_dir_name: str, db_name: str) -> int | None:
    if db_name == "milvus":
        match = re.search(r"_N1_(\d+)_\d{4}-\d{2}-\d{2}_", run_dir_name)
    elif db_name == "qdrant":
        match = re.search(r"_BS(\d+)_", run_dir_name)
    else:
        return None
    return int(match.group(1)) if match else None


def _metric_from_milvus_summary(path: Path) -> float | None:
    try:
        with path.open(newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("rank") == "all" and row.get("operation") == "op":
                    return float(row["rank_v/s"])
    except (OSError, ValueError, KeyError):
        return None
    return None


def _metric_from_qdrant_summary(path: Path) -> float | None:
    try:
        with path.open(newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("rank") == "all" and row.get("operation") == "op":
                    return float(row["rank_v/s"])
    except (OSError, ValueError, KeyError):
        return None
    return None


def _summary_metric(path: Path, db_name: str) -> float | None:
    if db_name == "milvus":
        return _metric_from_milvus_summary(path)
    if db_name == "qdrant":
        return _metric_from_qdrant_summary(path)
    return None


def collect_batch_values(root: Path, dataset: str, db_name: str) -> dict[int, list[float]]:
    values_by_batch: dict[int, list[float]] = {}

    for sweep_dir_name in SWEEP_DIR_CANDIDATES:
        db_root = root / sweep_dir_name / dataset / db_name
        if not db_root.exists():
            continue

        for run_dir in db_root.iterdir():
            if not run_dir.is_dir():
                continue

            batch_size = _extract_batch_size(run_dir.name, db_name)
            if batch_size is None:
                continue

            summary_name = "query_summary.txt" if db_name == "milvus" else "query_summary.csv"
            value = _summary_metric(run_dir / summary_name, db_name)
            if value is None:
                continue

            values_by_batch.setdefault(batch_size, []).append(value)

    return values_by_batch


def mean_and_std(values: list[float]) -> tuple[float | None, float | None]:
    if not values:
        return None, None
    arr = np.asarray(values, dtype=float)
    return float(arr.mean()), float(arr.std(ddof=0))


def dataset_batch_sizes(root: Path, dataset: str, series_cfg: list[tuple[str, str]]) -> list[int]:
    batch_sizes: set[int] = set()
    for _, db_name in series_cfg:
        batch_sizes.update(collect_batch_values(root, dataset, db_name))
    return sorted(batch_sizes)


def series_stats(root: Path, dataset: str, db_name: str, batch_sizes: list[int]) -> tuple[list[float], list[float]]:
    values_by_batch = collect_batch_values(root, dataset, db_name)

    means: list[float] = []
    stds: list[float] = []
    for batch_size in batch_sizes:
        values = values_by_batch.get(batch_size, [])
        mean, std = mean_and_std(values)
        means.append(np.nan if mean is None else mean)
        if std is None or len(values) <= 1:
            stds.append(0.0)
        else:
            stds.append(std)
    return means, stds


def plot_panel(ax, root: Path, dataset: str, title: str, annotation_fontsize: int):
    batch_sizes = dataset_batch_sizes(root, dataset, SERIES)
    x_labels = [str(batch_size) for batch_size in batch_sizes]
    x = np.arange(len(batch_sizes))
    bar_width = 0.32
    offsets = np.arange(len(SERIES)) - (len(SERIES) - 1) / 2
    offsets = offsets * bar_width

    all_tops: list[float] = []
    plotted_bars = []

    for i, (label, db_name) in enumerate(SERIES):
        means, stds = series_stats(root, dataset, db_name, batch_sizes)
        bars = ax.bar(x + offsets[i], means, bar_width, yerr=stds, capsize=3, label=label)
        plotted_bars.append((bars, means, stds))
        for mean, std in zip(means, stds):
            if np.isfinite(mean):
                all_tops.append(mean + std)

    max_height = max(all_tops, default=0.0)
    offset = max(0.012 * max_height, 0.5) if max_height > 0 else 0.5

    for bars, means, stds in plotted_bars:
        for bar, value, std in zip(bars, means, stds):
            if not np.isfinite(value) or value <= 0:
                continue
            x_pos = bar.get_x() + bar.get_width() / 2
            ax.text(
                x_pos,
                value + std + offset,
                f"{value:.0f}",
                ha="center",
                va="bottom",
                fontsize=annotation_fontsize,
                fontweight="bold",
            )

    ax.set_title(title, fontsize=16, fontweight="bold")
    ax.set_xticks(x)
    ax.set_xticklabels(x_labels, fontsize=12)
    ax.tick_params(axis="y", labelsize=12)
    return max_height


def print_stats(root: Path, dataset: str):
    batch_sizes = dataset_batch_sizes(root, dataset, SERIES)
    print(f"\n{dataset} | Aurora | root={root}")
    for label, db_name in SERIES:
        means, stds = series_stats(root, dataset, db_name, batch_sizes)
        stats = []
        for i, batch_size in enumerate(batch_sizes):
            if np.isfinite(means[i]):
                stats.append(f"{batch_size}: mean={means[i]:.3f}, std={stds[i]:.3f}")
            else:
                stats.append(f"{batch_size}: mean=n/a, std=n/a")
        print(f"  {label:<7} " + " | ".join(stats))


def create_figure(output_path: str, annotation_fontsize: int):
    fig, axes = plt.subplots(2, 1, figsize=(12, 8), sharex=False)

    panel_tops = []
    for ax, dataset in zip(axes, DATASETS):
        top = plot_panel(ax, ROOT, dataset, DATASET_LABELS[dataset], annotation_fontsize)
        panel_tops.append(top)
        ax.set_ylabel("Query Throughput (vectors/s)", fontsize=13)
        print_stats(ROOT, dataset)

    for ax, top in zip(axes, panel_tops):
        if top > 0:
            ax.set_ylim(0, top * 1.22)

    axes[-1].legend(loc="upper left", fontsize=12, frameon=False)
    axes[-1].set_xlabel("Batch Size", fontsize=13)
    fig.tight_layout(rect=[0, 0, 1, 0.97])
    fig.savefig(output_path, format="pdf", bbox_inches="tight")
    return fig


create_figure(str(SCRIPT_DIR / "aurora_query_batch_sweep.pdf"), 9)
plt.show()
