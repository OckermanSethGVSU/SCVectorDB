from pathlib import Path
import re

import matplotlib.pyplot as plt
import numpy as np

SUPERCOMPUTER_NAME = "SUPERCOMPUTER1"
ROOT = Path("coreTesting/polaris")
UNRESTRICTED_ROOT = Path("polaris")
ROUNDS = ["round1", "round2", "round3"]
POINT_COUNT = 10_000_000

DATASET_LABELS = {
    "pes2o": "HPC-Pes2o",
    "yandex": "Yandex-Text-to-Image",
}

SERIES = [
    ("Milvus", "milvus"),
    ("Qdrant", "qdrant"),
]


def _extract_cores(run_dir_name: str) -> int | None:
    match = re.search(r"CORES(\d+)", run_dir_name)
    return int(match.group(1)) if match else None


def _read_index_time(index_time_path: Path) -> float | None:
    try:
        return float(index_time_path.read_text().strip().split()[0])
    except (OSError, ValueError, IndexError):
        return None


def _extract_point_count(run_dir_name: str, db_name: str) -> int | None:
    if db_name == "milvus":
        match = re.search(r"_N1_(\d+)_\d{4}-\d{2}-\d{2}_", run_dir_name)
    elif db_name == "qdrant":
        match = re.search(r"_CS(\d+)_\d{4}-\d{2}-\d{2}_", run_dir_name)
    else:
        return None
    return int(match.group(1)) if match else None


def collect_core_values(root: Path, dataset: str, rounds: list[str], db_name: str) -> dict[int, list[float]]:
    values_by_core: dict[int, list[float]] = {}

    for round_name in rounds:
        db_root = root / dataset / round_name / db_name
        if not db_root.exists():
            continue

        for run_dir in db_root.iterdir():
            if not run_dir.is_dir():
                continue

            cores = _extract_cores(run_dir.name)
            if cores is None:
                continue

            value = _read_index_time(run_dir / "index_time.txt")
            if value is None:
                continue

            values_by_core.setdefault(cores, []).append(value)

    return values_by_core


def collect_unrestricted_values(root: Path, dataset: str, rounds: list[str], db_name: str) -> list[float]:
    values: list[float] = []

    for round_name in rounds:
        db_root = root / dataset / round_name / db_name
        if not db_root.exists():
            continue

        for run_dir in db_root.iterdir():
            if not run_dir.is_dir():
                continue
            if "GPUFalse" not in run_dir.name:
                continue
            if _extract_point_count(run_dir.name, db_name) != POINT_COUNT:
                continue

            value = _read_index_time(run_dir / "index_time.txt")
            if value is not None:
                values.append(value)

    return values


def mean_and_std(values: list[float]) -> tuple[float, float]:
    if not values:
        return 0.0, 0.0
    arr = np.asarray(values, dtype=float)
    return float(arr.mean()), float(arr.std(ddof=0))


def series_stats(
    root: Path, dataset: str, rounds: list[str], db_name: str, core_counts: list[int]
) -> tuple[list[float], list[float]]:
    values_by_core = collect_core_values(root, dataset, rounds, db_name)

    means: list[float] = []
    stds: list[float] = []
    for core_count in core_counts:
        mean, std = mean_and_std(values_by_core.get(core_count, []))
        means.append(mean)
        stds.append(std)
    return means, stds


def dataset_core_counts(root: Path, dataset: str, rounds: list[str], series_cfg: list[tuple[str, str]]) -> list[int]:
    core_counts: set[int] = set()
    for _, db_name in series_cfg:
        core_counts.update(collect_core_values(root, dataset, rounds, db_name))
    return sorted(core_counts)


def plot_panel(
    ax,
    root: Path,
    dataset: str,
    rounds: list[str],
    series_cfg: list[tuple[str, str]],
    title: str,
    annotation_fontsize: int,
):
    core_counts = dataset_core_counts(root, dataset, rounds, series_cfg)
    x_labels = [str(core_count) for core_count in core_counts] + ["Unrestricted"]
    x = np.arange(len(x_labels))
    bar_width = 0.35
    offsets = np.arange(len(series_cfg)) - (len(series_cfg) - 1) / 2
    offsets = offsets * bar_width

    all_means: list[float] = []
    all_stds: list[float] = []
    plotted_bars = []

    for i, (label, db_name) in enumerate(series_cfg):
        means, stds = series_stats(root, dataset, rounds, db_name, core_counts)
        unrestricted_mean, unrestricted_std = mean_and_std(
            collect_unrestricted_values(UNRESTRICTED_ROOT, dataset, rounds, db_name)
        )
        means.append(unrestricted_mean)
        stds.append(unrestricted_std)
        bars = ax.bar(x + offsets[i], means, bar_width, yerr=stds, capsize=3, label=label)
        plotted_bars.append((bars, means, stds))
        all_means.extend(means)
        all_stds.extend(stds)

    max_height = max(all_means) if all_means else 0.0
    offset = max(0.012 * max_height, 0.06) if max_height > 0 else 0.06

    for bars, means, stds in plotted_bars:
        for bar, value, std in zip(bars, means, stds):
            if value <= 0:
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
                rotation=0,
            )

    top = max((mean + std for mean, std in zip(all_means, all_stds)), default=0.0)

    ax.set_title(title, fontsize=16, fontweight="bold")
    ax.set_xticks(x)
    ax.set_xticklabels(x_labels, fontsize=13)
    ax.tick_params(axis="y", labelsize=13)
    return top


def print_stats(root: Path, dataset: str, rounds: list[str], series_cfg: list[tuple[str, str]]):
    core_counts = dataset_core_counts(root, dataset, rounds, series_cfg)
    print(f"\n{dataset} | {SUPERCOMPUTER_NAME} | root={root} | rounds={rounds}")
    for label, db_name in series_cfg:
        means, stds = series_stats(root, dataset, rounds, db_name, core_counts)
        unrestricted_mean, unrestricted_std = mean_and_std(
            collect_unrestricted_values(UNRESTRICTED_ROOT, dataset, rounds, db_name)
        )
        stats = [f"{core_counts[i]}: mean={means[i]:.3f}, std={stds[i]:.3f}" for i in range(len(core_counts))]
        stats.append(f"Unrestricted: mean={unrestricted_mean:.3f}, std={unrestricted_std:.3f}")
        print(f"  {label:<7} " + " | ".join(stats))


def create_figure(output_path: str, annotation_fontsize: int):
    fig, axes = plt.subplots(2, 1, figsize=(10, 8), sharex=False)

    panel_tops = []
    for ax, dataset in zip(axes, ["pes2o", "yandex"]):
        top = plot_panel(
            ax,
            ROOT,
            dataset,
            ROUNDS,
            SERIES,
            DATASET_LABELS[dataset],
            annotation_fontsize,
        )
        panel_tops.append(top)
        ax.set_ylabel("Indexing Time (s)", fontsize=14)
        print_stats(ROOT, dataset, ROUNDS, SERIES)

    for ax, top in zip(axes, panel_tops):
        if top > 0:
            ax.set_ylim(0, top * 1.23)

    axes[0].legend(loc="upper right", fontsize=12, frameon=False)
    axes[-1].set_xlabel("Number of Cores", fontsize=14)
    fig.tight_layout(rect=[0, 0, 1, 0.95])
    fig.savefig(output_path, format="pdf", bbox_inches="tight")
    return fig


create_figure("indexing_time_SUPERCOMPUTER1_cores.pdf", 10)
plt.show()
