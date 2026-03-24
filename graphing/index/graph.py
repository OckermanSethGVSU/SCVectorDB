from pathlib import Path
import re

import matplotlib.pyplot as plt
import numpy as np

POINT_COUNTS = [1_000_000, 5_000_000, 10_000_000]
POINT_LABELS = ["1M", "5M", "10M"]
X = np.arange(len(POINT_COUNTS))

SUPERCOMPUTERS = {
    "SUPERCOMPUTER1": {"root": Path("polaris"), "rounds": ["round1", "round2", "round3"]},
    "SUPERCOMPUTER2": {"root": Path("aurora"), "rounds": ["round1", "round2", "round3"]},
}

DATASET_LABELS = {
    "pes2o": "HPC-Pes2o",
    "yandex": "Yandex-Text-to-Image",
}

LEFT_SERIES = [
    ("Milvus", "milvus", False),
    ("Qdrant", "qdrant", False),
    ("Weaviate", "weaviate", False),
    ("GPU-Milvus", "milvus", True),
    ("GPU-Qdrant", "qdrant", True),
]

RIGHT_SERIES = [
    ("Milvus", "milvus", False),
    ("Qdrant", "qdrant", False),
    ("Weaviate", "weaviate", False),
]


def _extract_point_count(run_dir_name: str, db_name: str) -> int | None:
    if db_name == "milvus":
        patterns = [
            r"_N1_(\d+)_\d{4}-\d{2}-\d{2}_",
            r"_(\d+)_memory_N1_\d{4}-\d{2}-\d{2}_",
        ]
        for pattern in patterns:
            match = re.search(pattern, run_dir_name)
            if match:
                return int(match.group(1))
        return None
    elif db_name == "qdrant":
        match = re.search(r"_CS(\d+)_\d{4}-\d{2}-\d{2}_", run_dir_name)
    else:
        return None
    return int(match.group(1)) if match else None


def _read_index_time(index_time_path: Path) -> float | None:
    try:
        return float(index_time_path.read_text().strip().split()[0])
    except (ValueError, IndexError):
        return None


def collect_values(root: Path, dataset: str, rounds: list[str], db_name: str, use_gpu: bool, point_count: int) -> list[float]:
    if db_name == "weaviate":
        return []

    values: list[float] = []
    gpu_token = "GPUTrue" if use_gpu else "GPUFalse"

    for round_name in rounds:
        db_root = root / dataset / round_name / db_name
        if not db_root.exists():
            continue

        for run_dir in db_root.iterdir():
            if not run_dir.is_dir():
                continue
            name = run_dir.name
            has_gpu_flag = "GPUTrue" in name or "GPUFalse" in name
            if has_gpu_flag and gpu_token not in name:
                continue
            if use_gpu and not has_gpu_flag:
                continue
            if _extract_point_count(name, db_name) != point_count:
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


def series_stats(root: Path, dataset: str, rounds: list[str], db_name: str, use_gpu: bool) -> tuple[list[float], list[float]]:
    means: list[float] = []
    stds: list[float] = []
    for point_count in POINT_COUNTS:
        values = collect_values(root, dataset, rounds, db_name, use_gpu, point_count)
        mean, std = mean_and_std(values)
        means.append(mean)
        stds.append(std)
    return means, stds


def plot_panel(ax, root: Path, dataset: str, rounds: list[str], series_cfg: list[tuple[str, str, bool]], bar_width: float, title: str):
    offsets = np.arange(len(series_cfg)) - (len(series_cfg) - 1) / 2
    offsets = offsets * bar_width

    all_means: list[float] = []
    plotted_bars = []

    for i, (label, db_name, use_gpu) in enumerate(series_cfg):
        means, stds = series_stats(root, dataset, rounds, db_name, use_gpu)
        bars = ax.bar(X + offsets[i], means, bar_width, yerr=stds, capsize=3, label=label)
        plotted_bars.append((bars, means, stds))
        all_means.extend(means)

    max_height = max(all_means) if all_means else 0.0
    offset = max(0.012 * max_height, 0.06) if max_height > 0 else 0.06

    for bars, means, stds in plotted_bars:
        for bar, value, std in zip(bars, means, stds):
            if value <= 0:
                continue
            x = bar.get_x() + bar.get_width() / 2
            label = f"{value:.1f}s"
            ax.text(
                x,
                value + std + offset,
                label,
                ha="center",
                va="bottom",
                fontsize=8,
                rotation=60,
            )

    # Return the height needed so shared-y rows can use the tallest panel.
    top = max((m + s for m, s in zip(all_means, [st for _, _, stds in plotted_bars for st in stds])), default=0.0)

    ax.set_title(title)
    ax.set_xticks(X)
    ax.set_xticklabels(POINT_LABELS)
    return top


def print_stats(root: Path, dataset: str, sc_name: str, rounds: list[str], series_cfg: list[tuple[str, str, bool]]):
    print(f"\n{dataset} | {sc_name} | root={root.name} | rounds={rounds}")
    for label, db_name, use_gpu in series_cfg:
        means, stds = series_stats(root, dataset, rounds, db_name, use_gpu)
        stats = [f"{POINT_LABELS[i]}: mean={means[i]:.3f}, std={stds[i]:.3f}" for i in range(len(POINT_LABELS))]
        print(f"  {label:<11} " + " | ".join(stats))


fig, axes = plt.subplots(2, 2, figsize=(14, 8), sharey="row")

bw_left = 0.15
bw_right = 0.22

# Row 1: pes2o
pes2o_left_top = plot_panel(axes[0, 0], SUPERCOMPUTERS["SUPERCOMPUTER1"]["root"], "pes2o", SUPERCOMPUTERS["SUPERCOMPUTER1"]["rounds"], LEFT_SERIES, bw_left, "SUPERCOMPUTER1")
pes2o_right_top = plot_panel(axes[0, 1], SUPERCOMPUTERS["SUPERCOMPUTER2"]["root"], "pes2o", SUPERCOMPUTERS["SUPERCOMPUTER2"]["rounds"], RIGHT_SERIES, bw_right, "SUPERCOMPUTER2")
pes2o_row_top = max(pes2o_left_top, pes2o_right_top)
if pes2o_row_top > 0:
    axes[0, 0].set_ylim(0, pes2o_row_top * 1.23)
    axes[0, 1].set_ylim(0, pes2o_row_top * 1.23)
print_stats(SUPERCOMPUTERS["SUPERCOMPUTER1"]["root"], "pes2o", "SUPERCOMPUTER1", SUPERCOMPUTERS["SUPERCOMPUTER1"]["rounds"], LEFT_SERIES)
print_stats(SUPERCOMPUTERS["SUPERCOMPUTER2"]["root"], "pes2o", "SUPERCOMPUTER2", SUPERCOMPUTERS["SUPERCOMPUTER2"]["rounds"], RIGHT_SERIES)
axes[0, 0].set_ylabel("Indexing Time (s)")

# Row 2: yandex
yandex_left_top = plot_panel(axes[1, 0], SUPERCOMPUTERS["SUPERCOMPUTER1"]["root"], "yandex", SUPERCOMPUTERS["SUPERCOMPUTER1"]["rounds"], LEFT_SERIES, bw_left, "SUPERCOMPUTER1")
yandex_right_top = plot_panel(axes[1, 1], SUPERCOMPUTERS["SUPERCOMPUTER2"]["root"], "yandex", SUPERCOMPUTERS["SUPERCOMPUTER2"]["rounds"], RIGHT_SERIES, bw_right, "SUPERCOMPUTER2")
yandex_row_top = max(yandex_left_top, yandex_right_top)
if yandex_row_top > 0:
    axes[1, 0].set_ylim(0, yandex_row_top * 1.23)
    axes[1, 1].set_ylim(0, yandex_row_top * 1.23)
print_stats(SUPERCOMPUTERS["SUPERCOMPUTER1"]["root"], "yandex", "SUPERCOMPUTER1", SUPERCOMPUTERS["SUPERCOMPUTER1"]["rounds"], LEFT_SERIES)
print_stats(SUPERCOMPUTERS["SUPERCOMPUTER2"]["root"], "yandex", "SUPERCOMPUTER2", SUPERCOMPUTERS["SUPERCOMPUTER2"]["rounds"], RIGHT_SERIES)
axes[1, 0].set_ylabel("Indexing Time (s)")
axes[1, 0].set_xlabel("Number of Points")
axes[1, 1].set_xlabel("Number of Points")
axes[0, 1].tick_params(axis="y", labelleft=True)
axes[1, 1].tick_params(axis="y", labelleft=True)

fig.text(0.02, 0.73, DATASET_LABELS["pes2o"], rotation=90, va="center", ha="center", fontsize=11)
fig.text(0.02, 0.28, DATASET_LABELS["yandex"], rotation=90, va="center", ha="center", fontsize=11)

handles, labels = axes[0, 0].get_legend_handles_labels()
fig.legend(handles, labels, loc="upper center", ncol=5, bbox_to_anchor=(0.5, 1.02))

plt.tight_layout(rect=[0.04, 0.03, 1, 0.94])
plt.savefig("indexing_time_hpc_pes2o_deep1b.pdf", format="pdf", bbox_inches="tight")
plt.show()
