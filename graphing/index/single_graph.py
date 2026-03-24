from pathlib import Path
import csv
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

WEAVIATE_STATS_FILE = Path("fig4_weaviate_v2.csv")
WEAVIATE_SUPERCOMPUTER_MAP = {
    "SUPERCOMPUTER1": "Polaris",
    "SUPERCOMPUTER2": "Aurora",
}


def load_weaviate_stats(csv_path: Path) -> dict[str, dict[str, dict[int, tuple[float, float]]]]:
    stats: dict[str, dict[str, dict[int, tuple[float, float]]]] = {}
    current_sc: str | None = None
    current_dataset: str | None = None

    with csv_path.open(newline="") as csv_file:
        reader = csv.reader(csv_file)
        for row in reader:
            cells = [cell.strip() for cell in row]
            if not any(cells):
                continue

            first_cell = cells[0]
            if first_cell in {"Polaris", "Aurora"}:
                current_sc = first_cell
                stats.setdefault(current_sc, {})
                current_dataset = None
                continue

            if cells[1:6] == ["Pes2o_r1", "Pes2o_r2", "Pes2o_r3", "mean", "std"]:
                current_dataset = "pes2o"
                stats[current_sc].setdefault(current_dataset, {})
                continue

            if cells[1:6] == ["Yandex_r1", "Yandex_r2", "Yandex_r3", "mean", "std"]:
                current_dataset = "yandex"
                stats[current_sc].setdefault(current_dataset, {})
                continue

            if current_sc is None or current_dataset is None or first_cell not in POINT_LABELS:
                continue

            point_count = POINT_COUNTS[POINT_LABELS.index(first_cell)]
            mean = float(cells[4])
            std = float(cells[5])
            stats[current_sc][current_dataset][point_count] = (mean, std)

    return stats


WEAVIATE_STATS = load_weaviate_stats(WEAVIATE_STATS_FILE)


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
    if db_name == "weaviate":
        sc_name = next(name for name, cfg in SUPERCOMPUTERS.items() if cfg["root"] == root)
        sc_key = WEAVIATE_SUPERCOMPUTER_MAP[sc_name]
        stats = WEAVIATE_STATS.get(sc_key, {}).get(dataset, {})
        means = [stats.get(point_count, (0.0, 0.0))[0] for point_count in POINT_COUNTS]
        stds = [stats.get(point_count, (0.0, 0.0))[1] for point_count in POINT_COUNTS]
        return means, stds

    means: list[float] = []
    stds: list[float] = []
    for point_count in POINT_COUNTS:
        values = collect_values(root, dataset, rounds, db_name, use_gpu, point_count)
        mean, std = mean_and_std(values)
        means.append(mean)
        stds.append(std)
    return means, stds


def plot_panel(
    ax,
    root: Path,
    dataset: str,
    rounds: list[str],
    series_cfg: list[tuple[str, str, bool]],
    bar_width: float,
    title: str,
    annotation_fontsize: int,
):
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
            label = f"{value:.0f}"
            ax.text(
                x,
                value + std + offset,
                label,
                ha="center",
                va="bottom",
                fontsize=annotation_fontsize,
                fontweight="bold",
                rotation=0,
            )

    # Return the height needed so shared-y rows can use the tallest panel.
    top = max((m + s for m, s in zip(all_means, [st for _, _, stds in plotted_bars for st in stds])), default=0.0)

    ax.set_title(title, fontsize=16, fontweight="bold")
    ax.set_xticks(X)
    ax.set_xticklabels(POINT_LABELS, fontsize=13)
    ax.tick_params(axis="y", labelsize=13)
    return top


def print_stats(root: Path, dataset: str, sc_name: str, rounds: list[str], series_cfg: list[tuple[str, str, bool]]):
    print(f"\n{dataset} | {sc_name} | root={root.name} | rounds={rounds}")
    for label, db_name, use_gpu in series_cfg:
        means, stds = series_stats(root, dataset, rounds, db_name, use_gpu)
        stats = [f"{POINT_LABELS[i]}: mean={means[i]:.3f}, std={stds[i]:.3f}" for i in range(len(POINT_LABELS))]
        print(f"  {label:<11} " + " | ".join(stats))


def create_supercomputer_figure(
    sc_name: str,
    series_cfg: list[tuple[str, str, bool]],
    bar_width: float,
    output_path: str,
    annotation_fontsize: int,
):
    sc = SUPERCOMPUTERS[sc_name]
    fig, axes = plt.subplots(2, 1, figsize=(10, 8), sharex=True)

    panel_tops = []
    for ax, dataset in zip(axes, ["pes2o", "yandex"]):
        top = plot_panel(
            ax,
            sc["root"],
            dataset,
            sc["rounds"],
            series_cfg,
            bar_width,
            DATASET_LABELS[dataset],
            annotation_fontsize,
        )
        panel_tops.append(top)
        ax.set_ylabel("Indexing Time (s)", fontsize=14)
        print_stats(sc["root"], dataset, sc_name, sc["rounds"], series_cfg)

    for ax, top in zip(axes, panel_tops):
        if top > 0:
            ax.set_ylim(0, top * 1.23)

    axes[0].tick_params(axis="x", labelbottom=True)
    axes[-1].set_xlabel("Number of Points", fontsize=14)

    axes[0].legend(loc="upper left", fontsize=12)
    fig.tight_layout(rect=[0, 0, 1, 0.95])
    fig.savefig(output_path, format="pdf", bbox_inches="tight")
    return fig


create_supercomputer_figure(
    "SUPERCOMPUTER1",
    LEFT_SERIES,
    0.15,
    "indexing_time_SUPERCOMPUTER1.pdf",
    10,
)
create_supercomputer_figure(
    "SUPERCOMPUTER2",
    RIGHT_SERIES,
    0.22,
    "indexing_time_SUPERCOMPUTER2.pdf",
    10,
)
plt.show()
