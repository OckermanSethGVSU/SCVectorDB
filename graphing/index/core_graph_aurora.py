from pathlib import Path
import re

import csv

import matplotlib.pyplot as plt
import numpy as np

SUPERCOMPUTER_NAME = "Aurora"
SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR / "aurora" / "coreTesting"
UNRESTRICTED_ROOT = SCRIPT_DIR / "aurora"
ROUNDS = ["round1", "round2", "round3"]
POINT_COUNT = 10_000_000
EXCLUDED_CORE_COUNTS = {1, 2, 104, 112}
WEAVIATE_CSV = SCRIPT_DIR / "aurora_core_weaviate.csv"

DATASET_LABELS = {
    "pes2o": "HPC-Pes2o",
    "yandex": "Yandex-Text-to-Image",
}

SERIES = [
    ("Milvus", "milvus"),
    ("Qdrant", "qdrant"),
    ("Weaviate", "weaviate"),
]


def _extract_cores(run_dir_name: str) -> int | None:
    match = re.search(r"CORES(\d+)", run_dir_name, flags=re.IGNORECASE)
    return int(match.group(1)) if match else None


def _read_index_time(index_time_path: Path) -> float | None:
    try:
        return float(index_time_path.read_text().strip().split()[0])
    except (OSError, ValueError, IndexError):
        return None


def _extract_point_count(run_dir_name: str, db_name: str) -> int | None:
    if db_name == "milvus":
        patterns = [
            r"_N1_(\d+)_\d{4}-\d{2}-\d{2}_",
            r"_(\d+)_memory_N1_\d{4}-\d{2}-\d{2}_",
        ]
    elif db_name == "qdrant":
        patterns = [r"_CS(\d+)_\d{4}-\d{2}-\d{2}_"]
    else:
        return None

    for pattern in patterns:
        match = re.search(pattern, run_dir_name, flags=re.IGNORECASE)
        if match:
            return int(match.group(1))
    return None


def _load_weaviate_csv(csv_path: Path) -> dict[str, dict[str, object]]:
    results: dict[str, dict[str, object]] = {}
    if not csv_path.exists():
        return results

    with csv_path.open(newline="") as f:
        rows = list(csv.reader(f))

    current_dataset: str | None = None
    for row in rows:
        if not row or not any(cell.strip() for cell in row):
            continue

        first = row[0].strip()
        if first in {"Aurora", "Polaris", ""}:
            continue
        if first in {"Pes2o", "Yandex"}:
            current_dataset = first.lower()
            results.setdefault(current_dataset, {"core_values": {}, "core_stds": {}, "unrestricted": [], "unrestricted_stds": []})
            continue
        if current_dataset is None:
            continue

        label = first.lower()
        if label == "unrestricted":
            try:
                results[current_dataset]["unrestricted"].append(float(row[4]))
                results[current_dataset]["unrestricted_stds"].append(float(row[5]))
            except (IndexError, ValueError, TypeError):
                pass
            continue

        try:
            core_count = int(first)
            mean_value = float(row[4])
            std_value = float(row[5])
        except (ValueError, IndexError, TypeError):
            continue

        results[current_dataset]["core_values"].setdefault(core_count, []).append(mean_value)
        results[current_dataset]["core_stds"][core_count] = std_value

    return results


WEAVIATE_DATA = _load_weaviate_csv(WEAVIATE_CSV)


def collect_core_values(root: Path, dataset: str, rounds: list[str], db_name: str) -> dict[int, list[float]]:
    if db_name == "weaviate":
        dataset_values = WEAVIATE_DATA.get(dataset, {})
        core_values = dataset_values.get("core_values", {})
        return {core: list(values) for core, values in core_values.items()}

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
    if db_name == "weaviate":
        dataset_values = WEAVIATE_DATA.get(dataset, {})
        return list(dataset_values.get("unrestricted", []))

    values: list[float] = []

    for round_name in rounds:
        db_root = root / dataset / round_name / db_name
        if not db_root.exists():
            continue

        for run_dir in db_root.iterdir():
            if not run_dir.is_dir():
                continue
            if "CORES" in run_dir.name.upper():
                continue
            if _extract_point_count(run_dir.name, db_name) != POINT_COUNT:
                continue

            value = _read_index_time(run_dir / "index_time.txt")
            if value is not None:
                values.append(value)

    return values


def mean_and_std(values: list[float]) -> tuple[float, float] | tuple[None, None]:
    if not values:
        return None, None
    arr = np.asarray(values, dtype=float)
    return float(arr.mean()), float(arr.std(ddof=0))


def series_stats(
    root: Path, dataset: str, rounds: list[str], db_name: str, core_counts: list[int]
) -> tuple[list[float], list[float]]:
    if db_name == "weaviate":
        dataset_values = WEAVIATE_DATA.get(dataset, {})
        core_values = dataset_values.get("core_values", {})
        core_stds = dataset_values.get("core_stds", {})

        means: list[float] = []
        stds: list[float] = []
        for core_count in core_counts:
            values = core_values.get(core_count, [])
            if not values:
                means.append(np.nan)
                stds.append(0.0)
                continue
            means.append(values[0])
            stds.append(float(core_stds.get(core_count, 0.0)))
        return means, stds

    values_by_core = collect_core_values(root, dataset, rounds, db_name)

    means: list[float] = []
    stds: list[float] = []
    for core_count in core_counts:
        mean, std = mean_and_std(values_by_core.get(core_count, []))
        means.append(np.nan if mean is None else mean)
        stds.append(0.0 if std is None else std)
    return means, stds


def dataset_core_counts(root: Path, dataset: str, rounds: list[str], series_cfg: list[tuple[str, str]]) -> list[int]:
    core_counts: set[int] = set()
    for _, db_name in series_cfg:
        core_counts.update(collect_core_values(root, dataset, rounds, db_name))
    return sorted(core_count for core_count in core_counts if core_count not in EXCLUDED_CORE_COUNTS)


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
    bar_width = 0.24
    offsets = np.arange(len(series_cfg)) - (len(series_cfg) - 1) / 2
    offsets = offsets * bar_width

    all_tops: list[float] = []
    plotted_bars = []

    for i, (label, db_name) in enumerate(series_cfg):
        means, stds = series_stats(root, dataset, rounds, db_name, core_counts)
        if db_name == "weaviate":
            dataset_values = WEAVIATE_DATA.get(dataset, {})
            unrestricted_values = list(dataset_values.get("unrestricted", []))
            unrestricted_mean = unrestricted_values[0] if unrestricted_values else None
            unrestricted_stds = list(dataset_values.get("unrestricted_stds", []))
            unrestricted_std = unrestricted_stds[0] if unrestricted_stds else None
        else:
            unrestricted_mean, unrestricted_std = mean_and_std(
                collect_unrestricted_values(UNRESTRICTED_ROOT, dataset, rounds, db_name)
            )
        means.append(np.nan if unrestricted_mean is None else unrestricted_mean)
        stds.append(0.0 if unrestricted_std is None else unrestricted_std)

        bars = ax.bar(x + offsets[i], means, bar_width, yerr=stds, capsize=3, label=label)
        plotted_bars.append((bars, means, stds))

        for mean, std in zip(means, stds):
            if np.isfinite(mean):
                all_tops.append(mean + std)

    max_height = max(all_tops, default=0.0)
    offset = max(0.012 * max_height, 0.06) if max_height > 0 else 0.06

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
                rotation=0,
            )

    ax.set_title(title, fontsize=16, fontweight="bold")
    ax.set_xticks(x)
    ax.set_xticklabels(x_labels, fontsize=13)
    ax.tick_params(axis="y", labelsize=13)
    return max_height


def _format_stat(core_count: int, mean: float, std: float) -> str:
    if not np.isfinite(mean):
        return f"{core_count}: mean=n/a, std=n/a"
    return f"{core_count}: mean={mean:.3f}, std={std:.3f}"


def print_stats(root: Path, dataset: str, rounds: list[str], series_cfg: list[tuple[str, str]]):
    core_counts = dataset_core_counts(root, dataset, rounds, series_cfg)
    print(f"\n{dataset} | {SUPERCOMPUTER_NAME} | root={root} | rounds={rounds}")
    for label, db_name in series_cfg:
        means, stds = series_stats(root, dataset, rounds, db_name, core_counts)
        if db_name == "weaviate":
            dataset_values = WEAVIATE_DATA.get(dataset, {})
            unrestricted_values = list(dataset_values.get("unrestricted", []))
            unrestricted_mean = unrestricted_values[0] if unrestricted_values else None
            unrestricted_stds = list(dataset_values.get("unrestricted_stds", []))
            unrestricted_std = unrestricted_stds[0] if unrestricted_stds else None
        else:
            unrestricted_mean, unrestricted_std = mean_and_std(
                collect_unrestricted_values(UNRESTRICTED_ROOT, dataset, rounds, db_name)
            )
        stats = [_format_stat(core_counts[i], means[i], stds[i]) for i in range(len(core_counts))]
        if unrestricted_mean is None:
            stats.append("Unrestricted: mean=n/a, std=n/a")
        else:
            stats.append(f"Unrestricted: mean={unrestricted_mean:.3f}, std={unrestricted_std:.3f}")
        print(f"  {label:<7} " + " | ".join(stats))


def create_figure(output_path: str, annotation_fontsize: int):
    fig, axes = plt.subplots(2, 1, figsize=(12, 8), sharex=False)

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


create_figure(str(SCRIPT_DIR / "indexing_time_aurora_cores.pdf"), 9)
plt.show()
