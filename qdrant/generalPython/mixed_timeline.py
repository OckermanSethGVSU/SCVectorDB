import argparse
import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import numpy as np


@dataclass(frozen=True)
class InsertEvent:
    client_id: int
    op_index: int
    issued_at_ns: int
    completed_at_ns: int
    batch_start_row: int
    batch_end_row: int
    inserted_id_start: int
    inserted_id_end_exclusive: int
    status: str


@dataclass(frozen=True)
class QueryEvent:
    client_id: int
    op_index: int
    issued_at_ns: int
    completed_at_ns: int
    query_start_row: int
    query_end_row: int
    query_row_indices: List[int]
    result_ids: List[List[int]]
    result_scores: List[List[float]]
    status: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Merge mixed Milvus logs into a single timeline and compute recall under visibility assumptions."
    )
    parser.add_argument("--log-dir", required=True, help="Directory containing mixed runner JSONL logs")
    parser.add_argument("--insert-vectors", required=True, help="Insert vector matrix (.npy)")
    parser.add_argument("--query-vectors", required=True, help="Query vector matrix (.npy)")
    parser.add_argument("--init-vectors", default=None, help="Optional initial visible vector matrix (.npy)")
    parser.add_argument(
        "--metric",
        default="l2",
        choices=("dot", "cosine", "l2"),
        help="Distance/similarity metric for exact recall reconstruction",
    )
    parser.add_argument(
        "--visibility-lag-ms",
        type=float,
        nargs="*",
        default=[0.0],
        help="Insert visibility lag assumptions in milliseconds. 0 means instant visibility.",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=None,
        help="Override top-k. By default it is inferred from the query logs.",
    )
    parser.add_argument(
        "--timeline-out",
        default=None,
        help="Output path for merged timeline JSONL. Defaults to <log-dir>/global_timeline.jsonl",
    )
    parser.add_argument(
        "--recall-detail-out",
        default=None,
        help="Output path for per-query recall JSONL. Defaults to <log-dir>/recall_details.jsonl",
    )
    parser.add_argument(
        "--recall-summary-out",
        default=None,
        help="Output path for aggregated recall CSV. Defaults to <log-dir>/recall_summary.csv",
    )
    parser.add_argument(
        "--throughput-summary-out",
        default=None,
        help="Output path for throughput CSV. Defaults to <log-dir>/throughput_summary.csv",
    )
    parser.add_argument(
        "--init-id-offset",
        type=int,
        default=0,
        help="ID offset for optional init vectors. Defaults to 0.",
    )
    parser.add_argument(
        "--insert-id-offset",
        type=int,
        default=0,
        help="ID offset for insert vectors. Defaults to 0, matching the current mixed runner.",
    )
    return parser.parse_args()


def load_jsonl(path: Path) -> List[dict]:
    records: List[dict] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def load_events(log_dir: Path) -> Tuple[List[InsertEvent], List[QueryEvent]]:
    insert_events: List[InsertEvent] = []
    query_events: List[QueryEvent] = []

    for path in sorted(log_dir.glob("insert_client_*.jsonl")):
        for record in load_jsonl(path):
            insert_events.append(
                InsertEvent(
                    client_id=int(record["client_id"]),
                    op_index=int(record["op_index"]),
                    issued_at_ns=int(record["issued_at_ns"]),
                    completed_at_ns=int(record.get("completed_at_ns", record["issued_at_ns"])),
                    batch_start_row=int(record["batch_start_row"]),
                    batch_end_row=int(record["batch_end_row"]),
                    inserted_id_start=int(record["inserted_id_start"]),
                    inserted_id_end_exclusive=int(record["inserted_id_end_exclusive"]),
                    status=str(record["status"]),
                )
            )

    for path in sorted(log_dir.glob("query_client_*.jsonl")):
        for record in load_jsonl(path):
            query_events.append(
                QueryEvent(
                    client_id=int(record["client_id"]),
                    op_index=int(record["op_index"]),
                    issued_at_ns=int(record["issued_at_ns"]),
                    completed_at_ns=int(record.get("completed_at_ns", record["issued_at_ns"])),
                    query_start_row=int(record["query_start_row"]),
                    query_end_row=int(record["query_end_row"]),
                    query_row_indices=[int(v) for v in record["query_row_indices"]],
                    result_ids=[[int(v) for v in row] for row in record["result_ids"]],
                    result_scores=[[float(v) for v in row] for row in record["result_scores"]],
                    status=str(record["status"]),
                )
            )

    insert_events.sort(key=lambda event: (event.completed_at_ns, 0, event.client_id, event.op_index))
    query_events.sort(key=lambda event: (event.completed_at_ns, 1, event.client_id, event.op_index))
    return insert_events, query_events


def build_timeline(insert_events: List[InsertEvent], query_events: List[QueryEvent]) -> List[dict]:
    combined: List[dict] = []
    for event in insert_events:
        combined.append(
            {
                "issued_at_ns": event.issued_at_ns,
                "completed_at_ns": event.completed_at_ns,
                "op_type": "insert",
                "client_id": event.client_id,
                "op_index": event.op_index,
                "batch_start_row": event.batch_start_row,
                "batch_end_row": event.batch_end_row,
                "inserted_id_start": event.inserted_id_start,
                "inserted_id_end_exclusive": event.inserted_id_end_exclusive,
                "status": event.status,
            }
        )
    for event in query_events:
        combined.append(
            {
                "issued_at_ns": event.issued_at_ns,
                "completed_at_ns": event.completed_at_ns,
                "op_type": "query",
                "client_id": event.client_id,
                "op_index": event.op_index,
                "query_start_row": event.query_start_row,
                "query_end_row": event.query_end_row,
                "query_row_indices": event.query_row_indices,
                "result_ids": event.result_ids,
                "status": event.status,
            }
        )

    combined.sort(key=lambda record: (record["completed_at_ns"], 0 if record["op_type"] == "insert" else 1, record["client_id"], record["op_index"]))
    for sequence_id, record in enumerate(combined):
        record["sequence_id"] = sequence_id
    return combined


def compute_observed_rates(insert_events: List[InsertEvent], query_events: List[QueryEvent]) -> dict:
    timestamps: List[int] = []
    total_insert_ops = 0
    total_insert_vectors = 0
    total_query_ops = 0
    total_query_vectors = 0

    for event in insert_events:
        if event.status != "ok":
            continue
        timestamps.append(event.completed_at_ns)
        total_insert_ops += 1
        total_insert_vectors += event.batch_end_row - event.batch_start_row

    for event in query_events:
        if event.status != "ok":
            continue
        timestamps.append(event.completed_at_ns)
        total_query_ops += 1
        total_query_vectors += event.query_end_row - event.query_start_row

    if not timestamps:
        duration_s = 0.0
    else:
        min_ts = min(timestamps)
        max_ts = max(timestamps)
        duration_s = max((max_ts - min_ts) / 1_000_000_000.0, 0.0)

    if duration_s <= 0:
        query_ops_per_sec = 0.0
        query_vectors_per_sec = 0.0
        insert_ops_per_sec = 0.0
        insert_vectors_per_sec = 0.0
    else:
        query_ops_per_sec = total_query_ops / duration_s
        query_vectors_per_sec = total_query_vectors / duration_s
        insert_ops_per_sec = total_insert_ops / duration_s
        insert_vectors_per_sec = total_insert_vectors / duration_s

    return {
        "observed_duration_s": duration_s,
        "total_query_ops": total_query_ops,
        "total_query_vectors": total_query_vectors,
        "total_insert_ops": total_insert_ops,
        "total_insert_vectors": total_insert_vectors,
        "achieved_query_ops_per_sec": query_ops_per_sec,
        "achieved_query_vectors_per_sec": query_vectors_per_sec,
        "achieved_insert_ops_per_sec": insert_ops_per_sec,
        "achieved_insert_vectors_per_sec": insert_vectors_per_sec,
    }


def _compute_rate_row(
    scope: str,
    role_name: str,
    client_id: str,
    timestamps: List[int],
    op_count: int,
    vector_count: int,
) -> dict:
    if timestamps:
        duration_s = max((max(timestamps) - min(timestamps)) / 1_000_000_000.0, 0.0)
    else:
        duration_s = 0.0

    if duration_s <= 0.0:
        ops_per_sec = 0.0
        vectors_per_sec = 0.0
    else:
        ops_per_sec = op_count / duration_s
        vectors_per_sec = vector_count / duration_s

    return {
        "scope": scope,
        "role": role_name,
        "client_id": client_id,
        "observed_duration_s": duration_s,
        "op_count": op_count,
        "vector_count": vector_count,
        "ops_per_sec": ops_per_sec,
        "vectors_per_sec": vectors_per_sec,
    }


def compute_throughput_rows(insert_events: List[InsertEvent], query_events: List[QueryEvent]) -> List[dict]:
    rows: List[dict] = []

    insert_ok = [event for event in insert_events if event.status == "ok"]
    query_ok = [event for event in query_events if event.status == "ok"]

    rows.append(
        _compute_rate_row(
            "aggregate",
            "insert",
            "all",
            [event.completed_at_ns for event in insert_ok],
            len(insert_ok),
            sum(event.batch_end_row - event.batch_start_row for event in insert_ok),
        )
    )
    rows.append(
        _compute_rate_row(
            "aggregate",
            "query",
            "all",
            [event.completed_at_ns for event in query_ok],
            len(query_ok),
            sum(event.query_end_row - event.query_start_row for event in query_ok),
        )
    )

    insert_by_client: Dict[int, List[InsertEvent]] = {}
    for event in insert_ok:
        insert_by_client.setdefault(event.client_id, []).append(event)
    for client_id, events in sorted(insert_by_client.items()):
        rows.append(
            _compute_rate_row(
                "per_client",
                "insert",
                str(client_id),
                [event.completed_at_ns for event in events],
                len(events),
                sum(event.batch_end_row - event.batch_start_row for event in events),
            )
        )

    query_by_client: Dict[int, List[QueryEvent]] = {}
    for event in query_ok:
        query_by_client.setdefault(event.client_id, []).append(event)
    for client_id, events in sorted(query_by_client.items()):
        rows.append(
            _compute_rate_row(
                "per_client",
                "query",
                str(client_id),
                [event.completed_at_ns for event in events],
                len(events),
                sum(event.query_end_row - event.query_start_row for event in events),
            )
        )

    return rows


def write_jsonl(path: Path, records: Iterable[dict]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record))
            handle.write("\n")


def normalize_rows(matrix: np.ndarray) -> np.ndarray:
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    norms = np.where(norms == 0, 1.0, norms)
    return matrix / norms


def score_vectors(metric: str, queries: np.ndarray, candidates: np.ndarray) -> np.ndarray:
    if metric == "dot":
        return queries @ candidates.T
    if metric == "cosine":
        return normalize_rows(queries) @ normalize_rows(candidates).T
    if metric == "l2":
        diff = queries[:, None, :] - candidates[None, :, :]
        return np.sum(diff * diff, axis=2)
    raise ValueError(f"unsupported metric {metric}")


def topk_indices(metric: str, scores: np.ndarray, k: int) -> np.ndarray:
    if scores.shape[1] == 0:
        return np.empty((scores.shape[0], 0), dtype=np.int64)

    k = min(k, scores.shape[1])
    if metric == "l2":
        part = np.argpartition(scores, kth=k - 1, axis=1)[:, :k]
        part_scores = np.take_along_axis(scores, part, axis=1)
        order = np.argsort(part_scores, axis=1)
        return np.take_along_axis(part, order, axis=1)

    part = np.argpartition(-scores, kth=k - 1, axis=1)[:, :k]
    part_scores = np.take_along_axis(scores, part, axis=1)
    order = np.argsort(-part_scores, axis=1)
    return np.take_along_axis(part, order, axis=1)


def infer_topk(query_events: List[QueryEvent]) -> int:
    for event in query_events:
        if event.result_ids:
            return len(event.result_ids[0])
    raise ValueError("could not infer top-k from query logs")


def build_visible_insert_rows(insert_events: List[InsertEvent], query_time_ns: int, lag_ns: int) -> np.ndarray:
    visible_rows: List[int] = []
    cutoff = query_time_ns - lag_ns
    for event in insert_events:
        if event.status != "ok":
            continue
        if event.completed_at_ns > cutoff:
            break
        visible_rows.extend(range(event.batch_start_row, event.batch_end_row))
    if not visible_rows:
        return np.empty((0,), dtype=np.int64)
    return np.asarray(visible_rows, dtype=np.int64)


def compute_recall_details(
    insert_events: List[InsertEvent],
    query_events: List[QueryEvent],
    insert_vectors: np.ndarray,
    query_vectors: np.ndarray,
    init_vectors: np.ndarray | None,
    metric: str,
    top_k: int,
    visibility_lags_ms: List[float],
    init_id_offset: int,
    insert_id_offset: int,
) -> List[dict]:
    lag_specs = [(lag_ms, int(round(lag_ms * 1_000_000.0))) for lag_ms in sorted(set(visibility_lags_ms))]

    init_ids = np.arange(init_id_offset, init_id_offset + (0 if init_vectors is None else init_vectors.shape[0]), dtype=np.int64)
    insert_ids = np.arange(insert_id_offset, insert_id_offset + insert_vectors.shape[0], dtype=np.int64)

    details: List[dict] = []
    for event in query_events:
        if event.status != "ok":
            continue

        query_batch = query_vectors[event.query_start_row:event.query_end_row]
        actual_ids = [list(row) for row in event.result_ids]
        actual_scores = [list(row) for row in event.result_scores]

        record: dict = {
            "issued_at_ns": event.issued_at_ns,
            "completed_at_ns": event.completed_at_ns,
            "client_id": event.client_id,
            "op_index": event.op_index,
            "query_start_row": event.query_start_row,
            "query_end_row": event.query_end_row,
            "queries": [],
        }

        visible_rows_cache: Dict[int, np.ndarray] = {}
        exact_cache: Dict[int, Tuple[List[List[int]], List[List[float]]]] = {}
        for lag_ms, lag_ns in lag_specs:
            visible_insert_rows = build_visible_insert_rows(insert_events, event.completed_at_ns, lag_ns)
            visible_rows_cache[lag_ns] = visible_insert_rows

            candidate_blocks = []
            candidate_ids_blocks = []
            if init_vectors is not None and init_vectors.shape[0] > 0:
                candidate_blocks.append(init_vectors)
                candidate_ids_blocks.append(init_ids)
            if visible_insert_rows.size > 0:
                candidate_blocks.append(insert_vectors[visible_insert_rows])
                candidate_ids_blocks.append(insert_ids[visible_insert_rows])

            if candidate_blocks:
                candidate_vectors = np.vstack(candidate_blocks)
                candidate_ids = np.concatenate(candidate_ids_blocks)
                scores = score_vectors(metric, query_batch, candidate_vectors)
                topk = topk_indices(metric, scores, top_k)
                exact_ids = [[int(candidate_ids[idx]) for idx in row] for row in topk]
                exact_scores = [[float(scores[q_idx, idx]) for idx in row] for q_idx, row in enumerate(topk)]
            else:
                exact_ids = [[] for _ in range(query_batch.shape[0])]
                exact_scores = [[] for _ in range(query_batch.shape[0])]

            exact_cache[lag_ns] = (exact_ids, exact_scores)

        for local_idx, query_row in enumerate(event.query_row_indices):
            query_entry = {
                "query_row_index": query_row,
                "actual_result_ids": actual_ids[local_idx],
                "actual_result_scores": actual_scores[local_idx],
                "assumptions": {},
            }
            actual_set = set(actual_ids[local_idx][:top_k])
            for lag_ms, lag_ns in lag_specs:
                exact_ids, exact_scores = exact_cache[lag_ns]
                expected = exact_ids[local_idx]
                overlap = len(actual_set.intersection(expected[:top_k]))
                denom = min(top_k, len(expected)) if expected else top_k
                recall = float(overlap / denom) if denom > 0 else 1.0
                assumption_name = f"lag_ms_{lag_ms:g}"
                query_entry["assumptions"][assumption_name] = {
                    "visible_insert_count": int(visible_rows_cache[lag_ns].size),
                    "expected_result_ids": expected,
                    "expected_result_scores": exact_scores[local_idx],
                    "recall_at_k": recall,
                    "overlap_count": overlap,
                }
            record["queries"].append(query_entry)
        details.append(record)
    return details


def write_recall_summary(path: Path, details: List[dict], observed_rates: dict) -> None:
    assumption_totals: Dict[str, List[float]] = {}
    query_count = 0
    for event in details:
        for query in event["queries"]:
            query_count += 1
            for assumption_name, payload in query["assumptions"].items():
                assumption_totals.setdefault(assumption_name, []).append(payload["recall_at_k"])

    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow([
            "assumption",
            "query_count",
            "mean_recall_at_k",
            "min_recall_at_k",
            "max_recall_at_k",
            "observed_duration_s",
            "total_query_ops",
            "total_query_vectors",
            "total_insert_ops",
            "total_insert_vectors",
            "achieved_query_ops_per_sec",
            "achieved_query_vectors_per_sec",
            "achieved_insert_ops_per_sec",
            "achieved_insert_vectors_per_sec",
        ])
        for assumption_name, recalls in sorted(assumption_totals.items()):
            writer.writerow([
                assumption_name,
                query_count,
                float(np.mean(recalls)) if recalls else 0.0,
                float(np.min(recalls)) if recalls else 0.0,
                float(np.max(recalls)) if recalls else 0.0,
                observed_rates["observed_duration_s"],
                observed_rates["total_query_ops"],
                observed_rates["total_query_vectors"],
                observed_rates["total_insert_ops"],
                observed_rates["total_insert_vectors"],
                observed_rates["achieved_query_ops_per_sec"],
                observed_rates["achieved_query_vectors_per_sec"],
                observed_rates["achieved_insert_ops_per_sec"],
                observed_rates["achieved_insert_vectors_per_sec"],
            ])


def write_throughput_summary(path: Path, throughput_rows: List[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow([
            "scope",
            "role",
            "client_id",
            "observed_duration_s",
            "op_count",
            "vector_count",
            "ops_per_sec",
            "vectors_per_sec",
        ])
        for row in throughput_rows:
            writer.writerow([
                row["scope"],
                row["role"],
                row["client_id"],
                row["observed_duration_s"],
                row["op_count"],
                row["vector_count"],
                row["ops_per_sec"],
                row["vectors_per_sec"],
            ])


def main() -> None:
    args = parse_args()

    log_dir = Path(args.log_dir).expanduser().resolve()
    timeline_out = Path(args.timeline_out).expanduser().resolve() if args.timeline_out else log_dir / "global_timeline.jsonl"
    recall_detail_out = Path(args.recall_detail_out).expanduser().resolve() if args.recall_detail_out else log_dir / "recall_details.jsonl"
    recall_summary_out = Path(args.recall_summary_out).expanduser().resolve() if args.recall_summary_out else log_dir / "recall_summary.csv"
    throughput_summary_out = Path(args.throughput_summary_out).expanduser().resolve() if args.throughput_summary_out else log_dir / "throughput_summary.csv"

    insert_events, query_events = load_events(log_dir)
    if not insert_events and not query_events:
        raise SystemExit(f"no mixed runner logs found in {log_dir}")

    insert_vectors = np.asarray(np.load(Path(args.insert_vectors).expanduser()), dtype=np.float32)
    query_vectors = np.asarray(np.load(Path(args.query_vectors).expanduser()), dtype=np.float32)
    init_vectors = None
    if args.init_vectors:
        init_vectors = np.asarray(np.load(Path(args.init_vectors).expanduser()), dtype=np.float32)

    if insert_vectors.ndim != 2 or query_vectors.ndim != 2:
        raise SystemExit("insert-vectors and query-vectors must be 2D matrices")
    if init_vectors is not None and init_vectors.ndim != 2:
        raise SystemExit("init-vectors must be a 2D matrix")

    top_k = args.top_k if args.top_k is not None else infer_topk(query_events)

    timeline = build_timeline(insert_events, query_events)
    write_jsonl(timeline_out, timeline)
    observed_rates = compute_observed_rates(insert_events, query_events)
    throughput_rows = compute_throughput_rows(insert_events, query_events)

    details = compute_recall_details(
        insert_events=insert_events,
        query_events=query_events,
        insert_vectors=insert_vectors,
        query_vectors=query_vectors,
        init_vectors=init_vectors,
        metric=args.metric,
        top_k=top_k,
        visibility_lags_ms=args.visibility_lag_ms,
        init_id_offset=args.init_id_offset,
        insert_id_offset=args.insert_id_offset,
    )
    write_jsonl(recall_detail_out, details)
    write_recall_summary(recall_summary_out, details, observed_rates)
    write_throughput_summary(throughput_summary_out, throughput_rows)

    print(f"Wrote timeline to {timeline_out}")
    print(f"Wrote recall details to {recall_detail_out}")
    print(f"Wrote recall summary to {recall_summary_out}")
    print(f"Wrote throughput summary to {throughput_summary_out}")


if __name__ == "__main__":
    main()
