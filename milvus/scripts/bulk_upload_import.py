import argparse
import json
import logging
import math
import multiprocessing as mp
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
from pymilvus import CollectionSchema, DataType, FieldSchema, MilvusClient
from pymilvus.bulk_writer import (
    BulkFileType,
    LocalBulkWriter,
    RemoteBulkWriter,
    bulk_import,
    get_import_progress,
)


def read_ip_from_file(path: str) -> str:
    with open(path, "r", encoding="utf-8") as handle:
        return handle.read().strip()


def read_registry_host_port(path: str) -> tuple[str, int]:
    with open(path, "r", encoding="utf-8") as handle:
        line = handle.readline().strip()
    rank, host, port = line.split(",")
    _ = rank
    return host, int(port)


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    return int(value) if value not in (None, "") else default


def env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    return float(value) if value not in (None, "") else default


def first_non_empty(*values: str | None) -> str | None:
    for value in values:
        if value not in (None, ""):
            return value
    return None


def env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value in (None, ""):
        return default
    return value.strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def load_matrix(path: str, use_mmap: bool) -> np.ndarray:
    if use_mmap:
        return np.load(path, mmap_mode="r")
    return np.load(path)


def split_range(total: int, part_count: int, part_index: int) -> tuple[int, int]:
    base, remainder = divmod(total, part_count)
    start = part_index * base + min(part_index, remainder)
    stop = start + base + (1 if part_index < remainder else 0)
    return start, stop


def build_schema(id_field: str, vector_field: str, vector_dim: int) -> CollectionSchema:
    fields = [
        FieldSchema(name=id_field, dtype=DataType.INT64, is_primary=True, auto_id=False),
        FieldSchema(name=vector_field, dtype=DataType.FLOAT_VECTOR, dim=vector_dim),
    ]
    return CollectionSchema(fields=fields)


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    ensure_parent(path)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def extract_job_id(response_json: dict[str, Any]) -> str:
    data = response_json.get("data", {})
    job_id = data.get("jobId")
    if job_id in (None, ""):
        raise RuntimeError(f"bulk_import response did not include jobId: {response_json}")
    return str(job_id)


def normalize_progress(progress: Any) -> float | None:
    if progress is None:
        return None
    value = float(progress)
    if value > 1.0:
        value /= 100.0
    return max(0.0, min(1.0, value))


def format_progress_bar(progress: float | None, width: int = 32) -> str:
    if progress is None:
        return "[" + ("?" * width) + "]"
    clamped = max(0.0, min(1.0, progress))
    filled = int(round(clamped * width))
    return "[" + ("#" * filled) + ("-" * (width - filled)) + "]"


def print_progress_line(label: str, progress: float | None, detail: str = "") -> None:
    progress_text = "n/a" if progress is None else f"{progress * 100.0:6.2f}%"
    suffix = f" {detail}" if detail else ""
    print(f"\r{label} {format_progress_bar(progress)} {progress_text}{suffix}", end="", flush=True)


def finish_progress_line() -> None:
    print("", flush=True)


def configure_logging(verbose: bool) -> None:
    level = logging.INFO if verbose else logging.WARNING
    for logger_name in (
        "pymilvus",
        "pymilvus.bulk_writer",
        "pymilvus.bulk_writer.buffer",
        "pymilvus.bulk_writer.local_bulk_writer",
        "pymilvus.bulk_writer.remote_bulk_writer",
        "pymilvus.bulk_writer.bulk_import",
        "pymilvus.bulk_writer.volume_file_manager",
        "pymilvus.bulk_writer.volume_manager",
        "EndpointResolver",
        "bulk_writer",
    ):
        logger = logging.getLogger(logger_name)
        logger.setLevel(level)
        if not verbose:
            logger.propagate = False


def worker_write(
    worker_index: int,
    process_count: int,
    input_path: str,
    row_count: int,
    row_id_start: int,
    batch_row_count: int,
    stage_dir: str,
    chunk_size_bytes: int,
    file_type_name: str,
    writer_mode: str,
    id_field: str,
    vector_field: str,
    vector_dim: int,
    progress_every_rows: int,
    remote_endpoint: str | None,
    remote_bucket: str,
    remote_access_key: str | None,
    remote_secret_key: str | None,
    remote_secure: bool,
    remote_path: str,
    use_mmap: bool,
    shared_progress: Any,
    shared_results: Any,
) -> None:
    started_at = time.perf_counter()
    file_type = getattr(BulkFileType, file_type_name)
    start_row, stop_row = split_range(row_count, process_count, worker_index)

    if start_row >= stop_row:
        shared_results.append(
            {
                "worker_index": worker_index,
                "start_row": start_row,
                "stop_row": stop_row,
                "rows": 0,
                "batch_files": [],
                "write_seconds": 0.0,
                "status": "ok",
            }
        )
        shared_progress[worker_index] = 0
        return

    data = load_matrix(input_path, use_mmap=use_mmap)
    schema = build_schema(id_field=id_field, vector_field=vector_field, vector_dim=vector_dim)
    if writer_mode == "local":
        writer_root = Path(stage_dir) / f"proc_{worker_index:03d}"
        writer = LocalBulkWriter(
            schema=schema,
            local_path=str(writer_root),
            chunk_size=chunk_size_bytes,
            file_type=file_type,
        )
    else:
        connect_param = RemoteBulkWriter.S3ConnectParam(
            endpoint=remote_endpoint,
            access_key=remote_access_key,
            secret_key=remote_secret_key,
            bucket_name=remote_bucket,
            secure=remote_secure,
        )
        writer = RemoteBulkWriter(
            schema=schema,
            remote_path=str(Path(remote_path) / f"proc_{worker_index:03d}"),
            connect_param=connect_param,
            chunk_size=chunk_size_bytes,
            file_type=file_type,
        )

    rows_written = 0
    shared_progress[worker_index] = 0
    try:
        for batch_start in range(start_row, stop_row, batch_row_count):
            batch_stop = min(batch_start + batch_row_count, stop_row)
            for row_index in range(batch_start, batch_stop):
                writer.append_row(
                    {
                        id_field: row_id_start + row_index,
                        vector_field: np.asarray(data[row_index], dtype=np.float32).tolist(),
                    }
                )
            writer.commit()
            rows_written += batch_stop - batch_start
            shared_progress[worker_index] = rows_written
    except Exception as exc:
        shared_results.append(
            {
                "worker_index": worker_index,
                "start_row": start_row,
                "stop_row": stop_row,
                "rows": rows_written,
                "batch_files": writer.batch_files,
                "write_seconds": time.perf_counter() - started_at,
                "status": "error",
                "error": repr(exc),
            }
        )
        raise

    shared_progress[worker_index] = rows_written
    shared_results.append(
        {
            "worker_index": worker_index,
            "start_row": start_row,
            "stop_row": stop_row,
            "rows": rows_written,
            "batch_files": writer.batch_files,
            "write_seconds": time.perf_counter() - started_at,
            "status": "ok",
        }
    )


def parse_args() -> argparse.Namespace:
    default_input = first_non_empty(
        os.getenv("INSERT_DATA_FILEPATH"),
        os.getenv("DATA_FILEPATH"),
    )
    parser = argparse.ArgumentParser(
        description="Split a single NumPy matrix across processes, create bulk-import files, and launch Milvus bulk_import.",
    )
    parser.add_argument("--input", default=default_input, help="Path to the source .npy matrix.")
    parser.add_argument(
        "--writer-mode",
        default=os.getenv("BULK_WRITER_MODE", "local"),
        choices=["local", "remote"],
        help="Use LocalBulkWriter or RemoteBulkWriter.",
    )
    parser.add_argument(
        "--processes",
        type=int,
        default=env_int("BULK_UPLOAD_PROCESSES", env_int("INSERT_CLIENTS", 1)),
        help="Number of parallel writer processes.",
    )
    parser.add_argument(
        "--corpus-size",
        type=int,
        default=env_int("INSERT_CORPUS_SIZE", env_int("CORPUS_SIZE", 0)),
        help="Rows to read from the input matrix; 0 means all rows.",
    )
    parser.add_argument(
        "--row-id-start",
        type=int,
        default=env_int("INSERT_START_ID", 0),
        help="Base INT64 ID assigned to the first row.",
    )
    parser.add_argument(
        "--collection",
        default=os.getenv("COLLECTION_NAME", "standalone"),
        help="Target Milvus collection.",
    )
    parser.add_argument(
        "--db-name",
        default=os.getenv("DB_NAME", "default"),
        help="Target Milvus database name.",
    )
    parser.add_argument(
        "--vector-field",
        default=os.getenv("VECTOR_FIELD", "vector"),
        help="Vector field name.",
    )
    parser.add_argument(
        "--id-field",
        default=os.getenv("ID_FIELD", "id"),
        help="Primary key field name.",
    )
    parser.add_argument(
        "--vector-dim",
        type=int,
        default=env_int("VECTOR_DIM", 0),
        help="Vector dimension; 0 infers it from the input matrix.",
    )
    parser.add_argument(
        "--stage-dir",
        default=os.getenv("BULK_IMPORT_STAGE_DIR", "/var/lib/milvus/data/bulk-import"),
        help="Directory visible to Milvus local storage for generated bulk files when --writer-mode=local.",
    )
    parser.add_argument(
        "--batch-rows",
        type=int,
        default=env_int("INSERT_BATCH_SIZE", 1024),
        help="Rows appended before each LocalBulkWriter commit().",
    )
    parser.add_argument(
        "--chunk-size-mb",
        type=float,
        default=env_float("BULK_WRITER_CHUNK_SIZE_MB", 128.0),
        help="Target bulk-writer chunk size in MiB.",
    )
    parser.add_argument(
        "--file-type",
        default=os.getenv("BULK_WRITER_FILE_TYPE", "PARQUET"),
        choices=["JSON", "CSV", "NUMPY", "PARQUET"],
        help="Bulk file type written by LocalBulkWriter.",
    )
    parser.add_argument(
        "--milvus-url",
        default=os.getenv("MILVUS_URL"),
        help="Milvus REST base URL. Defaults to http://MILVUS_HOST:MILVUS_GRPC_PORT.",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=env_float("BULK_IMPORT_POLL_INTERVAL", 5.0),
        help="Seconds between import-progress polls.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=env_float("BULK_IMPORT_TIMEOUT_SECONDS", 0.0),
        help="Abort polling after this many seconds; 0 disables the timeout.",
    )
    parser.add_argument(
        "--summary-path",
        default=os.getenv("BULK_IMPORT_SUMMARY_PATH", "bulk_upload_import_summary.json"),
        help="Where to write the end-to-end timing and import summary JSON.",
    )
    parser.add_argument(
        "--import-request-path",
        default=os.getenv("BULK_IMPORT_REQUEST_PATH", "bulk_import_request.json"),
        help="Where to write the reusable bulk-import request JSON after bulk files are generated.",
    )
    parser.add_argument(
        "--load-import-request",
        default=os.getenv("BULK_IMPORT_LOAD_REQUEST"),
        help="Load a previously generated bulk-import request JSON and start the import job without regenerating files.",
    )
    parser.add_argument(
        "--prepare-only",
        action="store_true",
        default=env_flag("BULK_IMPORT_PREPARE_ONLY", default=False),
        help="Generate bulk files and the reusable import request JSON, but do not start bulk_import.",
    )
    parser.add_argument(
        "--progress-every-rows",
        type=int,
        default=env_int("BULK_UPLOAD_PROGRESS_EVERY_ROWS", 50000),
        help="Deprecated per-process progress interval; retained for compatibility.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        default=env_flag("BULK_IMPORT_VERBOSE", default=False),
        help="Print verbose status logs and the full JSON summary to stdout.",
    )
    parser.add_argument(
        "--remote-endpoint",
        default=os.getenv("MINIO_ENDPOINT"),
        help="S3/MinIO endpoint for --writer-mode=remote. Defaults to MINIO_ENDPOINT or MINIO_ADDRESS.",
    )
    parser.add_argument(
        "--remote-bucket",
        default=os.getenv("MINIO_BUCKET_NAME", "a-bucket"),
        help="Bucket name for --writer-mode=remote.",
    )
    parser.add_argument(
        "--remote-access-key",
        default=first_non_empty(os.getenv("MINIO_ACCESS_KEY_ID"), os.getenv("AWS_ACCESS_KEY_ID"), "minioadmin"),
        help="Access key for --writer-mode=remote.",
    )
    parser.add_argument(
        "--remote-secret-key",
        default=first_non_empty(
            os.getenv("MINIO_SECRET_ACCESS_KEY"),
            os.getenv("AWS_SECRET_ACCESS_KEY"),
            "minioadmin",
        ),
        help="Secret key for --writer-mode=remote.",
    )
    parser.add_argument(
        "--remote-secure",
        action="store_true",
        default=os.getenv("MINIO_USE_SSL", "false").strip().lower() == "true",
        help="Use TLS for --writer-mode=remote.",
    )
    parser.add_argument(
        "--remote-path",
        default=os.getenv("BULK_REMOTE_PATH", "bulk-import"),
        help="Object prefix for --writer-mode=remote.",
    )
    return parser.parse_args()


def resolve_milvus_url(explicit_url: str | None) -> str:
    if explicit_url:
        return explicit_url
    host = os.getenv("MILVUS_HOST")
    worker_ip_path = Path(os.getenv("RUNTIME_STATE_DIR", "./runtime_state")) / "worker.ip"
    if not host and worker_ip_path.exists():
        host = read_ip_from_file(str(worker_ip_path))
    if not host:
        host = "127.0.0.1"
    port = env_int("MILVUS_GRPC_PORT", 19530)
    return f"http://{host}:{port}"


def resolve_remote_endpoint(explicit_endpoint: str | None) -> str:
    endpoint = explicit_endpoint
    if not endpoint:
        endpoint = os.getenv("MINIO_ADDRESS")
    runtime_state_dir = Path(os.getenv("RUNTIME_STATE_DIR", "./runtime_state"))
    mode = os.getenv("MODE", "standalone").strip().lower()
    if mode == "distributed":
        minio_registry_path = Path("minioFiles/minio_registry.txt")
    else:
        minio_registry_path = runtime_state_dir / "minio_registry.txt"
    if not endpoint and minio_registry_path.exists():
        host, port = read_registry_host_port(str(minio_registry_path))
        endpoint = f"{host}:{port}"
    if not endpoint:
        endpoint = "127.0.0.1:9000"
    return endpoint


def maybe_check_collection(url: str, token: str, collection_name: str) -> None:
    try:
        client = MilvusClient(uri=url, token=token)
        if not client.has_collection(collection_name):
            raise RuntimeError(f"Collection '{collection_name}' does not exist at {url}")
    except Exception as exc:
        print(f"Collection preflight check skipped or failed: {exc}", flush=True)


def flatten_batch_files(results: list[dict[str, Any]]) -> list[list[str]]:
    batch_files: list[list[str]] = []
    for result in results:
        batch_files.extend(result["batch_files"])
    return batch_files


def build_import_request(
    *,
    input_path: Path,
    milvus_url: str,
    remote_endpoint: str | None,
    args: argparse.Namespace,
    row_count: int,
    vector_dim: int,
    batch_files: list[list[str]],
) -> dict[str, Any]:
    return {
        "request_type": "milvus_bulk_import",
        "request_version": 1,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "input_path": str(input_path),
        "collection": args.collection,
        "db_name": args.db_name,
        "milvus_url": milvus_url,
        "stage_dir": str(Path(args.stage_dir).expanduser().resolve()),
        "writer_mode": args.writer_mode,
        "row_count": row_count,
        "vector_dim": vector_dim,
        "row_id_start": args.row_id_start,
        "processes": args.processes,
        "batch_rows": args.batch_rows,
        "chunk_size_mb": args.chunk_size_mb,
        "file_type": args.file_type,
        "generated_batch_file_groups": len(batch_files),
        "generated_batch_files": batch_files,
        "remote_endpoint": remote_endpoint,
        "remote_bucket": args.remote_bucket if args.writer_mode == "remote" else None,
        "remote_path": args.remote_path if args.writer_mode == "remote" else None,
    }


def load_import_request(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("request_type") != "milvus_bulk_import":
        raise ValueError(f"Unsupported import request type in {path}: {payload.get('request_type')}")
    batch_files = payload.get("generated_batch_files")
    if not isinstance(batch_files, list) or not batch_files:
        raise ValueError(f"Import request {path} does not contain generated_batch_files")
    return payload


def poll_import_until_done(
    milvus_url: str,
    job_id: str,
    token: str,
    poll_interval: float,
    timeout_seconds: float,
    verbose: bool,
) -> dict[str, Any]:
    poll_started_at = time.perf_counter()
    last_payload: dict[str, Any] | None = None

    while True:
        response = get_import_progress(url=milvus_url, job_id=job_id, api_key=token)
        payload = response.json()
        last_payload = payload
        data = payload.get("data", {})
        state = str(data.get("state", "")).lower()
        progress = normalize_progress(data.get("progress"))
        imported_rows = data.get("importedRows", data.get("importRows"))
        total_rows = data.get("totalRows")

        if verbose:
            progress_text = "n/a" if progress is None else f"{progress * 100.0:.1f}%"
            print(
                f"[import job {job_id}] state={data.get('state')} progress={progress_text} "
                f"imported_rows={imported_rows} total_rows={total_rows}",
                flush=True,
            )
        else:
            detail = f"state={data.get('state')} rows={imported_rows}/{total_rows}"
            print_progress_line("Importing", progress, detail)

        if state in {"completed", "importcompleted"}:
            if not verbose:
                finish_progress_line()
            return payload
        if state in {"failed", "importfailed"}:
            if not verbose:
                finish_progress_line()
            reason = data.get("reason", "unknown failure")
            raise RuntimeError(f"bulk_import job {job_id} failed: {reason}")

        if timeout_seconds > 0 and (time.perf_counter() - poll_started_at) > timeout_seconds:
            if not verbose:
                finish_progress_line()
            raise TimeoutError(f"Timed out waiting for bulk_import job {job_id}: {last_payload}")

        time.sleep(poll_interval)


def start_import_from_request(
    *,
    request_payload: dict[str, Any],
    milvus_url: str,
    token: str,
    poll_interval: float,
    timeout_seconds: float,
    verbose: bool,
) -> tuple[str, dict[str, Any], dict[str, Any], float]:
    import_started_at = time.perf_counter()
    import_response = bulk_import(
        url=milvus_url,
        collection_name=str(request_payload["collection"]),
        db_name=str(request_payload["db_name"]),
        files=request_payload["generated_batch_files"],
        api_key=token,
    )
    import_payload = import_response.json()
    job_id = extract_job_id(import_payload)
    print(f"Started bulk_import job {job_id}", flush=True)

    final_progress_payload = poll_import_until_done(
        milvus_url=milvus_url,
        job_id=job_id,
        token=token,
        poll_interval=poll_interval,
        timeout_seconds=timeout_seconds,
        verbose=verbose,
    )
    return job_id, import_payload, final_progress_payload, time.perf_counter() - import_started_at


def monitor_writer_progress(
    *,
    processes: list[mp.Process],
    shared_progress: Any,
    row_count: int,
    verbose: bool,
) -> None:
    if verbose:
        for process in processes:
            process.join()
        return

    while True:
        total_written = sum(int(shared_progress.get(worker_index, 0)) for worker_index in range(len(processes)))
        progress = 1.0 if row_count <= 0 else min(1.0, total_written / row_count)
        print_progress_line("Writing  ", progress, f"rows={total_written}/{row_count}")
        if all(not process.is_alive() for process in processes):
            finish_progress_line()
            break
        time.sleep(0.5)

    for process in processes:
        process.join()


def main() -> None:
    args = parse_args()
    configure_logging(args.verbose)
    if args.processes <= 0:
        raise ValueError("--processes must be positive")
    if args.batch_rows <= 0:
        raise ValueError("--batch-rows must be positive")

    summary_path = Path(args.summary_path).expanduser().resolve()
    request_path = Path(args.import_request_path).expanduser().resolve()
    ensure_parent(summary_path)
    ensure_parent(request_path)

    milvus_url = resolve_milvus_url(args.milvus_url)
    token = os.getenv("MILVUS_TOKEN", "root:Milvus")
    overall_started_at = time.perf_counter()
    started_at_utc = datetime.now(timezone.utc).isoformat()

    if args.load_import_request:
        loaded_request_path = Path(args.load_import_request).expanduser().resolve()
        request_payload = load_import_request(loaded_request_path)
        maybe_check_collection(milvus_url, token, str(request_payload["collection"]))
        job_id, import_payload, final_progress_payload, import_seconds = start_import_from_request(
            request_payload=request_payload,
            milvus_url=milvus_url,
            token=token,
            poll_interval=args.poll_interval,
            timeout_seconds=args.timeout_seconds,
            verbose=args.verbose,
        )
        overall_finished_at = time.perf_counter()
        summary = {
            **request_payload,
            "loaded_import_request_path": str(loaded_request_path),
            "summary_path": str(summary_path),
            "job_id": job_id,
            "started_at_utc": started_at_utc,
            "finished_at_utc": datetime.now(timezone.utc).isoformat(),
            "write_seconds": 0.0,
            "import_seconds": import_seconds,
            "end_to_end_seconds": overall_finished_at - overall_started_at,
            "worker_results": [],
            "bulk_import_create_response": import_payload,
            "bulk_import_final_progress": final_progress_payload,
        }
        write_json(summary_path, summary)
        if args.verbose:
            print(json.dumps(summary, indent=2), flush=True)
        print(f"Wrote summary to {summary_path}", flush=True)
        return

    if not args.input:
        raise ValueError("--input is required unless --load-import-request is provided")

    input_path = Path(args.input).expanduser().resolve()
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    use_mmap = env_flag("INSERT_STREAMING", default=False)
    matrix = load_matrix(str(input_path), use_mmap=use_mmap)
    if matrix.ndim != 2:
        raise ValueError(f"Expected a 2D NumPy matrix, got shape={matrix.shape}")

    total_rows = int(matrix.shape[0])
    vector_dim = args.vector_dim or int(matrix.shape[1])
    row_count = total_rows if args.corpus_size <= 0 else min(args.corpus_size, total_rows)
    chunk_size_bytes = max(1, int(math.ceil(args.chunk_size_mb * 1024 * 1024)))
    stage_dir = Path(args.stage_dir).expanduser().resolve()
    if args.writer_mode == "local":
        stage_dir.mkdir(parents=True, exist_ok=True)

    remote_endpoint = resolve_remote_endpoint(args.remote_endpoint) if args.writer_mode == "remote" else None

    maybe_check_collection(milvus_url, token, args.collection)

    print(
        f"Preparing bulk files for {row_count} rows from {input_path} across {args.processes} processes",
        flush=True,
    )

    write_started_at = time.perf_counter()
    manager = mp.Manager()
    shared_progress = manager.dict()
    shared_results = manager.list()
    processes: list[mp.Process] = []

    for worker_index in range(args.processes):
        process = mp.Process(
            target=worker_write,
            args=(
                worker_index,
                args.processes,
                str(input_path),
                row_count,
                args.row_id_start,
                args.batch_rows,
                str(stage_dir),
                chunk_size_bytes,
                args.file_type,
                args.writer_mode,
                args.id_field,
                args.vector_field,
                vector_dim,
                args.progress_every_rows,
                remote_endpoint,
                args.remote_bucket,
                args.remote_access_key,
                args.remote_secret_key,
                args.remote_secure,
                args.remote_path,
                use_mmap,
                shared_progress,
                shared_results,
            ),
        )
        process.start()
        processes.append(process)

    monitor_writer_progress(
        processes=processes,
        shared_progress=shared_progress,
        row_count=row_count,
        verbose=args.verbose,
    )

    worker_results = sorted(list(shared_results), key=lambda item: item["worker_index"])

    failed_exit_codes = [process.exitcode for process in processes if process.exitcode != 0]
    failed_workers = [result for result in worker_results if result["status"] != "ok"]
    if len(worker_results) != args.processes:
        raise RuntimeError(
            f"Expected {args.processes} worker results, got {len(worker_results)}. "
            f"exit_codes={[process.exitcode for process in processes]}"
        )
    if failed_exit_codes or failed_workers:
        raise RuntimeError(
            f"Bulk file generation failed. exit_codes={failed_exit_codes}, worker_results={failed_workers}"
        )

    write_finished_at = time.perf_counter()
    batch_files = flatten_batch_files(worker_results)
    if not batch_files:
        raise RuntimeError("No bulk-import files were generated.")

    print(f"Generated {len(batch_files)} batch file groups", flush=True)

    request_payload = build_import_request(
        input_path=input_path,
        milvus_url=milvus_url,
        remote_endpoint=remote_endpoint,
        args=args,
        row_count=row_count,
        vector_dim=vector_dim,
        batch_files=batch_files,
    )
    write_json(request_path, request_payload)
    print(f"Wrote import request to {request_path}", flush=True)

    summary = {
        **request_payload,
        "summary_path": str(summary_path),
        "import_request_path": str(request_path),
        "started_at_utc": started_at_utc,
        "finished_at_utc": datetime.now(timezone.utc).isoformat(),
        "write_seconds": write_finished_at - write_started_at,
        "worker_results": worker_results,
    }

    if args.prepare_only:
        overall_finished_at = time.perf_counter()
        summary.update(
            {
                "job_id": None,
                "import_seconds": 0.0,
                "end_to_end_seconds": overall_finished_at - overall_started_at,
                "bulk_import_create_response": None,
                "bulk_import_final_progress": None,
            }
        )
        write_json(summary_path, summary)
        if args.verbose:
            print(json.dumps(summary, indent=2), flush=True)
        print(f"Wrote summary to {summary_path}", flush=True)
        return

    job_id, import_payload, final_progress_payload, import_seconds = start_import_from_request(
        request_payload=request_payload,
        milvus_url=milvus_url,
        token=token,
        poll_interval=args.poll_interval,
        timeout_seconds=args.timeout_seconds,
        verbose=args.verbose,
    )
    overall_finished_at = time.perf_counter()
    summary.update(
        {
            "job_id": job_id,
            "finished_at_utc": datetime.now(timezone.utc).isoformat(),
            "import_seconds": import_seconds,
            "end_to_end_seconds": overall_finished_at - overall_started_at,
            "bulk_import_create_response": import_payload,
            "bulk_import_final_progress": final_progress_payload,
        }
    )

    write_json(summary_path, summary)
    if args.verbose:
        print(json.dumps(summary, indent=2), flush=True)
    print(f"Wrote summary to {summary_path}", flush=True)


if __name__ == "__main__":
    main()
