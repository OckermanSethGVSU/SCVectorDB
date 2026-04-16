import argparse
import json
import logging
import math
import multiprocessing as mp
import os
import queue
import shutil
import subprocess
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
from pymilvus import CollectionSchema, DataType, FieldSchema, MilvusClient
from pymilvus.bulk_writer import BulkFileType, LocalBulkWriter, bulk_import, get_import_progress


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
        "pymilvus.bulk_writer.bulk_import",
        "EndpointResolver",
        "bulk_writer",
    ):
        logger = logging.getLogger(logger_name)
        logger.setLevel(level)
        if not verbose:
            logger.propagate = False


def parse_args() -> argparse.Namespace:
    default_input = first_non_empty(os.getenv("INSERT_DATA_FILEPATH"), os.getenv("DATA_FILEPATH"))
    parser = argparse.ArgumentParser(
        description="Generate local bulk files, pipeline them to MinIO with mc cp, and optionally launch Milvus bulk_import.",
    )
    parser.add_argument("--input", default=default_input, help="Path to the source .npy matrix.")
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
    parser.add_argument("--collection", default=os.getenv("COLLECTION_NAME", "standalone"), help="Target Milvus collection.")
    parser.add_argument("--db-name", default=os.getenv("DB_NAME", "default"), help="Target Milvus database name.")
    parser.add_argument("--vector-field", default=os.getenv("VECTOR_FIELD", "vector"), help="Vector field name.")
    parser.add_argument("--id-field", default=os.getenv("ID_FIELD", "id"), help="Primary key field name.")
    parser.add_argument(
        "--vector-dim",
        type=int,
        default=env_int("VECTOR_DIM", 0),
        help="Vector dimension; 0 infers it from the input matrix.",
    )
    parser.add_argument(
        "--stage-dir",
        default=os.getenv("BULK_IMPORT_STAGE_DIR"),
        help="Local staging directory for generated bulk files before upload. Overrides --staging-medium.",
    )
    parser.add_argument(
        "--staging-medium",
        default=os.getenv("BULK_UPLOAD_STAGING_MEDIUM", os.getenv("STORAGE_MEDIUM", "lustre")),
        choices=["memory", "lustre", "SSD", "ssd"],
        help="Where temporary staged bulk files live before upload: memory (/dev/shm), lustre (run dir), or SSD (/local/scratch).",
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
        default=os.getenv("BULK_IMPORT_SUMMARY_PATH", "bulk_upload_import_mc_summary.json"),
        help="Where to write the end-to-end timing and import summary JSON.",
    )
    parser.add_argument(
        "--import-request-path",
        default=os.getenv("BULK_IMPORT_REQUEST_PATH", "bulk_import_request.json"),
        help="Where to write the reusable bulk-import request JSON after files are uploaded.",
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
        help="Generate/upload files and the reusable import request JSON, but do not start bulk_import.",
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
        help="S3/MinIO endpoint. Defaults to MINIO_ENDPOINT or MINIO_ADDRESS.",
    )
    parser.add_argument(
        "--remote-bucket",
        default=os.getenv("MINIO_BUCKET_NAME", "a-bucket"),
        help="Bucket name for uploaded bulk files.",
    )
    parser.add_argument(
        "--remote-access-key",
        default=first_non_empty(os.getenv("MINIO_ACCESS_KEY_ID"), os.getenv("AWS_ACCESS_KEY_ID"), "minioadmin"),
        help="Access key for S3/MinIO.",
    )
    parser.add_argument(
        "--remote-secret-key",
        default=first_non_empty(os.getenv("MINIO_SECRET_ACCESS_KEY"), os.getenv("AWS_SECRET_ACCESS_KEY"), "minioadmin"),
        help="Secret key for S3/MinIO.",
    )
    parser.add_argument(
        "--remote-secure",
        action="store_true",
        default=os.getenv("MINIO_USE_SSL", "false").strip().lower() == "true",
        help="Use TLS for S3/MinIO.",
    )
    parser.add_argument(
        "--remote-path",
        default=os.getenv("BULK_REMOTE_PATH", "bulk-import"),
        help="Object prefix for uploaded bulk files.",
    )
    parser.add_argument(
        "--mc-bin",
        default=os.getenv("MC_BIN", "mc"),
        help="Path to the MinIO client executable.",
    )
    parser.add_argument(
        "--mc-alias",
        default=os.getenv("MC_ALIAS", "bulkstage"),
        help="Base alias name for temporary mc config.",
    )
    parser.add_argument(
        "--mc-config-root",
        default=os.getenv("MC_CONFIG_ROOT", ".mc-config"),
        help="Directory root for per-worker mc config dirs.",
    )
    parser.add_argument(
        "--delete-after-upload",
        action="store_true",
        default=env_flag("BULK_IMPORT_DELETE_AFTER_UPLOAD", default=True),
        help="Delete staged files after a successful mc cp. Enabled by default.",
    )
    return parser.parse_args()


def resolve_milvus_url(explicit_url: str | None) -> str:
    if explicit_url:
        return explicit_url
    host = os.getenv("MILVUS_HOST")
    if not host and Path("worker.ip").exists():
        host = read_ip_from_file("worker.ip")
    if not host:
        host = "127.0.0.1"
    port = env_int("MILVUS_GRPC_PORT", 19530)
    return f"http://{host}:{port}"


def resolve_remote_endpoint(explicit_endpoint: str | None) -> str:
    endpoint = explicit_endpoint
    if not endpoint:
        endpoint = os.getenv("MINIO_ADDRESS")
    if not endpoint and Path("minio_registry.txt").exists():
        host, port = read_registry_host_port("minio_registry.txt")
        endpoint = f"{host}:{port}"
    if not endpoint:
        endpoint = "127.0.0.1:9000"
    return endpoint


def resolve_stage_dir(explicit_stage_dir: str | None, staging_medium: str) -> Path:
    if explicit_stage_dir:
        return Path(explicit_stage_dir).expanduser().resolve()

    medium = staging_medium.strip()
    medium_lower = medium.lower()
    run_dir = Path.cwd()
    run_name = run_dir.name

    if medium_lower == "memory":
        return Path("/dev/shm") / run_name / "bulk-import-stage"
    if medium_lower == "lustre":
        return (run_dir / "bulk-import-stage").resolve()
    if medium_lower == "ssd":
        return Path("/local/scratch") / run_name / "bulk-import-stage"
    raise ValueError(f"Unsupported BULK_UPLOAD_STAGING_MEDIUM '{staging_medium}'")


def maybe_check_collection(url: str, token: str, collection_name: str) -> None:
    try:
        client = MilvusClient(uri=url, token=token)
        if not client.has_collection(collection_name):
            raise RuntimeError(f"Collection '{collection_name}' does not exist at {url}")
    except Exception as exc:
        print(f"Collection preflight check skipped or failed: {exc}", flush=True)


def build_import_request(
    *,
    input_path: Path,
    milvus_url: str,
    remote_endpoint: str,
    stage_dir: Path,
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
        "stage_dir": str(stage_dir),
        "writer_mode": "remote",
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
        "remote_bucket": args.remote_bucket,
        "remote_path": args.remote_path,
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


def flatten_batch_files(results: list[dict[str, Any]]) -> list[list[str]]:
    batch_files: list[list[str]] = []
    for result in results:
        batch_files.extend(result["batch_files"])
    return batch_files


def make_mc_env() -> dict[str, str]:
    env = os.environ.copy()
    for key in ("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY"):
        env.pop(key, None)
    return env


def run_mc_command(mc_bin: str, config_dir: Path, args: list[str]) -> None:
    subprocess.run(
        [mc_bin, "--config-dir", str(config_dir), *args],
        check=True,
        env=make_mc_env(),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )


def ensure_mc_alias(
    *,
    mc_bin: str,
    config_dir: Path,
    alias_name: str,
    endpoint: str,
    access_key: str | None,
    secret_key: str | None,
    secure: bool,
) -> None:
    config_dir.mkdir(parents=True, exist_ok=True)
    scheme = "https" if secure else "http"
    run_mc_command(
        mc_bin,
        config_dir,
        ["alias", "set", alias_name, f"{scheme}://{endpoint}", access_key or "", secret_key or ""],
    )


def upload_file_with_mc(
    *,
    mc_bin: str,
    config_dir: Path,
    alias_name: str,
    bucket: str,
    source_path: Path,
    object_key: str,
) -> None:
    run_mc_command(
        mc_bin,
        config_dir,
        ["cp", "--quiet", str(source_path), f"{alias_name}/{bucket}/{object_key}"],
    )


def uploader_loop(
    *,
    upload_queue: "queue.Queue[tuple[list[Path], list[str]] | None]",
    uploaded_batch_groups: list[list[str]],
    worker_index: int,
    mc_bin: str,
    mc_config_dir: Path,
    mc_alias: str,
    remote_endpoint: str,
    remote_bucket: str,
    remote_access_key: str | None,
    remote_secret_key: str | None,
    remote_secure: bool,
    delete_after_upload: bool,
) -> tuple[int, str | None]:
    try:
        ensure_mc_alias(
            mc_bin=mc_bin,
            config_dir=mc_config_dir,
            alias_name=mc_alias,
            endpoint=remote_endpoint,
            access_key=remote_access_key,
            secret_key=remote_secret_key,
            secure=remote_secure,
        )
        uploaded_files = 0
        while True:
            item = upload_queue.get()
            if item is None:
                return uploaded_files, None
            local_paths, remote_paths = item
            for local_path, remote_path in zip(local_paths, remote_paths, strict=True):
                upload_file_with_mc(
                    mc_bin=mc_bin,
                    config_dir=mc_config_dir,
                    alias_name=mc_alias,
                    bucket=remote_bucket,
                    source_path=local_path,
                    object_key=remote_path,
                )
                uploaded_files += 1
                if delete_after_upload:
                    try:
                        local_path.unlink()
                    except FileNotFoundError:
                        pass
            uploaded_batch_groups.append(remote_paths)
    except Exception as exc:
        return 0, f"worker {worker_index} uploader failed: {exc!r}"


def normalize_batch_group(group: Any) -> list[str]:
    if isinstance(group, str):
        return [group]
    return [str(item) for item in group]


def worker_write_and_upload(
    worker_index: int,
    process_count: int,
    input_path: str,
    row_count: int,
    row_id_start: int,
    batch_row_count: int,
    stage_dir: str,
    chunk_size_bytes: int,
    file_type_name: str,
    id_field: str,
    vector_field: str,
    vector_dim: int,
    remote_endpoint: str,
    remote_bucket: str,
    remote_access_key: str | None,
    remote_secret_key: str | None,
    remote_secure: bool,
    remote_path: str,
    mc_bin: str,
    mc_alias_base: str,
    mc_config_root: str,
    delete_after_upload: bool,
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
                "upload_seconds": 0.0,
                "status": "ok",
            }
        )
        shared_progress[worker_index] = 0
        return

    data = load_matrix(input_path, use_mmap=use_mmap)
    schema = build_schema(id_field=id_field, vector_field=vector_field, vector_dim=vector_dim)
    stage_root = Path(stage_dir).expanduser().resolve()
    worker_root = stage_root / f"proc_{worker_index:03d}"
    writer = LocalBulkWriter(
        schema=schema,
        local_path=str(worker_root),
        chunk_size=chunk_size_bytes,
        file_type=file_type,
    )

    upload_queue: "queue.Queue[tuple[list[Path], list[str]] | None]" = queue.Queue()
    uploaded_batch_groups: list[list[str]] = []
    uploader_result: dict[str, Any] = {"uploaded_files": 0, "error": None}
    mc_config_dir = Path(mc_config_root).expanduser().resolve() / f"worker_{worker_index:03d}"
    mc_alias = f"{mc_alias_base}-{worker_index:03d}-{uuid.uuid4().hex[:8]}"

    def uploader_runner() -> None:
        uploaded_files, error = uploader_loop(
            upload_queue=upload_queue,
            uploaded_batch_groups=uploaded_batch_groups,
            worker_index=worker_index,
            mc_bin=mc_bin,
            mc_config_dir=mc_config_dir,
            mc_alias=mc_alias,
            remote_endpoint=remote_endpoint,
            remote_bucket=remote_bucket,
            remote_access_key=remote_access_key,
            remote_secret_key=remote_secret_key,
            remote_secure=remote_secure,
            delete_after_upload=delete_after_upload,
        )
        uploader_result["uploaded_files"] = uploaded_files
        uploader_result["error"] = error

    uploader_thread = threading.Thread(target=uploader_runner, daemon=True)
    uploader_thread.start()

    rows_written = 0
    shared_progress[worker_index] = 0
    seen_groups = 0
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

            batch_groups = writer.batch_files
            while seen_groups < len(batch_groups):
                raw_group = normalize_batch_group(batch_groups[seen_groups])
                local_paths = [Path(path).expanduser().resolve() for path in raw_group]
                remote_paths = [
                    str(Path(remote_path.strip("/")) / local_path.relative_to(stage_root).as_posix())
                    for local_path in local_paths
                ]
                upload_queue.put((local_paths, remote_paths))
                seen_groups += 1
    except Exception as exc:
        upload_queue.put(None)
        uploader_thread.join()
        shared_results.append(
            {
                "worker_index": worker_index,
                "start_row": start_row,
                "stop_row": stop_row,
                "rows": rows_written,
                "batch_files": uploaded_batch_groups,
                "write_seconds": time.perf_counter() - started_at,
                "upload_seconds": time.perf_counter() - started_at,
                "status": "error",
                "error": repr(exc),
            }
        )
        raise

    upload_queue.put(None)
    uploader_thread.join()
    if uploader_result["error"]:
        raise RuntimeError(str(uploader_result["error"]))

    shared_progress[worker_index] = rows_written
    finished_at = time.perf_counter()
    shared_results.append(
        {
            "worker_index": worker_index,
            "start_row": start_row,
            "stop_row": stop_row,
            "rows": rows_written,
            "batch_files": uploaded_batch_groups,
            "write_seconds": finished_at - started_at,
            "upload_seconds": finished_at - started_at,
            "uploaded_files": uploader_result["uploaded_files"],
            "status": "ok",
        }
    )


def monitor_progress(
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
        print_progress_line("Write+Ship", progress, f"rows={total_written}/{row_count}")
        if all(not process.is_alive() for process in processes):
            finish_progress_line()
            break
        time.sleep(0.5)

    for process in processes:
        process.join()


def main() -> None:
    args = parse_args()
    configure_logging(args.verbose)
    if shutil.which(args.mc_bin) is None:
        raise FileNotFoundError(f"mc executable not found: {args.mc_bin}")
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
    remote_endpoint = resolve_remote_endpoint(args.remote_endpoint)
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
            "upload_seconds": 0.0,
            "import_seconds": import_seconds,
            "end_to_end_seconds": overall_finished_at - overall_started_at,
            "worker_results": [],
            "bulk_import_create_response": import_payload,
            "bulk_import_final_progress": final_progress_payload,
            "transport": "mc_cp",
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
    stage_dir = resolve_stage_dir(args.stage_dir, args.staging_medium)
    stage_dir.mkdir(parents=True, exist_ok=True)

    maybe_check_collection(milvus_url, token, args.collection)

    print(
        f"Preparing and uploading bulk files for {row_count} rows from {input_path} across {args.processes} processes",
        flush=True,
    )

    transfer_started_at = time.perf_counter()
    manager = mp.Manager()
    shared_progress = manager.dict()
    shared_results = manager.list()
    processes: list[mp.Process] = []

    for worker_index in range(args.processes):
        process = mp.Process(
            target=worker_write_and_upload,
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
                args.id_field,
                args.vector_field,
                vector_dim,
                remote_endpoint,
                args.remote_bucket,
                args.remote_access_key,
                args.remote_secret_key,
                args.remote_secure,
                args.remote_path,
                args.mc_bin,
                args.mc_alias,
                args.mc_config_root,
                args.delete_after_upload,
                use_mmap,
                shared_progress,
                shared_results,
            ),
        )
        process.start()
        processes.append(process)

    monitor_progress(processes=processes, shared_progress=shared_progress, row_count=row_count, verbose=args.verbose)

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
            f"Bulk file generation/upload failed. exit_codes={failed_exit_codes}, worker_results={failed_workers}"
        )

    transfer_finished_at = time.perf_counter()
    batch_files = flatten_batch_files(worker_results)
    if not batch_files:
        raise RuntimeError("No uploaded bulk-import files were generated.")

    print(f"Uploaded {len(batch_files)} batch file groups", flush=True)

    request_payload = build_import_request(
        input_path=input_path,
        milvus_url=milvus_url,
        remote_endpoint=remote_endpoint,
        stage_dir=stage_dir,
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
        "write_seconds": transfer_finished_at - transfer_started_at,
        "upload_seconds": transfer_finished_at - transfer_started_at,
        "worker_results": worker_results,
        "transport": "mc_cp",
        "delete_after_upload": args.delete_after_upload,
        "mc_bin": args.mc_bin,
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
