#!/usr/bin/env python3
import json
import os
import time

from qdrant_client import QdrantClient, models


def wait_for_qdrant(host: str, rest_port: int, grpc_port: int, timeout_sec: int) -> None:
    print(f"Waiting for Qdrant at {host}:{rest_port}...", flush=True)
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            client = QdrantClient(
                host=host,
                port=rest_port,
                grpc_port=grpc_port,
                prefer_grpc=True,
                timeout=10,
                grpc_options={"grpc.enable_http_proxy": 0},
            )
            client.get_collections()
            print("Qdrant is healthy.", flush=True)
            return
        except Exception:
            time.sleep(1)
    raise RuntimeError(f"Qdrant did not respond within {timeout_sec} seconds")


def load_ip_from_file(filepath: str) -> tuple[str, int]:
    with open(filepath, "r", encoding="utf-8") as f:
        rank, ip, port = f.readline().strip().split(",")
    return ip, int(port)


def main() -> int:
    registry_path = os.getenv("QDRANT_REGISTRY_PATH", "ip_registry.txt")
    collection_name = os.environ["COLLECTION_NAME"].strip()
    expected_corpus_size = int(os.getenv("EXPECTED_CORPUS_SIZE", "10000000"))
    health_timeout_sec = int(os.getenv("HEALTH_TIMEOUT_SEC", "60"))
    poll_timeout_sec = int(os.getenv("STATUS_TIMEOUT_SEC", str(60 * 10)))
    poll_interval_sec = float(os.getenv("STATUS_POLL_INTERVAL_SEC", "10"))

    base_ip, file_port = load_ip_from_file(registry_path)
    rest_port = int(os.getenv("QDRANT_REST_PORT", str(file_port - 2)))
    grpc_port = int(os.getenv("QDRANT_GRPC_PORT", str(rest_port + 1)))
    host = os.getenv("QDRANT_HOST", base_ip)

    wait_for_qdrant(host, rest_port, grpc_port, health_timeout_sec)
    time.sleep(2)

    client = QdrantClient(
        host=host,
        port=rest_port,
        grpc_port=grpc_port,
        prefer_grpc=True,
        timeout=600,
        grpc_options={"grpc.enable_http_proxy": 0},
    )

    start = time.time()
    last_print = 0.0

    while True:
        try:
            info = client.get_collection(collection_name)
            count_response = client.count(
                collection_name=collection_name,
                exact=True,
            )
            current_count = int(count_response.count)
            status = str(info.status)
            points_count = getattr(info, "points_count", None)
            indexed_vectors = getattr(info, "indexed_vectors_count", None)
        except Exception as exc:
            if time.time() - start > poll_timeout_sec:
                raise TimeoutError(f"Timed out while polling Qdrant state: {exc}") from exc
            print(f"[warn] polling failed: {exc}", flush=True)
            time.sleep(poll_interval_sec)
            continue

        now = time.time()
        if now - last_print > 60:
            print(
                f"[wait] status={status} rows={current_count}/{expected_corpus_size} points_count={points_count} indexed_vectors_count={indexed_vectors}",
                flush=True,
            )
            last_print = now

        if current_count == expected_corpus_size and info.status == models.CollectionStatus.GREEN:
            break

        if time.time() - start > poll_timeout_sec:
            raise TimeoutError(
                f"Expected row count/status not detected within {poll_timeout_sec} seconds: rows={current_count}, status={status}"
            )

        time.sleep(poll_interval_sec)

    final_info = client.get_collection(collection_name)
    print(final_info.model_dump_json(indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
