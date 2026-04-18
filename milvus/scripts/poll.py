import os
import time
from pathlib import Path
from typing import Iterable, List, Tuple

import requests

RUNTIME_STATE_DIR = Path(os.getenv("RUNTIME_STATE_DIR", "./runtime_state"))


def runtime_state_path(name: str) -> Path:
    return RUNTIME_STATE_DIR / name


REGISTRY_FILES = [
    runtime_state_path("DATA_registry.txt"),
    runtime_state_path("COORDINATOR_registry.txt"),
    runtime_state_path("STREAMING_registry.txt"),
    runtime_state_path("QUERY_registry.txt"),
    runtime_state_path("PROXY_registry.txt"),
]


def parse_registry_file(path: str | Path) -> List[Tuple[int, str, int, int]]:
    """
    Returns list of (rank, ip, service_port, metrics_port).
    Ignores blank lines and malformed rows.
    """
    entries: List[Tuple[int, str, int, int]] = []
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Registry file not found: {path}")

    for lineno, raw in enumerate(p.read_text().splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue

        parts = [x.strip() for x in line.split(",")]
        if len(parts) < 4:
            # If you ever have older 3-col files, you can decide what to do here.
            # For now: skip and warn.
            print(f"[WARN] {path}:{lineno} expected 4 columns, got {len(parts)}: {line}", flush=True)
            continue

        try:
            rank = int(parts[0])
            ip = parts[1]
            service_port = int(parts[2])
            metrics_port = int(parts[3])
        except Exception as e:
            print(f"[WARN] {path}:{lineno} parse error ({e}): {line}", flush=True)
            continue

        entries.append((rank, ip, service_port, metrics_port))

    if not entries:
        raise RuntimeError(f"No usable entries found in {path}")

    # Stable order: rank ascending
    entries.sort(key=lambda t: t[0])
    return entries


def wait_for_healthz(host: str, port: int, label: str = "", retries: int = 10000, sleep_s: float = 3.0) -> None:
    url = f"http://{host}:{port}/healthz"
    prefix = f"[{label}] " if label else ""

    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, timeout=2)
            if r.status_code == 200:
                print(f"{prefix}Online: {url}", flush=True)
                return
        except Exception:
            pass

        if attempt % 20 == 0:
            print(f"{prefix}Waiting ({attempt}/{retries}): {url}", flush=True)
        time.sleep(sleep_s)

    raise RuntimeError(f"{prefix}Did not become healthy in time: {url}")


def wait_for_distributed_cluster(registry_files: Iterable[str]) -> None:
    """
    For each registry file, wait for all entries' metrics health endpoint.
    """
    for reg in registry_files:
        entries = parse_registry_file(reg)
        print(f"\n== {reg}: {len(entries)} entries ==", flush=True)

        for rank, ip, svc_port, metrics_port in entries:
            label = f"{reg} rank={rank} svc={svc_port} metrics={metrics_port}"
            # Per your requirement: use ip + metrics_port for /healthz polling
            wait_for_healthz(ip, metrics_port, label=label)


# ---- main wiring ----
def read_ip_from_file(path: str) -> str:
    with open(path, "r") as f:
        return f.read().strip()


MODE = os.getenv("MODE", "standalone").lower()

if MODE == "standalone":
    MILVUS_HOST = read_ip_from_file(runtime_state_path("worker.ip"))
    MILVUS_HEALTH_PORT = int(os.getenv("MILVUS_HEALTH_PORT", "9091"))
    wait_for_healthz(MILVUS_HOST, MILVUS_HEALTH_PORT, label="standalone")

elif MODE == "distributed":
    wait_for_distributed_cluster(REGISTRY_FILES)

else:
    raise ValueError(f"Unknown MODE='{MODE}'. Expected 'standalone' or 'distributed'.")
