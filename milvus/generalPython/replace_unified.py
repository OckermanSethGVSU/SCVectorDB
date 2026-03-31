import argparse
import os
import time
from pathlib import Path
from typing import Dict

BLOCK = 8
PORT_BASES = {
    "COORDINATOR": 20000,
    "PROXY": 20001,
    "INTERNAL_PROXY": 20002,
    "COORDINATOR_QUERY": 20003,
    "QUERY": 20004,
    "COORDINATOR_DATA": 20005,
    "DATA": 20006,
    "STREAMING": 20007,
}
COMPONENT_SPECS = {
    "COORDINATOR": {
        "tokens": ["<COORDINATOR>", "<COORDINATOR_QUERY>", "<COORDINATOR_DATA>"],
        "ports": [
            ("port", PORT_BASES["COORDINATOR"]),
            ("port", PORT_BASES["COORDINATOR_QUERY"]),
            ("port", PORT_BASES["COORDINATOR_DATA"]),
        ],
    },
    "PROXY": {
        "tokens": ["<PROXY>", "<INTERNAL_PROXY>"],
        "ports": [
            ("port", PORT_BASES["PROXY"]),
            ("internalPort", PORT_BASES["INTERNAL_PROXY"]),
        ],
    },
    "QUERY": {
        "tokens": ["<QUERY>"],
        "ports": [("port", PORT_BASES["QUERY"])],
    },
    "DATA": {
        "tokens": ["<DATA>"],
        "ports": [("port", PORT_BASES["DATA"])],
    },
    "STREAMING": {
        "tokens": ["<STREAMING>"],
        "ports": [("port", PORT_BASES["STREAMING"])],
    },
}


def replace_port_line(text: str, old_port: int, new_port: int, field: str = "port") -> str:
    old_token_1 = f"{field}: {old_port}"
    old_token_2 = f"{field}:{old_port}"
    new_token = f"{field}: {new_port}"

    if old_token_1 in text:
        return text.replace(old_token_1, new_token)
    if old_token_2 in text:
        return text.replace(old_token_2, new_token)
    return text


def get_ip_by_rank(filename: str, target_rank: int, timeout_s: float = 60.0) -> str:
    deadline = time.time() + timeout_s
    target_rank = str(target_rank)

    while not os.path.exists(filename):
        if time.time() >= deadline:
            raise TimeoutError(f"Timed out after {timeout_s}s waiting for file: {filename}")
        time.sleep(1)

    deadline = time.time() + timeout_s
    while True:
        with open(filename) as f:
            for line in f:
                parts = line.strip().split(",")
                if len(parts) < 2:
                    continue
                if parts[0] == target_rank:
                    return parts[1]

        if time.time() >= deadline:
            break
        time.sleep(1)

    raise ValueError(f"Rank {target_rank} not found in {filename}")


def get_etcd_mode() -> str:
    return os.environ.get("ETCD_MODE", "replicated").strip().lower()


def get_dml_channels() -> str:
    value = os.environ.get("DML_CHANNELS", "16").strip().lower()
    return "16" if value == "" else value


def get_minio_mode() -> str:
    return os.environ.get("MINIO_MODE", "off").strip().lower()


def load_unified_template() -> str:
    return Path("configs/unified_milvus.yaml").read_text()


def load_distributed_component_template() -> str:
    distributed_path = Path("configs/distributed_milvus.yaml")
    if distributed_path.exists():
        return distributed_path.read_text()
    return load_unified_template()


def apply_common_tuning(text: str) -> str:
    gpu_enabled = os.environ.get("GPU_INDEX", "false").strip().lower() == "true"
    if not gpu_enabled:
        text = text.replace("initMemSize: 0", "initMemSize: 2048")
        text = text.replace("maxMemSize: 0", "maxMemSize: 4096")
    return text


def apply_debug_config(text: str) -> str:
    debug_enabled = os.environ.get("DEBUG", "false").strip().lower() in ["true", "t", "1", "yes"]
    if debug_enabled:
        text = text.replace(
            "level: info # Only supports debug, info, warn, error, panic, or fatal. Default 'info'.",
            "level: debug # Only supports debug, info, warn, error, panic, or fatal. Default 'info'.",
        )
        text = text.replace("logLevel: fatal", "logLevel: debug")
        text = text.replace("level: error", "level: debug")
        text = text.replace("level: WARNING", "level: DEBUG")
    else:
        text = text.replace(
            "level: info # Only supports debug, info, warn, error, panic, or fatal. Default 'info'.",
            "level: error # Only supports debug, info, warn, error, panic, or fatal. Default 'info'.",
        )
        text = text.replace("logLevel: fatal", "logLevel: error")
        text = text.replace("level: WARNING", "level: ERROR")
    return text


def apply_tracing_config(text: str) -> str:
    tracing = os.environ.get("TRACING", "false").strip().lower() == "true"
    if tracing:
        trace_ip = Path("otel.ip").read_text().strip()
        text = text.replace("<TRACE_FRACTION>", "1")
        text = text.replace("<OLTP_HTTP>", trace_ip)
    else:
        text = text.replace("exporter: otlp", "exporter: noop")
        text = text.replace("<TRACE_FRACTION>", "0")
        text = text.replace("<OLTP_HTTP>:4317", "")
    return text


def finalize_text(text: str, replacements: Dict[str, str]) -> str:
    for token, value in replacements.items():
        text = text.replace(token, value)
    return text


def build_standalone_config(wal: str) -> str:
    worker_ip = Path("worker.ip").read_text().strip()
    standalone_minio_mode = get_minio_mode()
    minio_ip = worker_ip

    if standalone_minio_mode == "single":
        minio_ip = get_ip_by_rank("minio_registry.txt", 0)
    elif standalone_minio_mode != "off":
        raise ValueError(
            f"Unsupported MINIO_MODE='{standalone_minio_mode}' for standalone. Expected 'off' or 'single'."
        )

    text = load_unified_template()
    text = finalize_text(
        text,
        {
            "<ETCD0>:2379,<ETCD1>:2379,<ETCD2>:2379": f"{worker_ip}:2379",
            "<MINIO>": minio_ip,
            "<WAL>": wal,
            "<MQ>": worker_ip,
            "<DML>": "16",
            "<COORDINATOR>": worker_ip,
            "<PROXY>": worker_ip,
            "<QUERY>": worker_ip,
            "<DATA>": worker_ip,
            "<STREAMING>": worker_ip,
            "<TLS_SNI>": "localhost",
        },
    )
    storage_type = "remote" if standalone_minio_mode == "single" else "local"
    text = text.replace("storageType: remote", f"storageType: {storage_type}")
    text = apply_tracing_config(text)
    text = apply_debug_config(text)
    text = apply_common_tuning(text)
    return text


def build_distributed_base_config(wal: str) -> str:
    text = load_unified_template()

    minio_ip = get_ip_by_rank("minio_registry.txt", 0)
    text = text.replace("<MINIO>", minio_ip)
    text = text.replace("<WAL>", wal)

    etcd_mode = get_etcd_mode()
    if etcd_mode == "single":
        etcd0 = get_ip_by_rank("etcd_registry.txt", 0)
        text = text.replace("<ETCD0>", etcd0)
        text = text.replace(",<ETCD1>:2379", "")
        text = text.replace(",<ETCD2>:2379", "")
    else:
        etcd0 = get_ip_by_rank("etcd_registry.txt", 0)
        etcd1 = get_ip_by_rank("etcd_registry.txt", 1)
        etcd2 = get_ip_by_rank("etcd_registry.txt", 2)
        text = text.replace("<ETCD0>:2379", f"{etcd0}:2379")
        text = text.replace("<ETCD1>:2379", f"{etcd1}:2479")
        text = text.replace("<ETCD2>:2379", f"{etcd2}:2579")

    text = text.replace("<DML>", get_dml_channels())
    text = text.replace("<TLS_SNI>", "localhost")
    text = apply_tracing_config(text)
    text = apply_debug_config(text)
    text = apply_common_tuning(text)
    return text


def build_component_config(mode: str, rank: int) -> str:
    spec = COMPONENT_SPECS[mode]
    text = load_distributed_component_template()
    ip = get_ip_by_rank(f"{mode}_registry.txt", rank)
    for token in spec["tokens"]:
        text = text.replace(token, ip)

    for field, old_port in spec["ports"]:
        new_port = old_port + (BLOCK * rank)
        text = replace_port_line(text, old_port, new_port, field=field)

    for component, component_spec in COMPONENT_SPECS.items():
        if component != mode:
            for token in component_spec["tokens"]:
                text = text.replace(token, "")

    return text


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", required=True, help="Required mode argument")
    parser.add_argument("--rank", required=False, help="rank of component", default=-1)
    args = parser.parse_args()

    mode = args.mode
    rank = int(args.rank)
    wal = os.environ.get("WAL", "default").strip()

    if mode == "standalone":
        Path("configs/milvus.yaml").write_text(build_standalone_config(wal))
    elif mode == "distributed":
        Path("configs/distributed_milvus.yaml").write_text(build_distributed_base_config(wal))
    elif mode in ["COORDINATOR", "STREAMING", "QUERY", "PROXY", "DATA"]:
        Path(f"configs/{mode}{rank}.yaml").write_text(build_component_config(mode, rank))
    else:
        print("Unknown MODE env var passed in", flush=True)


if __name__ == "__main__":
    main()
