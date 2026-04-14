#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONTAINER_NAME="${QDRANT_LOCAL_NAME:-qdrant-local}"
DATA_DIR="${QDRANT_LOCAL_DATA_DIR:-$ROOT_DIR/.local/qdrant/storage}"
CONFIG_DIR="${QDRANT_LOCAL_CONFIG_DIR:-$ROOT_DIR/.local/qdrant/config}"
SNAPSHOT_DIR="${QDRANT_LOCAL_SNAPSHOT_DIR:-$ROOT_DIR/.local/qdrant/snapshots}"
OUTPUT_DIR="${QDRANT_LOCAL_OUTPUT_DIR:-$ROOT_DIR/local_test_data}"
PERF_DIR="${QDRANT_LOCAL_PERF_DIR:-$ROOT_DIR/perf}"
REGISTRY_PATH="${QDRANT_LOCAL_REGISTRY_PATH:-$ROOT_DIR/ip_registry.txt}"

if command -v docker >/dev/null 2>&1; then
    CONTAINER_RUNTIME="docker"
elif command -v podman >/dev/null 2>&1; then
    CONTAINER_RUNTIME="podman"
else
    CONTAINER_RUNTIME=""
fi

if [[ -n "$CONTAINER_RUNTIME" ]]; then
    if "$CONTAINER_RUNTIME" ps --format '{{.Names}}' | grep -Fxq "$CONTAINER_NAME"; then
        echo "Stopping local Qdrant container '$CONTAINER_NAME'..."
        "$CONTAINER_RUNTIME" stop "$CONTAINER_NAME" >/dev/null
    fi
    if "$CONTAINER_RUNTIME" ps -a --format '{{.Names}}' | grep -Fxq "$CONTAINER_NAME"; then
        echo "Removing local Qdrant container '$CONTAINER_NAME'..."
        "$CONTAINER_RUNTIME" rm -f "$CONTAINER_NAME" >/dev/null
    fi
fi

rm -f "$REGISTRY_PATH"
rm -f "$PERF_DIR/workflow_start.txt" "$PERF_DIR/workflow_stop.txt"
rm -f "$ROOT_DIR"/*.txt "$ROOT_DIR"/*.csv "$ROOT_DIR"/*.out "$ROOT_DIR"/*.json "$ROOT_DIR"/*.yaml
rm -f "$ROOT_DIR"/insert_*_rank_*.npy "$ROOT_DIR"/query_*_rank_*.npy
rm -f "$ROOT_DIR"/insert_times.csv "$ROOT_DIR"/query_times.csv
rm -rf "$DATA_DIR" "$CONFIG_DIR" "$SNAPSHOT_DIR" "$OUTPUT_DIR"

if [[ -d "$ROOT_DIR/.local/qdrant" ]] && [[ -z "$(find "$ROOT_DIR/.local/qdrant" -mindepth 1 -print -quit 2>/dev/null)" ]]; then
    rmdir "$ROOT_DIR/.local/qdrant"
fi

echo "Local test artifacts removed."
