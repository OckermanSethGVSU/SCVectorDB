#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONTAINER_NAME="${QDRANT_LOCAL_NAME:-qdrant-local}"
IMAGE="${QDRANT_LOCAL_IMAGE:-qdrant/qdrant:latest}"
HOST="${QDRANT_LOCAL_HOST:-127.0.0.1}"
HTTP_PORT="${QDRANT_LOCAL_HTTP_PORT:-6333}"
GRPC_PORT="${QDRANT_LOCAL_GRPC_PORT:-6334}"
P2P_PORT="${QDRANT_LOCAL_P2P_PORT:-6335}"
DATA_DIR="${QDRANT_LOCAL_DATA_DIR:-$ROOT_DIR/.local/qdrant/storage}"
CONFIG_DIR="${QDRANT_LOCAL_CONFIG_DIR:-$ROOT_DIR/.local/qdrant/config}"
SNAPSHOT_DIR="${QDRANT_LOCAL_SNAPSHOT_DIR:-$ROOT_DIR/.local/qdrant/snapshots}"
REGISTRY_PATH="$ROOT_DIR/ip_registry.txt"
PERF_DIR="$ROOT_DIR/perf"
QDRANT_URL="http://${HOST}:${HTTP_PORT}"
COLLECTION_NAME="${QDRANT_LOCAL_COLLECTION:-singleShard}"
VECTOR_DIM="${QDRANT_LOCAL_VECTOR_DIM:-128}"
INSERT_COUNT="${QDRANT_LOCAL_INSERT_COUNT:-1000}"
QUERY_COUNT="${QDRANT_LOCAL_QUERY_COUNT:-100}"
DISTANCE="${QDRANT_LOCAL_DISTANCE:-Cosine}"
OUTPUT_DIR="${QDRANT_LOCAL_OUTPUT_DIR:-$ROOT_DIR/local_test_data}"
BINARY_PATH="$ROOT_DIR/rustCode/multiClientOP/target/debug/multiClientOP"

if command -v docker >/dev/null 2>&1; then
    CONTAINER_RUNTIME="docker"
elif command -v podman >/dev/null 2>&1; then
    CONTAINER_RUNTIME="podman"
else
    echo "Neither docker nor podman is installed." >&2
    exit 1
fi

if ! command -v curl >/dev/null 2>&1; then
    echo "curl is required." >&2
    exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
    echo "python3 is required." >&2
    exit 1
fi

mkdir -p "$DATA_DIR" "$CONFIG_DIR" "$SNAPSHOT_DIR" "$PERF_DIR" "$OUTPUT_DIR"

start_qdrant() {
    if "$CONTAINER_RUNTIME" ps --format '{{.Names}}' | grep -Fxq "$CONTAINER_NAME"; then
        echo "Qdrant container '$CONTAINER_NAME' is already running."
    else
        if "$CONTAINER_RUNTIME" ps -a --format '{{.Names}}' | grep -Fxq "$CONTAINER_NAME"; then
            echo "Starting existing Qdrant container '$CONTAINER_NAME'..."
            "$CONTAINER_RUNTIME" start "$CONTAINER_NAME" >/dev/null
        else
            echo "Launching Qdrant container '$CONTAINER_NAME' from image '$IMAGE'..."
            "$CONTAINER_RUNTIME" run -d \
                --name "$CONTAINER_NAME" \
                -p "${HTTP_PORT}:6333" \
                -p "${GRPC_PORT}:6334" \
                -p "${P2P_PORT}:6335" \
                -v "${DATA_DIR}:/qdrant/storage" \
                -v "${CONFIG_DIR}:/qdrant/config/local" \
                -v "${SNAPSHOT_DIR}:/qdrant/snapshots" \
                "$IMAGE" >/dev/null
        fi
    fi

    printf '0,%s,%s\n' "$HOST" "$P2P_PORT" > "$REGISTRY_PATH"
    rm -f "$PERF_DIR/workflow_start.txt" "$PERF_DIR/workflow_stop.txt"

    echo "Waiting for Qdrant health check on ${QDRANT_URL}/healthz ..."
    for _ in {1..60}; do
        if curl -fsS "${QDRANT_URL}/healthz" >/dev/null; then
            echo "Qdrant is ready."
            return 0
        fi
        sleep 1
    done

    echo "Qdrant did not become healthy within 60 seconds." >&2
    echo "Inspect logs with: ${CONTAINER_RUNTIME} logs ${CONTAINER_NAME}" >&2
    exit 1
}

prepare_local_data() {
    echo "Preparing collection and local .npy files..."
    python3 "$ROOT_DIR/qdrantSetup/local_prepare.py" \
        --host "$HOST" \
        --port "$HTTP_PORT" \
        --collection "$COLLECTION_NAME" \
        --distance "$DISTANCE" \
        --vector-dim "$VECTOR_DIM" \
        --insert-count "$INSERT_COUNT" \
        --query-count "$QUERY_COUNT" \
        --output-dir "$OUTPUT_DIR"
}

build_binary_if_needed() {
    if [[ ! -x "$BINARY_PATH" ]]; then
        echo "Building multiClientOP..."
        (cd "$ROOT_DIR/rustCode/multiClientOP" && cargo build)
    fi
}

run_insert() {
    echo "Running local insert test..."
    export ACTIVE_TASK=INSERT
    export N_WORKERS=1
    export INSERT_CLIENTS_PER_WORKER=1
    export INSERT_BATCH_SIZE="${INSERT_BATCH_SIZE:-1}"
    export INSERT_BALANCE_STRATEGY=NO_BALANCE
    export INSERT_CORPUS_SIZE="$INSERT_COUNT"
    export INSERT_FILEPATH="$OUTPUT_DIR/insert_vectors.npy"
    "$BINARY_PATH"
}

run_query() {
    echo "Running local query test..."
    export ACTIVE_TASK=QUERY
    export N_WORKERS=1
    export QUERY_CLIENTS_PER_WORKER=1
    export QUERY_BATCH_SIZE="${QUERY_BATCH_SIZE:-1}"
    export QUERY_BALANCE_STRATEGY=NO_BALANCE
    export QUERY_CORPUS_SIZE="$QUERY_COUNT"
    export QUERY_FILEPATH="$OUTPUT_DIR/query_vectors.npy"
    export QUERY_DEBUG_RESULTS="${QUERY_DEBUG_RESULTS:-true}"
    "$BINARY_PATH"
}

start_qdrant
prepare_local_data
build_binary_if_needed
run_insert
run_query

echo
echo "Local test completed successfully."
echo "Collection: $COLLECTION_NAME"
echo "Insert file: $OUTPUT_DIR/insert_vectors.npy"
echo "Query file: $OUTPUT_DIR/query_vectors.npy"
