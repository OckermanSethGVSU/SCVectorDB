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
QDRANT_GRPC_URL="http://${HOST}:${GRPC_PORT}"
COLLECTION_NAME="${QDRANT_LOCAL_COLLECTION:-singleShard}"
VECTOR_DIM="${QDRANT_LOCAL_VECTOR_DIM:-128}"
INSERT_COUNT="${QDRANT_LOCAL_INSERT_COUNT:-1000}"
QUERY_COUNT="${QDRANT_LOCAL_QUERY_COUNT:-100}"
DISTANCE="${QDRANT_LOCAL_DISTANCE:-Cosine}"
OUTPUT_DIR="${QDRANT_LOCAL_OUTPUT_DIR:-$ROOT_DIR/local_test_data}"
STANDARD_BINARY_PATH="$ROOT_DIR/rustCode/multiClientOP/target/debug/multiClientOP"
MIXED_BINARY_PATH="$ROOT_DIR/rustCode/mixedrunner/target/debug/mixedrunner"
TEST_MODE="standard"

usage() {
    cat <<EOF
Usage: $(basename "$0") [--standard | --mixed] [options]

Modes:
  --standard                  Run the existing insert-then-query workflow (default)
  --mixed                     Run the mixed insert/query runner locally

Mixed options:
  --insert-clients N          Total insert workers for mixed mode
  --query-clients N           Total query workers for mixed mode
  --insert-batch-size N       Insert batch size
  --query-batch-size N        Query batch size
  --mode MODE                 Global mode: max or rate
  --insert-mode MODE          Insert mode override: max or rate
  --query-mode MODE           Query mode override: max or rate
  --insert-ops-per-sec RATE   Aggregate insert ops/sec in mixed rate mode
  --query-ops-per-sec RATE    Aggregate query ops/sec in mixed rate mode
  --top-k N                   Query top-k
  --ef-search N               Qdrant hnsw_ef for query requests
  --rpc-timeout DUR           Per-op timeout, e.g. 30s or 5m
  --help                      Show this help text

Environment variables still work and can override most defaults.
EOF
}

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

MIXED_INSERT_CLIENTS="${INSERT_CLIENTS:-1}"
MIXED_QUERY_CLIENTS="${QUERY_CLIENTS:-1}"
MIXED_INSERT_BATCH_SIZE="${INSERT_BATCH_SIZE:-1}"
MIXED_QUERY_BATCH_SIZE="${QUERY_BATCH_SIZE:-1}"
MIXED_MODE="${MODE:-max}"
MIXED_INSERT_MODE="${INSERT_MODE:-}"
MIXED_QUERY_MODE="${QUERY_MODE:-}"
MIXED_INSERT_OPS_PER_SEC="${INSERT_OPS_PER_SEC:-}"
MIXED_QUERY_OPS_PER_SEC="${QUERY_OPS_PER_SEC:-}"
MIXED_TOP_K="${TOP_K:-10}"
MIXED_EF_SEARCH="${QUERY_EF_SEARCH:-${EF_SEARCH:-64}}"
MIXED_RPC_TIMEOUT="${RPC_TIMEOUT:-600s}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --standard)
            TEST_MODE="standard"
            shift
            ;;
        --mixed)
            TEST_MODE="mixed"
            shift
            ;;
        --insert-clients)
            MIXED_INSERT_CLIENTS="$2"
            shift 2
            ;;
        --query-clients)
            MIXED_QUERY_CLIENTS="$2"
            shift 2
            ;;
        --insert-batch-size)
            MIXED_INSERT_BATCH_SIZE="$2"
            shift 2
            ;;
        --query-batch-size)
            MIXED_QUERY_BATCH_SIZE="$2"
            shift 2
            ;;
        --mode)
            MIXED_MODE="$2"
            shift 2
            ;;
        --insert-mode)
            MIXED_INSERT_MODE="$2"
            shift 2
            ;;
        --query-mode)
            MIXED_QUERY_MODE="$2"
            shift 2
            ;;
        --insert-ops-per-sec)
            MIXED_INSERT_OPS_PER_SEC="$2"
            shift 2
            ;;
        --query-ops-per-sec)
            MIXED_QUERY_OPS_PER_SEC="$2"
            shift 2
            ;;
        --top-k)
            MIXED_TOP_K="$2"
            shift 2
            ;;
        --ef-search)
            MIXED_EF_SEARCH="$2"
            shift 2
            ;;
        --rpc-timeout)
            MIXED_RPC_TIMEOUT="$2"
            shift 2
            ;;
        --help|-h)
            usage
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            usage >&2
            exit 1
            ;;
    esac
done

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

build_standard_binary_if_needed() {
    if [[ ! -x "$STANDARD_BINARY_PATH" ]]; then
        echo "Building multiClientOP..."
        (cd "$ROOT_DIR/rustCode/multiClientOP" && cargo build)
    fi
}

build_mixed_binary_if_needed() {
    if [[ ! -x "$MIXED_BINARY_PATH" ]]; then
        echo "Building mixedrunner..."
        (cd "$ROOT_DIR/rustCode/mixedrunner" && cargo build)
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
    "$STANDARD_BINARY_PATH"
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
    "$STANDARD_BINARY_PATH"
}

run_mixed() {
    echo "Running local mixed insert/query test..."
    export QDRANT_URL="$QDRANT_GRPC_URL"
    export COLLECTION_NAME
    export RESULT_PATH="${RESULT_PATH:-$OUTPUT_DIR/mixed_logs}"
    export INSERT_CLIENTS="$MIXED_INSERT_CLIENTS"
    export QUERY_CLIENTS="$MIXED_QUERY_CLIENTS"
    export INSERT_BATCH_SIZE="$MIXED_INSERT_BATCH_SIZE"
    export QUERY_BATCH_SIZE="$MIXED_QUERY_BATCH_SIZE"
    export INSERT_BALANCE_STRATEGY="${INSERT_BALANCE_STRATEGY:-NO_BALANCE}"
    export QUERY_BALANCE_STRATEGY="${QUERY_BALANCE_STRATEGY:-NO_BALANCE}"
    export INSERT_CORPUS_SIZE="$INSERT_COUNT"
    export QUERY_CORPUS_SIZE="$QUERY_COUNT"
    export INSERT_FILEPATH="$OUTPUT_DIR/insert_vectors.npy"
    export QUERY_FILEPATH="$OUTPUT_DIR/query_vectors.npy"
    export MODE="$MIXED_MODE"
    export TOP_K="$MIXED_TOP_K"
    export QUERY_EF_SEARCH="$MIXED_EF_SEARCH"
    export RPC_TIMEOUT="$MIXED_RPC_TIMEOUT"
    export QDRANT_REGISTRY_PATH="$REGISTRY_PATH"

    if [[ -n "$MIXED_INSERT_MODE" ]]; then
        export INSERT_MODE="$MIXED_INSERT_MODE"
    else
        unset INSERT_MODE 2>/dev/null || true
    fi
    if [[ -n "$MIXED_QUERY_MODE" ]]; then
        export QUERY_MODE="$MIXED_QUERY_MODE"
    else
        unset QUERY_MODE 2>/dev/null || true
    fi
    if [[ -n "$MIXED_INSERT_OPS_PER_SEC" ]]; then
        export INSERT_OPS_PER_SEC="$MIXED_INSERT_OPS_PER_SEC"
    else
        unset INSERT_OPS_PER_SEC 2>/dev/null || true
    fi
    if [[ -n "$MIXED_QUERY_OPS_PER_SEC" ]]; then
        export QUERY_OPS_PER_SEC="$MIXED_QUERY_OPS_PER_SEC"
    else
        unset QUERY_OPS_PER_SEC 2>/dev/null || true
    fi

    mkdir -p "$RESULT_PATH"
    "$MIXED_BINARY_PATH"

    echo "Mixed logs written to: $RESULT_PATH"
}

start_qdrant
prepare_local_data

if [[ "$TEST_MODE" == "mixed" ]]; then
    build_mixed_binary_if_needed
    run_mixed
else
    build_standard_binary_if_needed
    run_insert
    run_query
fi

echo
echo "Local test completed successfully."
echo "Mode: $TEST_MODE"
echo "Collection: $COLLECTION_NAME"
echo "Insert file: $OUTPUT_DIR/insert_vectors.npy"
echo "Query file: $OUTPUT_DIR/query_vectors.npy"
