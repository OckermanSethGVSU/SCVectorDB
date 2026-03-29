#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_DIR="${RUN_DIR:-$(pwd)}"

export myDIR="${myDIR:-$(basename "$RUN_DIR")}"
export VECTOR_DIM="${VECTOR_DIM:-128}"
export DISTANCE_METRIC="${DISTANCE_METRIC:-COSINE}"
export GPU_INDEX="${GPU_INDEX:-False}"
export QDRANT_EXECUTABLE="${QDRANT_EXECUTABLE:-qdrant}"
export PERF="${PERF:-NONE}"
export RESTORE_DIR="${RESTORE_DIR:-}"
export RESULT_PATH="${RESULT_PATH:-mixed_logs}"
export INSERT_MODE="${INSERT_MODE:-}"
export INSERT_OPS_PER_SEC="${INSERT_OPS_PER_SEC:-}"
export QUERY_MODE="${QUERY_MODE:-}"
export QUERY_OPS_PER_SEC="${QUERY_OPS_PER_SEC:-}"
export INSERT_START_ID="${INSERT_START_ID:-0}"
export COLLECTION_NAME="${COLLECTION_NAME:-singleShard}"
export TOP_K="${TOP_K:-}"
export QUERY_EF_SEARCH="${QUERY_EF_SEARCH:-}"
export INSERT_STREAMING="${INSERT_STREAMING:-}"
export QUERY_STREAMING="${QUERY_STREAMING:-}"
export RPC_TIMEOUT="${RPC_TIMEOUT:-}"
export QDRANT_REGISTRY_PATH="${QDRANT_REGISTRY_PATH:-$RUN_DIR/ip_registry.txt}"
export INSERT_BATCH_MIN="${INSERT_BATCH_MIN:-}"
export INSERT_BATCH_MAX="${INSERT_BATCH_MAX:-}"
export QUERY_BATCH_MIN="${QUERY_BATCH_MIN:-}"
export QUERY_BATCH_MAX="${QUERY_BATCH_MAX:-}"

export TASK="${TASK:-INSERT}"
export RUN_MODE="${RUN_MODE:-local}"
export WORKLOAD_MODE="${WORKLOAD_MODE:-}"
export EXPECTED_CORPUS_SIZE="${EXPECTED_CORPUS_SIZE:-0}"
export N_WORKERS=1

CONTAINER_NAME="${QDRANT_LOCAL_NAME:-qdrant-local}"
IMAGE="${QDRANT_LOCAL_IMAGE:-qdrant/qdrant:latest}"
HOST="${QDRANT_LOCAL_HOST:-127.0.0.1}"
HTTP_PORT="${QDRANT_LOCAL_HTTP_PORT:-6333}"
GRPC_PORT="${QDRANT_LOCAL_GRPC_PORT:-6334}"
P2P_PORT="${QDRANT_LOCAL_P2P_PORT:-6335}"
DATA_DIR="${QDRANT_LOCAL_DATA_DIR:-$RUN_DIR/.local/qdrant/storage}"
CONFIG_DIR="${QDRANT_LOCAL_CONFIG_DIR:-$RUN_DIR/.local/qdrant/config}"
SNAPSHOT_DIR="${QDRANT_LOCAL_SNAPSHOT_DIR:-$RUN_DIR/.local/qdrant/snapshots}"
PERF_DIR="${RUN_DIR}/perf"
STANDARD_BINARY_PATH="${STANDARD_BINARY_PATH:-}"
MIXED_BINARY_PATH="${MIXED_BINARY_PATH:-}"
QUERY_DEBUG_RESULTS="${QUERY_DEBUG_RESULTS:-true}"
LOCAL_RECREATE_COLLECTION="${LOCAL_RECREATE_COLLECTION:-true}"

export QDRANT_HOST="$HOST"
export QDRANT_REST_PORT="$HTTP_PORT"
export QDRANT_GRPC_PORT="$GRPC_PORT"
export QDRANT_URL="http://${HOST}:${GRPC_PORT}"

mkdir -p "$DATA_DIR" "$CONFIG_DIR" "$SNAPSHOT_DIR" "$PERF_DIR"

pick_binary() {
    local override="$1"
    shift

    if [[ -n "$override" ]]; then
        printf '%s\n' "$override"
        return 0
    fi

    local candidate
    for candidate in "$@"; do
        if [[ -x "$candidate" ]]; then
            printf '%s\n' "$candidate"
            return 0
        fi
    done

    printf '%s\n' "$1"
}

STANDARD_BINARY_PATH="$(pick_binary \
    "$STANDARD_BINARY_PATH" \
    "$ROOT_DIR/rustCode/multiClientOP/target/release/multiClientOP" \
    "$ROOT_DIR/rustCode/multiClientOP/target/debug/multiClientOP" \
    "$ROOT_DIR/multiClientOP")"

MIXED_BINARY_PATH="$(pick_binary \
    "$MIXED_BINARY_PATH" \
    "$ROOT_DIR/rustCode/mixedRunner/target/release/mixedrunner" \
    "$ROOT_DIR/rustCode/mixedRunner/target/debug/mixedrunner" \
    "$ROOT_DIR/mixedrunner")"

ensure_runtime_tools() {
    if command -v docker >/dev/null 2>&1; then
        CONTAINER_RUNTIME="docker"
    elif command -v podman >/dev/null 2>&1; then
        CONTAINER_RUNTIME="podman"
    else
        echo "Neither docker nor podman is installed." >&2
        exit 1
    fi

    command -v curl >/dev/null 2>&1 || { echo "curl is required." >&2; exit 1; }
    command -v python3 >/dev/null 2>&1 || { echo "python3 is required." >&2; exit 1; }
}

ensure_binaries() {
    if [[ "$TASK" == "MIXED" || "$WORKLOAD_MODE" == "mixed" ]]; then
        if [[ ! -x "$MIXED_BINARY_PATH" ]]; then
            echo "Missing mixedrunner binary at $MIXED_BINARY_PATH" >&2
            echo "Build it with: (cd $ROOT_DIR/rustCode/mixedRunner && cargo build --release)" >&2
            exit 1
        fi
    else
        if [[ ! -x "$STANDARD_BINARY_PATH" ]]; then
            echo "Missing multiClientOP binary at $STANDARD_BINARY_PATH" >&2
            echo "Build it with: (cd $ROOT_DIR/rustCode/multiClientOP && cargo build --release)" >&2
            exit 1
        fi
    fi
}

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

    printf '0,%s,%s\n' "$HOST" "$P2P_PORT" > "$QDRANT_REGISTRY_PATH"
    rm -f "$PERF_DIR/workflow_start.txt" "$PERF_DIR/workflow_stop.txt"

    echo "Waiting for Qdrant health check on http://${HOST}:${HTTP_PORT}/healthz ..."
    for _ in {1..60}; do
        if curl -fsS "http://${HOST}:${HTTP_PORT}/healthz" >/dev/null; then
            echo "Qdrant is ready."
            return 0
        fi
        sleep 1
    done

    echo "Qdrant did not become healthy within 60 seconds." >&2
    echo "Inspect logs with: ${CONTAINER_RUNTIME} logs ${CONTAINER_NAME}" >&2
    exit 1
}

setup_local_collection() {
    if [[ "$LOCAL_RECREATE_COLLECTION" != "true" ]]; then
        return 0
    fi

    NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" \
        python3 ./configureTopo.py
}

standard_collection_name() {
    printf '%s\n' "singleShard"
}

prepare_cluster_state() {
    if [[ -n "$RESTORE_DIR" ]]; then
        return 0
    fi

    if [[ "$TASK" == "MIXED" || "$WORKLOAD_MODE" == "mixed" ]]; then
        mkdir -p "$RESULT_PATH"
        setup_local_collection
    else
        export COLLECTION_NAME="$(standard_collection_name)"
        setup_local_collection
    fi
}

run_insert() {
    echo "Running local insert workflow..."
    export ACTIVE_TASK="INSERT"
    export COLLECTION_NAME="$(standard_collection_name)"
    export INSERT_CORPUS_SIZE="${INSERT_CORPUS_SIZE:?INSERT_CORPUS_SIZE is required}"
    export INSERT_CLIENTS_PER_WORKER="${INSERT_CLIENTS_PER_WORKER:-1}"
    export INSERT_BATCH_SIZE="${INSERT_BATCH_SIZE:-1}"
    export INSERT_BALANCE_STRATEGY="${INSERT_BALANCE_STRATEGY:-NO_BALANCE}"
    export INSERT_FILEPATH="${INSERT_FILEPATH:?INSERT_FILEPATH is required}"
    export INSERT_STREAMING
    "$STANDARD_BINARY_PATH"
}

run_query() {
    echo "Running local query workflow..."
    export ACTIVE_TASK="QUERY"
    export COLLECTION_NAME="$(standard_collection_name)"
    export QUERY_CORPUS_SIZE="${QUERY_CORPUS_SIZE:?QUERY_CORPUS_SIZE is required}"
    export QUERY_CLIENTS_PER_WORKER="${QUERY_CLIENTS_PER_WORKER:-1}"
    export QUERY_BATCH_SIZE="${QUERY_BATCH_SIZE:-1}"
    export QUERY_BALANCE_STRATEGY="${QUERY_BALANCE_STRATEGY:-NO_BALANCE}"
    export QUERY_FILEPATH="${QUERY_FILEPATH:?QUERY_FILEPATH is required}"
    export QUERY_DEBUG_RESULTS
    export QUERY_STREAMING
    "$STANDARD_BINARY_PATH"
}

run_mixed() {
    echo "Running local mixed insert/query workflow..."
    export RESULT_PATH
    export INSERT_CORPUS_SIZE="${INSERT_CORPUS_SIZE:?INSERT_CORPUS_SIZE is required}"
    export QUERY_CORPUS_SIZE="${QUERY_CORPUS_SIZE:?QUERY_CORPUS_SIZE is required}"
    export INSERT_FILEPATH="${INSERT_FILEPATH:?INSERT_FILEPATH is required}"
    export QUERY_FILEPATH="${QUERY_FILEPATH:?QUERY_FILEPATH is required}"
    export INSERT_CLIENTS_PER_WORKER="${INSERT_CLIENTS_PER_WORKER:-1}"
    export QUERY_CLIENTS_PER_WORKER="${QUERY_CLIENTS_PER_WORKER:-1}"
    export INSERT_BATCH_SIZE="${INSERT_BATCH_SIZE:-1}"
    export QUERY_BATCH_SIZE="${QUERY_BATCH_SIZE:-1}"
    export INSERT_BALANCE_STRATEGY="${INSERT_BALANCE_STRATEGY:-NO_BALANCE}"
    export QUERY_BALANCE_STRATEGY="${QUERY_BALANCE_STRATEGY:-NO_BALANCE}"
    export MIXED_CORPUS_SIZE="${MIXED_CORPUS_SIZE:-$INSERT_CORPUS_SIZE}"
    export MIXED_DATA_FILEPATH="${MIXED_DATA_FILEPATH:-$INSERT_FILEPATH}"
    export MIXED_QUERY_CLIENTS_PER_WORKER="${MIXED_QUERY_CLIENTS_PER_WORKER:-$QUERY_CLIENTS_PER_WORKER}"
    export MIXED_INSERT_CLIENTS_PER_WORKER="${MIXED_INSERT_CLIENTS_PER_WORKER:-$INSERT_CLIENTS_PER_WORKER}"
    export INSERT_MODE="${INSERT_MODE:-}"
    export INSERT_OPS_PER_SEC="${INSERT_OPS_PER_SEC:-}"
    export INSERT_START_ID="${INSERT_START_ID:-0}"
    export QUERY_MODE="${QUERY_MODE:-}"
    export QUERY_OPS_PER_SEC="${QUERY_OPS_PER_SEC:-}"
    export COLLECTION_NAME="${COLLECTION_NAME:-singleShard}"
    export TOP_K="${TOP_K:-}"
    export QUERY_EF_SEARCH="${QUERY_EF_SEARCH:-}"
    export RPC_TIMEOUT="${RPC_TIMEOUT:-}"
    export QDRANT_REGISTRY_PATH="${QDRANT_REGISTRY_PATH:-$RUN_DIR/ip_registry.txt}"
    export INSERT_BATCH_MIN="${INSERT_BATCH_MIN:-}"
    export INSERT_BATCH_MAX="${INSERT_BATCH_MAX:-}"
    export QUERY_BATCH_MIN="${QUERY_BATCH_MIN:-}"
    export QUERY_BATCH_MAX="${QUERY_BATCH_MAX:-}"
    mkdir -p "$RESULT_PATH"
    "$MIXED_BINARY_PATH"
}

summarize_standard_run() {
    python3 ./multi_client_summary.py
    mkdir -p uploadNPY
    shopt -s nullglob
    local npy_files=(./*.npy)
    if (( ${#npy_files[@]} > 0 )); then
        mv "${npy_files[@]}" uploadNPY/
    fi
    shopt -u nullglob
}

finalize_local_run() {
    touch flag.txt "$PERF_DIR/flag.txt"
    mkdir -p systemStats
    shopt -s nullglob
    local system_files=(./*_system_*.csv)
    if (( ${#system_files[@]} > 0 )); then
        mv "${system_files[@]}" systemStats/
    fi
    shopt -u nullglob
}

run_restore_status() {
    NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" \
        python3 ./status.py
}

run_index() {
    NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" \
        python3 ./index.py
}

run_mixed_timeline() {
    local mixed_timeline_metric="dot"
    if [[ "$DISTANCE_METRIC" == "COSINE" ]]; then
        mixed_timeline_metric="cosine"
    elif [[ "$DISTANCE_METRIC" == "L2" ]]; then
        mixed_timeline_metric="l2"
    fi

    local mixed_timeline_args=(
        ./mixed_timeline.py
        --log-dir "$RESULT_PATH"
        --insert-vectors "$MIXED_DATA_FILEPATH"
        --insert-max-rows "$MIXED_CORPUS_SIZE"
        --query-vectors "$QUERY_FILEPATH"
        --query-max-rows "$QUERY_CORPUS_SIZE"
        --metric "$mixed_timeline_metric"
        --insert-id-offset "$INSERT_START_ID"
    )
    if [[ -z "$RESTORE_DIR" ]]; then
        mixed_timeline_args+=(
            --init-vectors "$INSERT_FILEPATH"
            --init-max-rows "$INSERT_CORPUS_SIZE"
        )
    fi

    NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" \
        python3 "${mixed_timeline_args[@]}"
}

main() {
    cd "$RUN_DIR"
    ensure_runtime_tools
    ensure_binaries
    start_qdrant
    prepare_cluster_state

    if [[ -n "$RESTORE_DIR" ]]; then
        run_restore_status
    else
        run_insert

        if [[ "$TASK" == "INSERT" ]]; then
            summarize_standard_run
            finalize_local_run
            return 0
        fi

        summarize_standard_run

        if [[ "$TASK" == "INDEX" ]]; then
            run_index
            finalize_local_run
            return 0
        fi

        if [[ "$TASK" == "QUERY" ]]; then
            run_index
        fi

        if [[ "$TASK" == "MIXED" || "$WORKLOAD_MODE" == "mixed" ]]; then
            run_index
            run_mixed
            run_mixed_timeline
            echo "Mixed logs written to: $RESULT_PATH"
            return 0
        fi
    fi

    if [[ "$TASK" == "QUERY" ]]; then
        run_query
        summarize_standard_run
        finalize_local_run
        return 0
    fi

    if [[ "$TASK" == "MIXED" || "$WORKLOAD_MODE" == "mixed" ]]; then
        run_mixed
        run_mixed_timeline
        echo "Mixed logs written to: $RESULT_PATH"
        return 0
    fi

    if [[ "$TASK" != "INSERT" && "$TASK" != "INDEX" && "$TASK" != "QUERY" && "$TASK" != "MIXED" ]]; then
        echo "Unsupported TASK '$TASK' for local_main.sh" >&2
        exit 1
    fi
}

main "$@"
