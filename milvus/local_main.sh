#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_DIR="${RUN_DIR:-$(pwd)}"

PYTHON_ENV_VARS=(
    NO_PROXY=""
    no_proxy=""
    http_proxy=""
    https_proxy=""
    HTTP_PROXY=""
    HTTPS_PROXY=""
)

export BASE_DIR="${BASE_DIR:-$(dirname "$RUN_DIR")}"
export myDIR="${myDIR:-$(basename "$RUN_DIR")}"
export RESULT_PATH="${RESULT_PATH:-$RUN_DIR}"

export PLATFORM="${PLATFORM:-LOCAL}"
export CORES="${CORES:-1}"
export MODE="${MODE:-STANDALONE}"
export TASK="${TASK:-INSERT}"
export WAL="${WAL:-woodpecker}"
export DML_CHANNELS="${DML_CHANNELS:-16}"

export NUM_PROXIES="${NUM_PROXIES:-1}"
export NUM_PROXIES_PER_CN="${NUM_PROXIES_PER_CN:-1}"

export GPU_INDEX="${GPU_INDEX:-False}"
export VECTOR_DIM="${VECTOR_DIM:-2560}"
export DISTANCE_METRIC="${DISTANCE_METRIC:-COSINE}"
export INIT_FLAT_INDEX="${INIT_FLAT_INDEX:-TRUE}"

export TRACING="${TRACING:-False}"
export PERF="${PERF:-NONE}"
export DEBUG="${DEBUG:-False}"
export RESTORE_DIR="${RESTORE_DIR:-}"
export EXPECTED_CORPUS_SIZE="${EXPECTED_CORPUS_SIZE:-0}"

export MILVUS_HOST="${MILVUS_HOST:-127.0.0.1}"
export MILVUS_GRPC_PORT="${MILVUS_GRPC_PORT:-19530}"
export MILVUS_HEALTH_PORT="${MILVUS_HEALTH_PORT:-9091}"
export MILVUS_TOKEN="${MILVUS_TOKEN:-root:Milvus}"

CONTAINER_NAME="${MILVUS_LOCAL_NAME:-milvus-standalone}"
IMAGE="${MILVUS_LOCAL_IMAGE:-milvusdb/milvus:v2.6.12}"
ETCD_PORT="${MILVUS_ETCD_PORT:-2379}"
VOLUMES_DIR="${MILVUS_LOCAL_VOLUME_DIR:-$RUN_DIR/volumes/milvus}"
EMBED_ETCD_FILE="$RUN_DIR/embedEtcd.yaml"
USER_CONFIG_FILE="$RUN_DIR/user.yaml"
STANDARD_BINARY_PATH="${STANDARD_BINARY_PATH:-}"

mkdir -p "$RUN_DIR" "$RESULT_PATH" "$RUN_DIR/workerOut" "$VOLUMES_DIR"

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
    "$ROOT_DIR/goCode/multiClientOP/multiClientOP" \
    "$ROOT_DIR/multiClientOP")"

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

    if [[ ! -x "$STANDARD_BINARY_PATH" ]]; then
        echo "Missing multiClientOP binary at $STANDARD_BINARY_PATH" >&2
        exit 1
    fi
}

write_local_configs() {
    cat > "$EMBED_ETCD_FILE" <<'EOF'
listen-client-urls: http://0.0.0.0:2379
advertise-client-urls: http://0.0.0.0:2379
quota-backend-bytes: 4294967296
auto-compaction-mode: revision
auto-compaction-retention: '1000'
EOF

    cat > "$USER_CONFIG_FILE" <<'EOF'
# Extra config to override default milvus.yaml
EOF
}

write_registry_files() {
    printf '%s\n' "$MILVUS_HOST" > "$RUN_DIR/worker.ip"
    printf '0,%s,%s,%s\n' "$MILVUS_HOST" "$MILVUS_GRPC_PORT" "$MILVUS_HEALTH_PORT" > "$RUN_DIR/PROXY_registry.txt"
}

wait_for_milvus() {
    echo "Waiting for Milvus at ${MILVUS_HOST}:${MILVUS_HEALTH_PORT}..."
    for _ in {1..180}; do
        if env "${PYTHON_ENV_VARS[@]}" curl -fsS "http://${MILVUS_HOST}:${MILVUS_HEALTH_PORT}/healthz" >/dev/null 2>&1; then
            echo "Milvus is ready."
            return 0
        fi
        sleep 1
    done

    echo "Milvus did not become healthy within 180 seconds." >&2
    echo "Inspect logs with: ${CONTAINER_RUNTIME} logs ${CONTAINER_NAME}" >&2
    exit 1
}

start_local_milvus() {
    write_local_configs

    if "$CONTAINER_RUNTIME" ps --format '{{.Names}}' | grep -Fxq "$CONTAINER_NAME"; then
        echo "Milvus container '$CONTAINER_NAME' is already running."
    else
        if "$CONTAINER_RUNTIME" ps -a --format '{{.Names}}' | grep -Fxq "$CONTAINER_NAME"; then
            echo "Starting existing Milvus container '$CONTAINER_NAME'..."
            "$CONTAINER_RUNTIME" start "$CONTAINER_NAME" >/dev/null
        else
            echo "Launching Milvus container '$CONTAINER_NAME' from image '$IMAGE'..."
            "$CONTAINER_RUNTIME" run -d \
                --name "$CONTAINER_NAME" \
                --security-opt seccomp:unconfined \
                -e ETCD_USE_EMBED=true \
                -e ETCD_DATA_DIR=/var/lib/milvus/etcd \
                -e ETCD_CONFIG_PATH=/milvus/configs/embedEtcd.yaml \
                -e COMMON_STORAGETYPE=local \
                -e DEPLOY_MODE=STANDALONE \
                -v "${VOLUMES_DIR}:/var/lib/milvus" \
                -v "${EMBED_ETCD_FILE}:/milvus/configs/embedEtcd.yaml" \
                -v "${USER_CONFIG_FILE}:/milvus/configs/user.yaml" \
                -p "${MILVUS_GRPC_PORT}:19530" \
                -p "${MILVUS_HEALTH_PORT}:9091" \
                -p "${ETCD_PORT}:2379" \
                --health-cmd="curl -f http://localhost:9091/healthz" \
                --health-interval=30s \
                --health-start-period=90s \
                --health-timeout=20s \
                --health-retries=3 \
                "$IMAGE" milvus run standalone >/dev/null
        fi
    fi

    write_registry_files
    wait_for_milvus
}

run_setup_collection() {
    env "${PYTHON_ENV_VARS[@]}" python3 ./setup_collection.py
}

run_insert() {
	export ACTIVE_TASK="INSERT"
	export INSERT_BALANCE_STRATEGY="${INSERT_BALANCE_STRATEGY:?INSERT_BALANCE_STRATEGY is required}"
	export INSERT_CORPUS_SIZE="${INSERT_CORPUS_SIZE:?INSERT_CORPUS_SIZE is required}"
	export INSERT_CLIENTS_PER_PROXY="${INSERT_CLIENTS_PER_PROXY:?INSERT_CLIENTS_PER_PROXY is required}"
	export INSERT_DATA_FILEPATH="${INSERT_DATA_FILEPATH:?INSERT_DATA_FILEPATH is required}"
	export INSERT_BATCH_SIZE="${INSERT_BATCH_SIZE:?INSERT_BATCH_SIZE is required}"

	env GOGC="${LOCAL_INSERT_GOGC:-25}" "${PYTHON_ENV_VARS[@]}" "$STANDARD_BINARY_PATH"
}

run_index() {
    export ACTIVE_TASK="INDEX"
    touch ./workerOut/workflow_start.txt
    env "${PYTHON_ENV_VARS[@]}" python3 ./index_data.py
}

run_query() {
    export ACTIVE_TASK="QUERY"
    export QUERY_BALANCE_STRATEGY="${QUERY_BALANCE_STRATEGY:?QUERY_BALANCE_STRATEGY is required}"
    export QUERY_CORPUS_SIZE="${QUERY_CORPUS_SIZE:?QUERY_CORPUS_SIZE is required}"
    export QUERY_CLIENTS_PER_PROXY="${QUERY_CLIENTS_PER_PROXY:?QUERY_CLIENTS_PER_PROXY is required}"
    export QUERY_DATA_FILEPATH="${QUERY_DATA_FILEPATH:?QUERY_DATA_FILEPATH is required}"
    export QUERY_BATCH_SIZE="${QUERY_BATCH_SIZE:?QUERY_BATCH_SIZE is required}"

    env "${PYTHON_ENV_VARS[@]}" "$STANDARD_BINARY_PATH"
}

summarize_insert() {
    env "${PYTHON_ENV_VARS[@]}" python3 ./multi_client_summary.py
    [[ -f times.csv ]] && mv times.csv insert_times.txt
    [[ -f summary.csv ]] && mv summary.csv insert_summary.txt
    mkdir -p uploadNPY
    shopt -s nullglob
    local files=(./*.npy)
    if (( ${#files[@]} > 0 )); then
        mv "${files[@]}" uploadNPY/
    fi
    shopt -u nullglob
}

summarize_query() {
    env "${PYTHON_ENV_VARS[@]}" python3 ./multi_client_summary.py
    [[ -f times.csv ]] && mv times.csv query_times.txt
    [[ -f summary.csv ]] && mv summary.csv query_summary.txt
    mkdir -p queryNPY
    shopt -s nullglob
    local files=(./*.npy)
    if (( ${#files[@]} > 0 )); then
        mv "${files[@]}" queryNPY/
    fi
    shopt -u nullglob
}

run_restore_status() {
    export EXPECTED_CORPUS_SIZE
    env "${PYTHON_ENV_VARS[@]}" python3 ./status.py
}

main() {
    cd "$RUN_DIR"
    ensure_runtime_tools
    start_local_milvus

    if [[ -z "$RESTORE_DIR" ]]; then
        run_setup_collection
        run_insert

        if [[ "$TASK" == "INSERT" ]]; then
            touch flag.txt
        fi

        summarize_insert

        if [[ "$TASK" == "INDEX" || "$TASK" == "QUERY" ]]; then
            run_index

            if [[ "$TASK" == "INDEX" ]]; then
                touch ./workerOut/workflow_end.txt
                touch flag.txt
                return 0
            fi
        fi

        sleep 5
    else
        run_restore_status
    fi

    if [[ "$TASK" == "QUERY" ]]; then
        run_query
        summarize_query
        return 0
    fi

    if [[ "$TASK" != "INSERT" && "$TASK" != "INDEX" && "$TASK" != "QUERY" ]]; then
        echo "Unsupported TASK '$TASK' for local_main.sh" >&2
        exit 1
    fi
}

main "$@"
