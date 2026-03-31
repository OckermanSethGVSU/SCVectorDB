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
export MINIO_MODE="${MINIO_MODE:-off}"
export MINIO_MEDIUM="${MINIO_MEDIUM:-lustre}"

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
MINIO_CONTAINER_NAME="${MINIO_LOCAL_NAME:-milvus-minio}"
MINIO_IMAGE="${MINIO_LOCAL_IMAGE:-minio/minio:RELEASE.2025-02-28T09-55-16Z}"
MINIO_API_PORT="${MINIO_API_PORT:-9000}"
MINIO_CONSOLE_PORT="${MINIO_CONSOLE_PORT:-9001}"
MINIO_HOST="${MINIO_HOST:-127.0.0.1}"
MINIO_INTERNAL_HOST="${MINIO_INTERNAL_HOST:-$MINIO_CONTAINER_NAME}"
MINIO_BUCKET_NAME="${MINIO_BUCKET_NAME:-a-bucket}"
MINIO_ACCESS_KEY_ID="${MINIO_ACCESS_KEY_ID:-minioadmin}"
MINIO_SECRET_ACCESS_KEY="${MINIO_SECRET_ACCESS_KEY:-minioadmin}"
MINIO_NETWORK_NAME="${MINIO_NETWORK_NAME:-milvus-local-net}"
MINIO_VOLUMES_DIR="${MINIO_LOCAL_VOLUME_DIR:-$RUN_DIR/volumes/minio}"
EMBED_ETCD_FILE="$RUN_DIR/embedEtcd.yaml"
USER_CONFIG_FILE="$RUN_DIR/user.yaml"
STANDARD_BINARY_PATH="${STANDARD_BINARY_PATH:-}"
MIXED_BINARY_PATH="${MIXED_BINARY_PATH:-}"

mkdir -p "$RUN_DIR" "$RUN_DIR/workerOut" "$VOLUMES_DIR" "$MINIO_VOLUMES_DIR"

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

MIXED_BINARY_PATH="$(pick_binary \
    "$MIXED_BINARY_PATH" \
    "$ROOT_DIR/goCode/mixedRunner/mixedRunner" \
    "$ROOT_DIR/mixedRunner")"

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

    if [[ "$TASK" == "MIXED" ]]; then
        if [[ ! -x "$MIXED_BINARY_PATH" ]]; then
            echo "Missing mixedrunner binary at $MIXED_BINARY_PATH" >&2
            exit 1
        fi
    else
        if [[ ! -x "$STANDARD_BINARY_PATH" ]]; then
            echo "Missing multiClientOP binary at $STANDARD_BINARY_PATH" >&2
            exit 1
        fi
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

    if [[ "$MINIO_MODE" == "single" ]]; then
        printf '0,%s,%s\n' "$MINIO_HOST" "$MINIO_API_PORT" > "$RUN_DIR/minio_registry.txt"
    else
        rm -f "$RUN_DIR/minio_registry.txt"
    fi
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

ensure_container_network() {
    if ! "$CONTAINER_RUNTIME" network inspect "$MINIO_NETWORK_NAME" >/dev/null 2>&1; then
        "$CONTAINER_RUNTIME" network create "$MINIO_NETWORK_NAME" >/dev/null
    fi
}

container_exists() {
    local name="$1"
    "$CONTAINER_RUNTIME" ps -a --format '{{.Names}}' | grep -Fxq "$name"
}

container_running() {
    local name="$1"
    "$CONTAINER_RUNTIME" ps --format '{{.Names}}' | grep -Fxq "$name"
}

remove_container_if_present() {
    local name="$1"

    if container_running "$name"; then
        "$CONTAINER_RUNTIME" stop "$name" >/dev/null
    fi

    if container_exists "$name"; then
        "$CONTAINER_RUNTIME" rm "$name" >/dev/null
    fi
}

wait_for_minio() {
    echo "Waiting for MinIO at ${MINIO_HOST}:${MINIO_API_PORT}..."
    for _ in {1..120}; do
        if env "${PYTHON_ENV_VARS[@]}" curl -fsS "http://${MINIO_HOST}:${MINIO_API_PORT}/minio/health/live" >/dev/null 2>&1; then
            echo "MinIO is ready."
            return 0
        fi
        sleep 1
    done

    echo "MinIO did not become healthy within 120 seconds." >&2
    echo "Inspect logs with: ${CONTAINER_RUNTIME} logs ${MINIO_CONTAINER_NAME}" >&2
    exit 1
}

start_local_minio() {
    case "$MINIO_MODE" in
        off)
            return 0
            ;;
        single)
            ;;
        *)
            echo "Local mode only supports MINIO_MODE='off' or 'single' (got '${MINIO_MODE}')." >&2
            exit 1
            ;;
    esac

    ensure_container_network

    if container_exists "$MINIO_CONTAINER_NAME"; then
        local minio_port_bindings
        minio_port_bindings="$("$CONTAINER_RUNTIME" inspect "$MINIO_CONTAINER_NAME" --format '{{json .HostConfig.PortBindings}}')"

        if [[ "$minio_port_bindings" != *"9000/tcp"* ]]; then
            echo "Recreating MinIO container '$MINIO_CONTAINER_NAME' to publish host ports."
            remove_container_if_present "$MINIO_CONTAINER_NAME"
        fi
    fi

    if container_running "$MINIO_CONTAINER_NAME"; then
        echo "MinIO container '$MINIO_CONTAINER_NAME' is already running."
    else
        if container_exists "$MINIO_CONTAINER_NAME"; then
            echo "Starting existing MinIO container '$MINIO_CONTAINER_NAME'..."
            "$CONTAINER_RUNTIME" start "$MINIO_CONTAINER_NAME" >/dev/null
        else
            echo "Launching MinIO container '$MINIO_CONTAINER_NAME' from image '$MINIO_IMAGE'..."
            "$CONTAINER_RUNTIME" run -d \
                --name "$MINIO_CONTAINER_NAME" \
                --network "$MINIO_NETWORK_NAME" \
                -e MINIO_ROOT_USER="$MINIO_ACCESS_KEY_ID" \
                -e MINIO_ROOT_PASSWORD="$MINIO_SECRET_ACCESS_KEY" \
                -v "${MINIO_VOLUMES_DIR}:/data" \
                -p "${MINIO_API_PORT}:9000" \
                -p "${MINIO_CONSOLE_PORT}:9001" \
                --health-cmd="curl -f http://localhost:9000/minio/health/live" \
                --health-interval=15s \
                --health-start-period=10s \
                --health-timeout=5s \
                --health-retries=5 \
                "$MINIO_IMAGE" server /data --console-address ":9001" >/dev/null
        fi
    fi

    wait_for_minio
}

start_local_milvus() {
    local storage_type="local"
    local -a minio_env_args=()

    write_local_configs

    case "$MINIO_MODE" in
        off)
            ;;
        single)
            storage_type="remote"
            minio_env_args=(
                -e MINIO_ADDRESS="${MINIO_INTERNAL_HOST}:9000"
                -e MINIO_ACCESS_KEY_ID="${MINIO_ACCESS_KEY_ID}"
                -e MINIO_SECRET_ACCESS_KEY="${MINIO_SECRET_ACCESS_KEY}"
                -e MINIO_BUCKET_NAME="${MINIO_BUCKET_NAME}"
            )
            ensure_container_network
            ;;
        *)
            echo "Local mode only supports MINIO_MODE='off' or 'single' (got '${MINIO_MODE}')." >&2
            exit 1
            ;;
    esac

    if container_exists "$CONTAINER_NAME"; then
        local current_network
        local current_env
        current_network="$("$CONTAINER_RUNTIME" inspect "$CONTAINER_NAME" --format '{{.HostConfig.NetworkMode}}')"
        current_env="$("$CONTAINER_RUNTIME" inspect "$CONTAINER_NAME" --format '{{join .Config.Env "\n"}}')"

        if [[ "$current_network" != "$MINIO_NETWORK_NAME" ]]; then
            echo "Recreating Milvus container '$CONTAINER_NAME' to attach it to network '$MINIO_NETWORK_NAME'."
            remove_container_if_present "$CONTAINER_NAME"
        elif [[ "$storage_type" == "remote" && "$current_env" != *"COMMON_STORAGETYPE=remote"* ]]; then
            echo "Recreating Milvus container '$CONTAINER_NAME' to enable remote object storage."
            remove_container_if_present "$CONTAINER_NAME"
        elif [[ "$storage_type" == "local" && "$current_env" != *"COMMON_STORAGETYPE=local"* ]]; then
            echo "Recreating Milvus container '$CONTAINER_NAME' to restore local object storage."
            remove_container_if_present "$CONTAINER_NAME"
        fi
    fi

    if container_running "$CONTAINER_NAME"; then
        echo "Milvus container '$CONTAINER_NAME' is already running."
    else
        if container_exists "$CONTAINER_NAME"; then
            echo "Starting existing Milvus container '$CONTAINER_NAME'..."
            "$CONTAINER_RUNTIME" start "$CONTAINER_NAME" >/dev/null
        else
            echo "Launching Milvus container '$CONTAINER_NAME' from image '$IMAGE'..."
            "$CONTAINER_RUNTIME" run -d \
                --name "$CONTAINER_NAME" \
                --security-opt seccomp:unconfined \
                --network "$MINIO_NETWORK_NAME" \
                -e ETCD_USE_EMBED=true \
                -e ETCD_DATA_DIR=/var/lib/milvus/etcd \
                -e ETCD_CONFIG_PATH=/milvus/configs/embedEtcd.yaml \
                -e COMMON_STORAGETYPE="${storage_type}" \
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
                "${minio_env_args[@]}" \
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
	export INSERT_STREAMING="${INSERT_STREAMING:-}"

	env GOGC="${LOCAL_INSERT_GOGC:-25}" "${PYTHON_ENV_VARS[@]}" "$STANDARD_BINARY_PATH"
}

run_bulk_upload() {
    export ACTIVE_TASK="IMPORT"
    export INSERT_CORPUS_SIZE="${INSERT_CORPUS_SIZE:?INSERT_CORPUS_SIZE is required}"
    export INSERT_BATCH_SIZE="${INSERT_BATCH_SIZE:?INSERT_BATCH_SIZE is required}"
    export INSERT_STREAMING="${INSERT_STREAMING:-}"
    export IMPORT_PROCESSES="${IMPORT_PROCESSES:-${INSERT_CLIENTS_PER_PROXY:-1}}"
    export COLLECTION_NAME="${COLLECTION_NAME:-standalone}"
    export VECTOR_FIELD="${VECTOR_FIELD:-vector}"
    export ID_FIELD="${ID_FIELD:-id}"
    export MINIO_ENDPOINT="${MINIO_ENDPOINT:-${MINIO_HOST}:${MINIO_API_PORT}}"
    local bulk_request_args=()

    if [[ -n "${BULK_IMPORT_LOAD_REQUEST:-}" ]]; then
        bulk_request_args+=(--load-import-request "$BULK_IMPORT_LOAD_REQUEST")
    else
        export INSERT_DATA_FILEPATH="${INSERT_DATA_FILEPATH:?INSERT_DATA_FILEPATH is required}"
        bulk_request_args+=(--input "$INSERT_DATA_FILEPATH")
    fi

    if [[ -n "${BULK_IMPORT_REQUEST_PATH:-}" ]]; then
        bulk_request_args+=(--import-request-path "$BULK_IMPORT_REQUEST_PATH")
    fi

    if [[ "${BULK_IMPORT_PREPARE_ONLY:-}" =~ ^(1|true|TRUE|yes|YES|on|ON)$ ]]; then
        bulk_request_args+=(--prepare-only)
    fi

    if [[ "${MINIO_MODE}" != "single" ]]; then
        echo "TASK=IMPORT requires MINIO_MODE=single in local mode." >&2
        exit 1
    fi

    env "${PYTHON_ENV_VARS[@]}" python3 ./bulk_upload_import.py \
        --writer-mode remote \
        --processes "$IMPORT_PROCESSES" \
        --corpus-size "$INSERT_CORPUS_SIZE" \
        --collection "$COLLECTION_NAME" \
        --vector-field "$VECTOR_FIELD" \
        --id-field "$ID_FIELD" \
        --vector-dim "$VECTOR_DIM" \
        --batch-rows "$INSERT_BATCH_SIZE" \
        "${bulk_request_args[@]}"
}

run_index() {
    export ACTIVE_TASK="INDEX"
    touch ./workerOut/workflow_start.txt
    env "${PYTHON_ENV_VARS[@]}" python3 ./index.py
}

run_query() {
    export ACTIVE_TASK="QUERY"
    export QUERY_BALANCE_STRATEGY="${QUERY_BALANCE_STRATEGY:?QUERY_BALANCE_STRATEGY is required}"
    export QUERY_CORPUS_SIZE="${QUERY_CORPUS_SIZE:?QUERY_CORPUS_SIZE is required}"
    export QUERY_CLIENTS_PER_PROXY="${QUERY_CLIENTS_PER_PROXY:?QUERY_CLIENTS_PER_PROXY is required}"
    export QUERY_DATA_FILEPATH="${QUERY_DATA_FILEPATH:?QUERY_DATA_FILEPATH is required}"
    export QUERY_BATCH_SIZE="${QUERY_BATCH_SIZE:?QUERY_BATCH_SIZE is required}"
    export QUERY_STREAMING="${QUERY_STREAMING:-}"

    env "${PYTHON_ENV_VARS[@]}" "$STANDARD_BINARY_PATH"
}

run_mixed() {
    export ACTIVE_TASK="MIXED"
    export MIXED_RESULT_PATH="${MIXED_RESULT_PATH:-mixed_logs}"
    export INSERT_DATA_FILEPATH="${INSERT_DATA_FILEPATH:?INSERT_DATA_FILEPATH is required}"
    export INSERT_CORPUS_SIZE="${INSERT_CORPUS_SIZE:?INSERT_CORPUS_SIZE is required}"
    export QUERY_DATA_FILEPATH="${QUERY_DATA_FILEPATH:?QUERY_DATA_FILEPATH is required}"
    export QUERY_CORPUS_SIZE="${QUERY_CORPUS_SIZE:?QUERY_CORPUS_SIZE is required}"
    export INSERT_BATCH_SIZE="${INSERT_BATCH_SIZE:?INSERT_BATCH_SIZE is required}"
    export QUERY_BATCH_SIZE="${QUERY_BATCH_SIZE:?QUERY_BATCH_SIZE is required}"
    export INSERT_STREAMING="${INSERT_STREAMING:-}"
    export QUERY_STREAMING="${QUERY_STREAMING:-}"
    export INSERT_BALANCE_STRATEGY="${INSERT_BALANCE_STRATEGY:?INSERT_BALANCE_STRATEGY is required}"
    export QUERY_BALANCE_STRATEGY="${QUERY_BALANCE_STRATEGY:?QUERY_BALANCE_STRATEGY is required}"
    export INSERT_MODE="${INSERT_MODE:-max}"
    export INSERT_OPS_PER_SEC="${INSERT_OPS_PER_SEC:-}"
    export QUERY_MODE="${QUERY_MODE:-max}"
    export QUERY_OPS_PER_SEC="${QUERY_OPS_PER_SEC:-}"
    export INSERT_CLIENTS="${MIXED_INSERT_CLIENTS_PER_PROXY:-$INSERT_CLIENTS_PER_PROXY}"
    export QUERY_CLIENTS="${MIXED_QUERY_CLIENTS_PER_PROXY:-$QUERY_CLIENTS_PER_PROXY}"
    export MIXED_QUERY_CLIENTS_PER_PROXY="${MIXED_QUERY_CLIENTS_PER_PROXY:-}"
    export MIXED_INSERT_CLIENTS_PER_PROXY="${MIXED_INSERT_CLIENTS_PER_PROXY:-}"
    export MIXED_CORPUS_SIZE="${MIXED_CORPUS_SIZE:-$INSERT_CORPUS_SIZE}"
    export MIXED_DATA_FILEPATH="${MIXED_DATA_FILEPATH:-$INSERT_DATA_FILEPATH}"
    export COLLECTION_NAME="${COLLECTION_NAME:-standalone}"
    export VECTOR_FIELD="${VECTOR_FIELD:-vector}"
    export ID_FIELD="${ID_FIELD:-id}"
    export TOP_K="${TOP_K:-10}"
    export QUERY_EF_SEARCH="${QUERY_EF_SEARCH:-64}"
    export EFSearch="${EFSearch:-$QUERY_EF_SEARCH}"
    export SEARCH_CONSISTENCY="${SEARCH_CONSISTENCY:-bounded}"
    export RPC_TIMEOUT="${RPC_TIMEOUT:-10m}"
    export INSERT_BATCH_MIN="${INSERT_BATCH_MIN:-}"
    export INSERT_BATCH_MAX="${INSERT_BATCH_MAX:-}"
    export QUERY_BATCH_MIN="${QUERY_BATCH_MIN:-}"
    export QUERY_BATCH_MAX="${QUERY_BATCH_MAX:-}"
    export INSERT_START_ID="${INSERT_START_ID:?INSERT_START_ID is required}"

    mkdir -p "$MIXED_RESULT_PATH"
    env "${PYTHON_ENV_VARS[@]}" "$MIXED_BINARY_PATH"
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
        --log-dir "$MIXED_RESULT_PATH"
        --insert-vectors "$MIXED_DATA_FILEPATH"
        --insert-max-rows "$MIXED_CORPUS_SIZE"
        --query-vectors "$QUERY_DATA_FILEPATH"
        --query-max-rows "$QUERY_CORPUS_SIZE"
        --metric "$mixed_timeline_metric"
        --insert-id-offset "$INSERT_START_ID"
    )

    if [[ -z "$RESTORE_DIR" ]]; then
        mixed_timeline_args+=(
            --init-vectors "$INSERT_DATA_FILEPATH"
            --init-max-rows "$INSERT_CORPUS_SIZE"
        )
    fi

    env "${PYTHON_ENV_VARS[@]}" python3 "${mixed_timeline_args[@]}"
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
    start_local_minio
    start_local_milvus

    if [[ -z "$RESTORE_DIR" ]]; then
        run_setup_collection

        if [[ "$TASK" == "IMPORT" ]]; then
            run_bulk_upload
            return 0
        fi

        run_insert

        if [[ "$TASK" == "INSERT" ]]; then
            touch flag.txt
        fi

        summarize_insert

        if [[ "$TASK" == "INDEX" || "$TASK" == "QUERY" || "$TASK" == "MIXED" ]]; then
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

    if [[ "$TASK" == "MIXED" ]]; then
        run_mixed
        run_mixed_timeline
        return 0
    fi

    if [[ "$TASK" != "INSERT" && "$TASK" != "IMPORT" && "$TASK" != "INDEX" && "$TASK" != "QUERY" && "$TASK" != "MIXED" ]]; then
        echo "Unsupported TASK '$TASK' for local_main.sh" >&2
        exit 1
    fi
}

main "$@"
