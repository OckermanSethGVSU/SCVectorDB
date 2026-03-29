#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HARNESS_DIR="$ROOT_DIR/local_test_harness"
RUN_DIR="${RUN_DIR:-$ROOT_DIR/.local/qdrant_test_run}"
QDRANT_CONTAINER_NAME="${QDRANT_CONTAINER_NAME:-qdrant-local-test}"
QDRANT_IMAGE="${QDRANT_IMAGE:-qdrant/qdrant}"
QDRANT_HOST="${QDRANT_HOST:-127.0.0.1}"
QDRANT_REST_PORT="${QDRANT_REST_PORT:-6333}"
QDRANT_GRPC_PORT="${QDRANT_GRPC_PORT:-6334}"
QDRANT_REGISTRY_PORT="${QDRANT_REGISTRY_PORT:-6335}"
QDRANT_BASE_URL="http://${QDRANT_HOST}:${QDRANT_REST_PORT}"
COLLECTION_NAME="${COLLECTION_NAME:-singleShard}"
VECTOR_DIM="${VECTOR_DIM:-4}"
DISTANCE_METRIC="${DISTANCE_METRIC:-Dot}"
TEST_ROWS="${TEST_ROWS:-8}"
INSERT_BATCH_SIZE="${INSERT_BATCH_SIZE:-3}"
QUERY_BATCH_SIZE="${QUERY_BATCH_SIZE:-3}"
QUERY_TOP_K="${QUERY_TOP_K:-3}"
RUN_QUERY_TEST="${RUN_QUERY_TEST:-true}"
RUN_MULTI_CLIENT_TEST="${RUN_MULTI_CLIENT_TEST:-false}"
MULTI_N_WORKERS="${MULTI_N_WORKERS:-2}"
MULTI_INSERT_CLIENTS_PER_WORKER="${MULTI_INSERT_CLIENTS_PER_WORKER:-2}"
MULTI_QUERY_CLIENTS_PER_WORKER="${MULTI_QUERY_CLIENTS_PER_WORKER:-2}"
STREAMING="${STREAMING:-false}"
RUST_BINARY="${RUST_BINARY:-$ROOT_DIR/rustCode/multiClientOP/multiClientOP}"
TEST_NPY="${TEST_NPY:-$RUN_DIR/test.npy}"
QUERY_NPY="${QUERY_NPY:-$TEST_NPY}"
IP_REGISTRY_PATH="$RUN_DIR/ip_registry.txt"

require_cmd() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "Missing required command: $1" >&2
        exit 1
    fi
}

wait_for_qdrant() {
    local health_url="${QDRANT_BASE_URL}/healthz"
    for _ in $(seq 1 60); do
        if curl -fsS "$health_url" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done

    echo "Timed out waiting for Qdrant at $health_url" >&2
    exit 1
}

cleanup() {
    docker rm -f "$QDRANT_CONTAINER_NAME" >/dev/null 2>&1 || true
}

is_truthy() {
    [[ "$1" == "true" || "$1" == "1" || "$1" == "yes" || "$1" == "TRUE" || "$1" == "True" || "$1" == "YES" ]]
}

create_collection() {
    curl -fsS -X DELETE "${QDRANT_BASE_URL}/collections/${COLLECTION_NAME}" >/dev/null 2>&1 || true
    curl -fsS -X PUT "${QDRANT_BASE_URL}/collections/${COLLECTION_NAME}" \
        -H 'Content-Type: application/json' \
        -d "{
            \"vectors\": {
                \"size\": ${VECTOR_DIM},
                \"distance\": \"${DISTANCE_METRIC}\"
            }
        }" >/dev/null
}

run_insert_client() {
    local n_workers="$1"
    local clients_per_worker="$2"
    (
        cd "$RUN_DIR"
        ACTIVE_TASK=INSERT \
        N_WORKERS="$n_workers" \
        INSERT_CLIENTS_PER_WORKER="$clients_per_worker" \
        INSERT_CORPUS_SIZE="$TEST_ROWS" \
        INSERT_FILEPATH="$TEST_NPY" \
        INSERT_BATCH_SIZE="$INSERT_BATCH_SIZE" \
        INSERT_BALANCE_STRATEGY=NO_BALANCE \
        STREAMING="$STREAMING" \
        "$RUST_BINARY"
    )
}

run_query_client() {
    local n_workers="$1"
    local clients_per_worker="$2"
    (
        cd "$RUN_DIR"
        ACTIVE_TASK=QUERY \
        N_WORKERS="$n_workers" \
        QUERY_CLIENTS_PER_WORKER="$clients_per_worker" \
        QUERY_CORPUS_SIZE="$TEST_ROWS" \
        QUERY_FILEPATH="$QUERY_NPY" \
        QUERY_BATCH_SIZE="$QUERY_BATCH_SIZE" \
        QUERY_BALANCE_STRATEGY=NO_BALANCE \
        QUERY_TOP_K="$QUERY_TOP_K" \
        STREAMING="$STREAMING" \
        "$RUST_BINARY"
    )
}

run_verifiers() {
    python3 "$HARNESS_DIR/verify_qdrant_points.py" \
        --npy "$TEST_NPY" \
        --base-url "$QDRANT_BASE_URL" \
        --collection "$COLLECTION_NAME"

    if is_truthy "$RUN_QUERY_TEST"; then
        python3 "$HARNESS_DIR/verify_query_results.py" \
            --base-npy "$TEST_NPY" \
            --query-npy "$QUERY_NPY" \
            --result-npy "$RUN_DIR/query_result_ids.npy" \
            --metric "$DISTANCE_METRIC" \
            --top-k "$QUERY_TOP_K"
    fi
}

run_scenario() {
    local label="$1"
    local n_workers="$2"
    local insert_clients_per_worker="$3"
    local query_clients_per_worker="$4"

    echo "Running scenario: $label"
    echo "  N_WORKERS=$n_workers"
    echo "  INSERT_CLIENTS_PER_WORKER=$insert_clients_per_worker"
    echo "  QUERY_CLIENTS_PER_WORKER=$query_clients_per_worker"

    create_collection
    run_insert_client "$n_workers" "$insert_clients_per_worker"

    if is_truthy "$RUN_QUERY_TEST"; then
        run_query_client "$n_workers" "$query_clients_per_worker"
    fi

    run_verifiers
}

main() {
    require_cmd docker
    require_cmd curl
    require_cmd python3

    if [[ ! -x "$RUST_BINARY" ]]; then
        echo "Rust binary not found or not executable: $RUST_BINARY" >&2
        echo "Build it first, or set RUST_BINARY to the correct path." >&2
        exit 1
    fi

    cat <<EOF
Local Qdrant harness parameters:
  RUN_DIR=$RUN_DIR
  RUST_BINARY=$RUST_BINARY
  QDRANT_IMAGE=$QDRANT_IMAGE
  QDRANT_HOST=$QDRANT_HOST
  QDRANT_REST_PORT=$QDRANT_REST_PORT
  QDRANT_GRPC_PORT=$QDRANT_GRPC_PORT
  QDRANT_REGISTRY_PORT=$QDRANT_REGISTRY_PORT
  COLLECTION_NAME=$COLLECTION_NAME
  VECTOR_DIM=$VECTOR_DIM
  DISTANCE_METRIC=$DISTANCE_METRIC
  TEST_ROWS=$TEST_ROWS
  INSERT_BATCH_SIZE=$INSERT_BATCH_SIZE
  QUERY_BATCH_SIZE=$QUERY_BATCH_SIZE
  QUERY_TOP_K=$QUERY_TOP_K
  RUN_QUERY_TEST=$RUN_QUERY_TEST
  RUN_MULTI_CLIENT_TEST=$RUN_MULTI_CLIENT_TEST
  MULTI_N_WORKERS=$MULTI_N_WORKERS
  MULTI_INSERT_CLIENTS_PER_WORKER=$MULTI_INSERT_CLIENTS_PER_WORKER
  MULTI_QUERY_CLIENTS_PER_WORKER=$MULTI_QUERY_CLIENTS_PER_WORKER
  STREAMING=$STREAMING
  TEST_NPY=$TEST_NPY
  QUERY_NPY=$QUERY_NPY
EOF

    mkdir -p "$RUN_DIR" "$RUN_DIR/perf"
    trap cleanup EXIT

    cleanup
    docker run --rm -d \
        --name "$QDRANT_CONTAINER_NAME" \
        -p "${QDRANT_REST_PORT}:6333" \
        -p "${QDRANT_GRPC_PORT}:6334" \
        "$QDRANT_IMAGE" >/dev/null

    wait_for_qdrant
    create_collection

    python3 "$HARNESS_DIR/gen_test_npy.py" \
        --output "$TEST_NPY" \
        --rows "$TEST_ROWS" \
        --dim "$VECTOR_DIM"

    # multiClientOP subtracts 1 from the registry port before building the client URL,
    # so the registry must advertise gRPC+1 in order to target the actual gRPC port.
    printf '0,%s,%s\n' "$QDRANT_HOST" "$QDRANT_REGISTRY_PORT" > "$IP_REGISTRY_PATH"

    run_scenario "single-client" 1 1 1

    if is_truthy "$RUN_MULTI_CLIENT_TEST"; then
        run_scenario \
            "multi-client" \
            "$MULTI_N_WORKERS" \
            "$MULTI_INSERT_CLIENTS_PER_WORKER" \
            "$MULTI_QUERY_CLIENTS_PER_WORKER"
    fi

    echo "Local Qdrant integration test completed successfully."
}

main "$@"
