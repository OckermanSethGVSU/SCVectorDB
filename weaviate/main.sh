#!/bin/bash
# Weaviate launch script.
#
# Boots the Weaviate cluster via mpirun + launchWeaviateNode.sh, waits for all
# worker ranks to register in ip_registry.txt, creates the collection with
# cross-worker schema consensus, and dispatches on TASK:
#
#   query_scaling                     — three-phase pipeline:
#       1. insert  — drive go_client/$INSERT_BIN with
#                    TOTAL*UPLOAD_CLIENTS_PER_WORKER clients
#       2. index   — wait for object count, then index quiescence
#                    (queue==0 && all shards READY for N stable polls)
#       3. query   — fan out go_client/$QUERY_SCALING_BIN per client per worker
#                    with partitioned query ranges; aggregate into
#                    query_scaling_summary.json
#
#   insert | index | query_bs | query_core
#       Exec go_client/$WEAVIATE_CLIENT_BINARY against rank-0's HTTP endpoint
#       (behavior is fully driven by that binary's own env/flag contract).
set -euo pipefail
export ZSH_EVAL_CONTEXT="${ZSH_EVAL_CONTEXT:-}"

if [[ -f ./run_config.env ]]; then
    set -a
    source ./run_config.env
    set +a
fi

# ---------------------------------------------------------------
# Env vars surfaced by the unified schema (set via run_config.env).
# Repeating them here makes defaults explicit and the script runnable
# from a checked-out run directory without the submit manager.
# ---------------------------------------------------------------
export TASK="${TASK:-query_scaling}"
export PLATFORM="${PLATFORM:-}"
export myDIR="${myDIR:-}"

export VECTOR_DIM="${VECTOR_DIM}"
export DISTANCE_METRIC="${DISTANCE_METRIC}"
export GPU_INDEX="${GPU_INDEX:-false}"

export CORES="${CORES}"
export STORAGE_MEDIUM="${STORAGE_MEDIUM}"
export USEPERF="${USEPERF}"
export CLASS_NAME="${CLASS_NAME:-}"
export DATA_FILEPATH="${DATA_FILEPATH:-}"
export QUERY_FILEPATH="${QUERY_FILEPATH:-}"
export CORPUS_SIZE="${CORPUS_SIZE:-0}"

export QUERY_WORKLOAD="${QUERY_WORKLOAD:-0}"
export QUERY_TOPK="${QUERY_TOPK:-10}"
export QUERY_EF="${QUERY_EF:-64}"
export QUERY_BATCH_SIZE="${QUERY_BATCH_SIZE:-32}"
export QUERY_CLIENTS_PER_WORKER="${QUERY_CLIENTS_PER_WORKER:-1}"
export QUERY_CLIENT_MODE="${QUERY_CLIENT_MODE:-per_worker}"

export UPLOAD_BATCH_SIZE="${UPLOAD_BATCH_SIZE:-128}"
export UPLOAD_CLIENTS_PER_WORKER="${UPLOAD_CLIENTS_PER_WORKER:-4}"
export UPLOAD_BALANCE_STRATEGY="${UPLOAD_BALANCE_STRATEGY:-WORKER_BALANCE}"

# Operational defaults (not user-facing experiment dials — live here, not in schema.sh).
export INSERT_MODE="${INSERT_MODE:-max}"
export INSERT_OPS_PER_SEC="${INSERT_OPS_PER_SEC:-0}"
export RPC_TIMEOUT="${RPC_TIMEOUT:-30m}"
export WAIT_SEC="${WAIT_SEC:-300}"
export OVERALL_SEC="${OVERALL_SEC:-25000}"
export DRAIN_TIMEOUT_SEC="${DRAIN_TIMEOUT_SEC:-7200}"
export DRAIN_POLL_MS="${DRAIN_POLL_MS:-1000}"
export DRAIN_STABLE_POLLS="${DRAIN_STABLE_POLLS:-3}"
export DRAIN_PLATEAU_POLLS="${DRAIN_PLATEAU_POLLS:-60}"
export DRAIN_LOG_EVERY_POLLS="${DRAIN_LOG_EVERY_POLLS:-30}"
export INSERT_MAX_RETRIES="${INSERT_MAX_RETRIES:-10}"
export INSERT_RETRY_BACKOFF_MS="${INSERT_RETRY_BACKOFF_MS:-2000}"
export WORKER_REGISTRY_PATH="${WORKER_REGISTRY_PATH:-ip_registry.txt}"
export RESULT_PATH="${RESULT_PATH:-$(pwd)}"
export RESET_CLASS="${RESET_CLASS:-true}"
export DYNAMIC_THRESHOLD="${DYNAMIC_THRESHOLD:-10000}"
export START_ROW="${START_ROW:-0}"
export INSERT_BIN="${INSERT_BIN:-insert_streaming}"
export QUERY_SCALING_BIN="${QUERY_SCALING_BIN:-query}"
export WEAVIATE_CLIENT_BINARY="${WEAVIATE_CLIENT_BINARY:-insert_streaming}"
export RAFT_BOOTSTRAP_TIMEOUT="${RAFT_BOOTSTRAP_TIMEOUT:-1200}"

# DAOS settings (exported so launchWeaviateNode.sh can see them when STORAGE_MEDIUM=DAOS).
export DAOS_POOL="${DAOS_POOL:-radix-io}"
export DAOS_CONT="${DAOS_CONT:-songoh-vdb-sc26}"
export DFUSE_MOUNT_PARENT="${DFUSE_MOUNT_PARENT:-/tmp}"
export DAOS_SUBDIR_PREFIX="${DAOS_SUBDIR_PREFIX:-weaviate}"
export WIPE_WEAVIATE_DATA="${WIPE_WEAVIATE_DATA:-1}"

INSERT_TOLERANCE="${INSERT_TOLERANCE:-0.00001}"
INSERT_POLL_SEC="${INSERT_POLL_SEC:-5}"
SCHEMA_CONSENSUS_TIMEOUT="${SCHEMA_CONSENSUS_TIMEOUT:-300}"

# ---------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------

require_exe() {
    local p="$1"
    if [[ ! -x "$p" ]]; then
        echo "Error: executable not found or not executable: $p" >&2
        exit 1
    fi
}

emit_event() {
    local name="$1"
    touch "${name}.event"
    echo "[EVENT] ${name}  ($(date '+%Y-%m-%d %H:%M:%S'))"
}

epoch_now() {
    python3 -c "import time; print(f'{time.time():.6f}')"
}

wait_for_cluster_ready_signal() {
    local target="$1"
    local deadline=$((SECONDS + 1800))
    while [[ ! -e "${target}" ]]; do
        if ! kill -0 "${MPI_PID}" 2>/dev/null; then
            echo "Error: mpirun exited before readiness" >&2
            exit 1
        fi
        if (( SECONDS > deadline )); then
            echo "Error: timed out waiting for cluster readiness signal: ${target}" >&2
            exit 1
        fi
        sleep 0.5
    done
}

wait_for_all_nodes_healthy() {
    local timeout="${1:-300}"
    local deadline=$((SECONDS + timeout))

    echo "[HEALTH] Verifying all ${TOTAL} Weaviate workers are healthy (timeout=${timeout}s)..."

    while (( SECONDS < deadline )); do
        local all_ok=true
        local sick_nodes=""

        while IFS=',' read -r rank node_name ip http_port _g _d _r grpc_port _rest; do
            local url="http://${ip}:${http_port}/v1/.well-known/ready"
            if ! curl -sf --max-time 5 --noproxy "*" "${url}" > /dev/null 2>&1; then
                all_ok=false
                sick_nodes="${sick_nodes} rank${rank}(${ip}:${http_port})"
            fi
        done < ip_registry.txt

        if ${all_ok}; then
            echo "[HEALTH] All ${TOTAL} workers are healthy."
            return 0
        fi

        echo "[HEALTH] Unhealthy:${sick_nodes} — retrying in 3s..."
        sleep 3
    done

    echo "Error: not all workers healthy after ${timeout}s" >&2
    exit 1
}

wait_for_schema_consensus() {
    local class="$1"
    local timeout="${2:-${SCHEMA_CONSENSUS_TIMEOUT}}"
    local deadline=$((SECONDS + timeout))

    echo "[SCHEMA] Waiting for class '${class}' on all ${TOTAL} workers (timeout=${timeout}s)..."

    while (( SECONDS < deadline )); do
        local all_ready=true
        local missing_nodes=""

        while IFS=',' read -r rank node_name ip http_port _g _d _r grpc_port _rest; do
            local url="http://${ip}:${http_port}/v1/schema/${class}"
            local status
            status=$(curl -sf -o /dev/null -w '%{http_code}' --max-time 5 --noproxy "*" "${url}" 2>/dev/null || echo "000")
            if [[ "${status}" != "200" ]]; then
                all_ready=false
                missing_nodes="${missing_nodes} rank${rank}(${ip}:${http_port})=${status}"
            fi
        done < ip_registry.txt

        if ${all_ready}; then
            echo "[SCHEMA] Class '${class}' visible on all ${TOTAL} workers."
            return 0
        fi

        echo "[SCHEMA] Not converged:${missing_nodes} — retrying in 5s..."
        sleep 5
    done

    echo "Error: schema consensus not reached for '${class}' after ${timeout}s" >&2
    while IFS=',' read -r rank node_name ip http_port _g _d _r grpc_port _rest; do
        local status
        status=$(curl -sf -o /dev/null -w '%{http_code}' --max-time 5 --noproxy "*" \
            "http://${ip}:${http_port}/v1/schema/${class}" 2>/dev/null || echo "000")
        echo "  rank=${rank} ${ip}:${http_port} -> HTTP ${status}" >&2
    done < ip_registry.txt
    exit 1
}

create_class_with_consensus() {
    echo "[CLASS] Creating class '${CLASS_NAME}' on ${WEAVIATE_HTTP_ADDR}..."

    local class_json
    class_json=$(python3 - <<PY
import json
metric_map = {"COSINE": "cosine", "DOT": "dot", "L2": "l2-squared", "cosine": "cosine", "dot": "dot"}
distance = metric_map.get("${DISTANCE_METRIC}", "cosine")
class_def = {
    "class": "${CLASS_NAME}",
    "vectorIndexType": "hnsw",
    "vectorIndexConfig": {
        "efConstruction": 128,
        "maxConnections": 16,
        "ef": ${QUERY_EF},
        "dynamicEfMin": 100,
        "dynamicEfMax": 500,
        "dynamicEfFactor": 8,
        "distance": distance,
        "vectorCacheMaxObjects": 1000000000000,
    },
    "replicationConfig": {"factor": 1},
    "shardingConfig": {"desiredCount": ${TOTAL}, "virtualPerPhysical": 1},
}
print(json.dumps(class_def))
PY
    )

    if [[ "${RESET_CLASS}" == "true" ]]; then
        echo "[CLASS] Deleting existing class if present..."
        curl -sf --max-time 30 --noproxy "*" \
            -X DELETE "${WEAVIATE_HTTP_ADDR}/v1/schema/${CLASS_NAME}" \
            > /dev/null 2>&1 || true
        sleep 10
    fi

    local http_code
    http_code=$(curl -s -o /tmp/class_create_resp.json -w '%{http_code}' \
        --max-time 60 --noproxy "*" \
        -X POST "${WEAVIATE_HTTP_ADDR}/v1/schema" \
        -H "Content-Type: application/json" \
        -d "${class_json}" 2>/dev/null || echo "000")

    if [[ "${http_code}" == "200" || "${http_code}" == "201" ]]; then
        echo "[CLASS] Created (HTTP ${http_code})"
    elif [[ "${http_code}" == "422" ]]; then
        echo "[CLASS] Already exists (HTTP 422)"
    else
        echo "Error: failed to create class (HTTP ${http_code})" >&2
        cat /tmp/class_create_resp.json >&2 2>/dev/null || true
        exit 1
    fi

    wait_for_schema_consensus "${CLASS_NAME}"
}

fetch_nodes_verbose() {
    curl -sS --noproxy "*" \
        "${WEAVIATE_HTTP_ADDR}/v1/nodes?output=verbose&collection=${CLASS_NAME}" \
        > nodes_verbose.json 2>/dev/null || true
}

wait_for_object_count() {
    local expected="$1"
    local timeout_s="${2:-43200}"
    local deadline=$((SECONDS + timeout_s))

    local min_acceptable
    min_acceptable=$(python3 -c "import math; print(math.ceil(${expected} * (1.0 - ${INSERT_TOLERANCE})))")

    echo "[INFO] Waiting for object count: expected=${expected} min_acceptable=${min_acceptable}"

    while (( SECONDS < deadline )); do
        fetch_nodes_verbose

        local observed
        observed=$(python3 - <<'PY'
import json
try:
    with open("nodes_verbose.json") as f:
        data = json.load(f)
    total = 0
    for node in (data.get("nodes") or []):
        for shard in (node.get("shards") or []):
            try: total += int(shard.get("objectCount", 0))
            except: pass
    print(total)
except: print(0)
PY
        )

        echo "[INFO] observed_objects=${observed} expected=${expected}"

        if [[ "${observed}" -ge "${expected}" ]]; then
            echo "[INFO] Object count reached expected value"
            return 0
        fi
        if [[ "${observed}" -ge "${min_acceptable}" ]]; then
            echo "[WARN] Object count ${observed} within tolerance (missing $((expected - observed)))"
            return 0
        fi
        sleep 10
    done

    echo "Error: object count never reached ${expected}" >&2
    exit 1
}

get_actual_object_count() {
    fetch_nodes_verbose

    python3 - <<'PY'
import json
try:
    with open("nodes_verbose.json") as f:
        data = json.load(f)
    total = 0
    for node in (data.get("nodes") or []):
        for shard in (node.get("shards") or []):
            try: total += int(shard.get("objectCount", 0))
            except: pass
    print(total)
except: print(0)
PY
}

wait_for_index_quiescence() {
    local stable_needed="${1:-5}"
    local poll_s="${2:-10}"
    local timeout_s="${3:-43200}"
    local deadline=$((SECONDS + timeout_s))
    local stable=0

    echo "[INFO] Waiting for indexing quiescence (need ${stable_needed} consecutive stable polls)..."

    while (( SECONDS < deadline )); do
        fetch_nodes_verbose

        read -r queue_sum indexing_sum <<< "$(python3 - <<'PY'
import json
def as_int(x):
    try: return int(x)
    except: return 0
try:
    with open("nodes_verbose.json") as f:
        data = json.load(f)
    q = 0; idx = 0
    for node in (data.get("nodes") or []):
        for shard in (node.get("shards") or []):
            q += as_int(shard.get("vectorQueueLength", 0))
            q += as_int(shard.get("queueLength", 0))
            vis = shard.get("vectorIndexingStatus", "")
            if isinstance(vis, str):
                idx += 1 if vis.strip().upper() == "INDEXING" else 0
            else:
                idx += as_int(vis)
    print(q, idx)
except: print(0, 0)
PY
        )"

        echo "[INFO] queue_sum=${queue_sum} indexing_shards=${indexing_sum} stable=${stable}/${stable_needed}"

        if [[ "${queue_sum}" -eq 0 && "${indexing_sum}" -eq 0 ]]; then
            stable=$((stable + 1))
            if [[ "${stable}" -ge "${stable_needed}" ]]; then
                echo "[INFO] Index quiescence reached (all queues drained, all shards READY)"
                return 0
            fi
        else
            stable=0
        fi

        sleep "${poll_s}"
    done

    echo "Error: indexing never quiesced" >&2
    exit 1
}

assert_all_shards_ready() {
    echo "[GATE] Final pre-query check: all shards must be READY with zero queue..."
    fetch_nodes_verbose

    python3 - <<'PY'
import json, sys

with open("nodes_verbose.json") as f:
    data = json.load(f)

problems = []
total_objects = 0
for node in (data.get("nodes") or []):
    name = node.get("name", "?")
    for shard in (node.get("shards") or []):
        oc = int(shard.get("objectCount", 0))
        ql = int(shard.get("vectorQueueLength", 0)) + int(shard.get("queueLength", 0))
        st = str(shard.get("vectorIndexingStatus", "")).strip().upper()
        total_objects += oc
        if ql > 0:
            problems.append(f"  {name}: queueLength={ql}")
        if st != "READY":
            problems.append(f"  {name}: vectorIndexingStatus={st}")

if problems:
    print("[GATE] FAIL — shards not ready:", file=sys.stderr)
    for p in problems:
        print(p, file=sys.stderr)
    sys.exit(1)

print(f"[GATE] PASS — all shards READY, total_objects={total_objects}")
PY

    if [[ $? -ne 0 ]]; then
        echo "Error: pre-query gate failed — refusing to start query phase with incomplete indexing" >&2
        exit 1
    fi
}

partition_range() {
    local total="$1" parts="$2" idx="$3"
    python3 - "$total" "$parts" "$idx" <<'PY'
import sys
total, parts, idx = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])
base, rem = divmod(total, parts)
start = idx * base + min(idx, rem)
size = base + (1 if idx < rem else 0)
print(start, size)
PY
}

# ---------------------------------------------------------------
# Phases
# ---------------------------------------------------------------

run_insert_phase() {
    local total_insert_clients=$((TOTAL * UPLOAD_CLIENTS_PER_WORKER))
    echo "[INFO] Insert phase: TOTAL_INSERT_CLIENTS=${total_insert_clients} BATCH_SIZE=${UPLOAD_BATCH_SIZE}"
    require_exe "./go_client/${INSERT_BIN}"

    local min_acceptable
    min_acceptable=$(python3 -c "import math; print(math.ceil(${CORPUS_SIZE} * (1.0 - ${INSERT_TOLERANCE})))")
    echo "[INFO] CORPUS_SIZE=${CORPUS_SIZE} min_acceptable=${min_acceptable} (tolerance=${INSERT_TOLERANCE})"

    emit_event "insert_start_0"

    env NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" \
        UPLOAD_CLIENTS_PER_WORKER="${total_insert_clients}" \
        UPLOAD_BATCH_SIZE="${UPLOAD_BATCH_SIZE}" \
        UPLOAD_BALANCE_STRATEGY="${UPLOAD_BALANCE_STRATEGY}" \
        INSERT_MODE="${INSERT_MODE}" \
        INSERT_OPS_PER_SEC="${INSERT_OPS_PER_SEC}" \
        RPC_TIMEOUT="${RPC_TIMEOUT}" \
        WAIT_SEC="${WAIT_SEC}" \
        OVERALL_SEC="${OVERALL_SEC}" \
        DRAIN_TIMEOUT_SEC="${DRAIN_TIMEOUT_SEC}" \
        DRAIN_POLL_MS="${DRAIN_POLL_MS}" \
        DRAIN_STABLE_POLLS="${DRAIN_STABLE_POLLS}" \
        DRAIN_PLATEAU_POLLS="${DRAIN_PLATEAU_POLLS}" \
        DRAIN_LOG_EVERY_POLLS="${DRAIN_LOG_EVERY_POLLS}" \
        INSERT_MAX_RETRIES="${INSERT_MAX_RETRIES}" \
        INSERT_RETRY_BACKOFF_MS="${INSERT_RETRY_BACKOFF_MS}" \
        SHARD_COUNT="${TOTAL}" \
        ./go_client/${INSERT_BIN} &
    local GO_PID=$!
    echo "[INFO] Go insert client started (PID=${GO_PID})"

    local insert_done=false
    while true; do
        if ! kill -0 "${GO_PID}" 2>/dev/null; then
            local rc=0
            wait "${GO_PID}" 2>/dev/null || rc=$?
            if [[ "${rc}" -eq 0 ]]; then
                echo "[INFO] Go insert client exited cleanly (rc=0)"
                insert_done=true
            else
                echo "[WARN] Go insert client exited with rc=${rc}"
                local actual
                actual=$(get_actual_object_count)
                local missing=$(( CORPUS_SIZE - actual ))
                echo "[INFO] actual=${actual} expected=${CORPUS_SIZE} missing=${missing}"
                if [[ "${actual}" -ge "${min_acceptable}" ]]; then
                    echo "[WARN] Insert had errors but ${actual}/${CORPUS_SIZE} objects landed (within tolerance). Continuing."
                    insert_done=true
                else
                    echo "Error: insert phase failed, only ${actual}/${CORPUS_SIZE} objects (exceeds tolerance)" >&2
                    emit_event "insert_end_1"
                    exit "${rc}"
                fi
            fi
            break
        fi

        local actual
        actual=$(get_actual_object_count)
        if [[ "${actual}" -ge "${min_acceptable}" ]]; then
            local missing=$(( CORPUS_SIZE - actual ))
            echo "[INFO] Object count ${actual}/${CORPUS_SIZE} reached acceptable threshold (missing ${missing}). Killing Go client."
            kill "${GO_PID}" 2>/dev/null || true
            wait "${GO_PID}" 2>/dev/null || true
            insert_done=true
            break
        fi

        sleep "${INSERT_POLL_SEC}"
    done

    emit_event "insert_end_1"

    if ! "${insert_done}"; then
        echo "Error: insert phase ended without reaching acceptable object count" >&2
        exit 1
    fi
}

run_indexing_phase() {
    local ingest_start_epoch="${1}"
    local ingest_start_seconds="${2}"

    local index_start_epoch
    index_start_epoch=$(epoch_now)
    local index_start_seconds=${SECONDS}

    emit_event "index_start_2"

    echo "[INFO] Indexing phase: waiting for object count and index quiescence..."
    wait_for_object_count "${CORPUS_SIZE}"
    wait_for_index_quiescence 5 10 43200

    local index_end_epoch
    index_end_epoch=$(epoch_now)
    local index_end_seconds=${SECONDS}
    local post_insert_drain_elapsed=$(( index_end_seconds - index_start_seconds ))
    local total_ingest_elapsed=$(( index_end_seconds - ingest_start_seconds ))

    emit_event "index_end_3"

    echo "[INFO] Post-insert drain/quiescence: ${post_insert_drain_elapsed}s"
    echo "[INFO] Total ingest wall time: ${total_ingest_elapsed}s"

    python3 - "${ingest_start_epoch}" "${index_start_epoch}" "${index_end_epoch}" <<'PY'
import json, os, sys

ingest_start_epoch = float(sys.argv[1])
index_start_epoch  = float(sys.argv[2])
index_end_epoch    = float(sys.argv[3])

insert_send_sec       = index_start_epoch - ingest_start_epoch
post_insert_drain_sec = index_end_epoch - index_start_epoch
total_ingest_sec      = index_end_epoch - ingest_start_epoch

result_path = os.environ["RESULT_PATH"]

go_send_sec = go_drain_sec = go_throughput_send = go_throughput_drain = 0.0
insert_summary_path = os.path.join(result_path, "insert_summary.json")
if os.path.exists(insert_summary_path):
    with open(insert_summary_path) as f:
        ins = json.load(f)
    go_send_sec = float(ins.get("send_sec", 0))
    go_drain_sec = float(ins.get("drain_sec", 0))
    go_throughput_send = float(ins.get("throughput_send_vps", 0))
    go_throughput_drain = float(ins.get("throughput_drain_vps", 0))

shard_details = []
if os.path.exists("nodes_verbose.json"):
    with open("nodes_verbose.json") as f:
        data = json.load(f)
    for node in (data.get("nodes") or []):
        for shard in (node.get("shards") or []):
            shard_details.append({
                "node": node.get("name", "unknown"),
                "shard": shard.get("name", ""),
                "objectCount": shard.get("objectCount", 0),
                "queueLength": shard.get("queueLength", 0),
                "vectorQueueLength": shard.get("vectorQueueLength", 0),
                "vectorIndexingStatus": shard.get("vectorIndexingStatus", ""),
            })

corpus_size = int(os.environ.get("CORPUS_SIZE", "0"))
index_time = {
    "class_name": os.environ.get("CLASS_NAME", ""),
    "corpus_size": corpus_size,
    "workers_total": int(os.environ.get("NODES", "0")) * int(os.environ.get("WORKERS_PER_NODE", "0")),
    "total_ingest_sec": round(total_ingest_sec, 3),
    "insert_send_sec": round(insert_send_sec, 3),
    "post_insert_drain_sec": round(post_insert_drain_sec, 3),
    "go_insert_send_sec": round(go_send_sec, 3),
    "go_insert_total_sec": round(go_drain_sec, 3),
    "go_throughput_send_vps": round(go_throughput_send, 3),
    "go_throughput_drain_vps": round(go_throughput_drain, 3),
    "total_ingest_throughput_vps": round(corpus_size / total_ingest_sec, 3) if total_ingest_sec > 0 else 0.0,
    "ingest_start_epoch": ingest_start_epoch,
    "index_start_epoch": index_start_epoch,
    "index_end_epoch": index_end_epoch,
    "shard_details": shard_details,
    "status": "ok",
}

out = os.path.join(result_path, "index_time.json")
with open(out, "w") as f:
    json.dump(index_time, f, indent=2)
print(f"[INFO] Wrote {out}")
PY
}

run_query_phase() {
    require_exe "./go_client/${QUERY_SCALING_BIN}"

    assert_all_shards_ready

    local qcpw="${QUERY_CLIENTS_PER_WORKER}"
    local total_query_clients
    if [[ "${QUERY_CLIENT_MODE}" == "fixed" ]]; then
        total_query_clients="${qcpw}"
    else
        total_query_clients=$((TOTAL * qcpw))
    fi
    echo "[INFO] Query phase: ${total_query_clients} clients (mode=${QUERY_CLIENT_MODE}), workload=${QUERY_WORKLOAD}"

    wait_for_all_nodes_healthy 120

    emit_event "query_start_4"

    local pids=()
    local client_id=0
    local clients_remaining=${total_query_clients}

    while IFS=',' read -r rank node_name ip http_port _g _d _r grpc_port _rest; do
        if [[ "${clients_remaining}" -le 0 ]]; then break; fi

        local clients_on_this_worker
        if [[ "${QUERY_CLIENT_MODE}" == "fixed" ]]; then
            if [[ "${clients_remaining}" -ge "${qcpw}" ]]; then
                clients_on_this_worker="${qcpw}"
            else
                clients_on_this_worker="${clients_remaining}"
            fi
        else
            clients_on_this_worker="${qcpw}"
        fi

        for _qc in $(seq 1 "${clients_on_this_worker}"); do
            local cid=${client_id}
            local client_dir="${RESULT_PATH}/query_client_${cid}"
            mkdir -p "${client_dir}"

            read -r q_start q_size <<< "$(partition_range "${QUERY_WORKLOAD}" "${total_query_clients}" "${cid}")"

            if [[ "${q_size}" -le 0 ]]; then
                client_id=$((client_id + 1))
                clients_remaining=$((clients_remaining - 1))
                continue
            fi

            (
                export WEAVIATE_SCHEME="http"
                export WEAVIATE_HOST="${ip}:${http_port}"
                export START_ROW=$(( ${START_ROW:-0} + q_start ))
                export QUERY_WORKLOAD="${q_size}"
                export RESULT_PATH="${client_dir}"
                export QUERY_FILEPATH="${QUERY_FILEPATH}"
                export CLASS_NAME="${CLASS_NAME}"
                export QUERY_BATCH_SIZE="${QUERY_BATCH_SIZE}"
                export QUERY_TOPK="${QUERY_TOPK}"
                export QUERY_EF="${QUERY_EF}"
                export VECTOR_DIM="${VECTOR_DIM}"
                export WAIT_SEC="${WAIT_SEC}"
                export OVERALL_SEC="${OVERALL_SEC}"
                export RPC_TIMEOUT="${RPC_TIMEOUT}"

                env NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" \
                    ./go_client/${QUERY_SCALING_BIN}
            ) > "${client_dir}/client.out" 2>&1 &

            pids+=($!)
            echo "[INFO] Query client ${cid} -> ${ip}:${http_port} start=$((${START_ROW:-0} + q_start)) count=${q_size}"
            client_id=$((client_id + 1))
            clients_remaining=$((clients_remaining - 1))
        done
    done < ip_registry.txt

    echo "[INFO] Waiting for ${#pids[@]} query clients..."

    local any_failed=0
    for pid in "${pids[@]}"; do
        if ! wait "${pid}"; then
            echo "[WARN] Query client PID=${pid} failed" >&2
            any_failed=1
        fi
    done

    emit_event "query_end_5"

    if [[ "${any_failed}" -ne 0 ]]; then
        echo "Error: one or more query clients failed" >&2
        exit 1
    fi

    echo "[INFO] All ${client_id} query clients completed"
}

write_run_summary() {
    python3 - <<'PY'
import json, os, glob

result_path = os.environ["RESULT_PATH"]
summary = {
    "task": os.environ.get("TASK"),
    "class_name": os.environ.get("CLASS_NAME"),
    "corpus_size": int(os.environ.get("CORPUS_SIZE", "0")),
    "query_workload": int(os.environ.get("QUERY_WORKLOAD", "0")),
    "workers_total": int(os.environ.get("NODES", "0")) * int(os.environ.get("WORKERS_PER_NODE", "0")),
    "upload_clients_per_worker": int(os.environ.get("UPLOAD_CLIENTS_PER_WORKER", "0")),
    "query_clients_per_worker": int(os.environ.get("QUERY_CLIENTS_PER_WORKER", "0")),
    "query_client_mode": os.environ.get("QUERY_CLIENT_MODE", "per_worker"),
    "query_batch_size": int(os.environ.get("QUERY_BATCH_SIZE", "0")),
    "storage_medium": os.environ.get("STORAGE_MEDIUM", ""),
    "insert_summary_files": [],
    "query_summary_files": [],
    "aggregate": {}
}

insert_jsons = sorted(glob.glob(os.path.join(result_path, "insert_summary*.json")))
summary["insert_summary_files"] = insert_jsons

go_send_sec = go_drain_sec = go_throughput_send = go_throughput_drain = 0.0
for fp in insert_jsons:
    with open(fp) as f:
        ins = json.load(f)
    go_send_sec = float(ins.get("send_sec", 0))
    go_drain_sec = float(ins.get("drain_sec", 0))
    go_throughput_send = float(ins.get("throughput_send_vps", 0))
    go_throughput_drain = float(ins.get("throughput_drain_vps", 0))

summary["aggregate"]["go_insert_send_sec"] = round(go_send_sec, 3)
summary["aggregate"]["go_insert_total_sec"] = round(go_drain_sec, 3)
summary["aggregate"]["insert_throughput_send_vps"] = round(go_throughput_send, 3)
summary["aggregate"]["insert_throughput_drain_vps"] = round(go_throughput_drain, 3)

index_time_path = os.path.join(result_path, "index_time.json")
total_ingest_sec = insert_send_sec_wall = post_insert_drain_sec = total_ingest_throughput = 0.0
if os.path.exists(index_time_path):
    with open(index_time_path) as f:
        idx = json.load(f)
    total_ingest_sec = float(idx.get("total_ingest_sec", 0))
    insert_send_sec_wall = float(idx.get("insert_send_sec", 0))
    post_insert_drain_sec = float(idx.get("post_insert_drain_sec", 0))
    total_ingest_throughput = float(idx.get("total_ingest_throughput_vps", 0))

summary["aggregate"]["insert_send_sec_wall"] = round(insert_send_sec_wall, 3)
summary["aggregate"]["post_insert_drain_sec"] = round(post_insert_drain_sec, 3)
summary["aggregate"]["total_ingest_sec"] = round(total_ingest_sec, 3)
summary["aggregate"]["total_ingest_throughput_vps"] = round(total_ingest_throughput, 3)

query_jsons = sorted(glob.glob(os.path.join(result_path, "query_client_*", "query_summary.json")))
summary["query_summary_files"] = query_jsons

total_queries = 0; total_time = 0.0; weighted_latency_num = 0.0
completed_clients = 0; per_client_qps = []

for fp in query_jsons:
    with open(fp) as f:
        data = json.load(f)
    q = int(data.get("queries_completed", 0))
    t = float(data.get("total_time_sec", 0) or 0)
    lat = float(data.get("mean_latency_sec", 0) or 0)
    qps = float(data.get("throughput_qps", 0) or 0)
    total_queries += q
    total_time = max(total_time, t)
    weighted_latency_num += q * lat
    completed_clients += 1
    per_client_qps.append(qps)

summary["aggregate"]["query_clients_completed"] = completed_clients
summary["aggregate"]["queries_completed"] = total_queries
summary["aggregate"]["query_wall_time_sec"] = round(total_time, 3)
summary["aggregate"]["query_throughput_qps"] = round((total_queries / total_time) if total_time > 0 else 0.0, 3)
summary["aggregate"]["query_mean_latency_sec"] = round((weighted_latency_num / total_queries) if total_queries > 0 else 0.0, 6)
summary["aggregate"]["query_per_client_qps"] = [round(x, 3) for x in per_client_qps]

out = os.path.join(result_path, "query_scaling_summary.json")
with open(out, "w") as f:
    json.dump(summary, f, indent=2)
print(f"[INFO] Wrote {out}")
PY
}

# ===============================================================
# MAIN EXECUTION
# ===============================================================

if [[ "${PLATFORM}" == "AURORA" ]]; then
    set +u
    module load apptainer
    module load frameworks
    STORAGE_MEDIUM_UC="${STORAGE_MEDIUM^^}"
    if [[ "${STORAGE_MEDIUM_UC}" == "DAOS" ]]; then
        module use /soft/modulefiles 2>/dev/null || true
        module load daos 2>/dev/null || true
    fi
    set -u
    source /lus/flare/projects/radix-io/sockerman/qdrant/qEnv/bin/activate
    cd "${PBS_O_WORKDIR}"
else
    echo "Error: main_query_scaling.sh currently supports PLATFORM=AURORA only" >&2
    exit 1
fi

exec > >(tee output.log) 2>&1

TOTAL=$((NODES * WORKERS_PER_NODE))
export NODES WORKERS_PER_NODE TOTAL
MAX_RANK=$((TOTAL - 1))

STORAGE_MEDIUM_UC="${STORAGE_MEDIUM^^}"

echo "[INFO] ============================================"
echo "[INFO] TASK=${TASK}"
echo "[INFO] Working directory: $(pwd)"
echo "[INFO] TOTAL worker ranks=${TOTAL}"
echo "[INFO] WORKERS_PER_NODE=${WORKERS_PER_NODE}"
echo "[INFO] CORES=${CORES}"
echo "[INFO] STORAGE_MEDIUM=${STORAGE_MEDIUM}"
echo "[INFO] UPLOAD_CLIENTS_PER_WORKER=${UPLOAD_CLIENTS_PER_WORKER}"
echo "[INFO] UPLOAD_BATCH_SIZE=${UPLOAD_BATCH_SIZE}"
echo "[INFO] QUERY_CLIENTS_PER_WORKER=${QUERY_CLIENTS_PER_WORKER}"
echo "[INFO] QUERY_CLIENT_MODE=${QUERY_CLIENT_MODE}"
echo "[INFO] UPLOAD_BALANCE_STRATEGY=${UPLOAD_BALANCE_STRATEGY}"
echo "[INFO] INSERT_MODE=${INSERT_MODE}"
echo "[INFO] INSERT_MAX_RETRIES=${INSERT_MAX_RETRIES}"
echo "[INFO] INSERT_RETRY_BACKOFF_MS=${INSERT_RETRY_BACKOFF_MS}"
echo "[INFO] RAFT_BOOTSTRAP_TIMEOUT=${RAFT_BOOTSTRAP_TIMEOUT}"
echo "[INFO] SCHEMA_CONSENSUS_TIMEOUT=${SCHEMA_CONSENSUS_TIMEOUT}"
if [[ "${STORAGE_MEDIUM_UC}" == "DAOS" ]]; then
    echo "[INFO] DAOS_POOL=${DAOS_POOL}"
    echo "[INFO] DAOS_CONT=${DAOS_CONT}"
    echo "[INFO] DFUSE_MOUNT_PARENT=${DFUSE_MOUNT_PARENT}"
    echo "[INFO] DAOS_SUBDIR_PREFIX=${DAOS_SUBDIR_PREFIX}"
    echo "[INFO] WIPE_WEAVIATE_DATA=${WIPE_WEAVIATE_DATA}"
fi
echo "[INFO] ============================================"

tail -n +2 "${PBS_NODEFILE}" | awk '!seen[$0]++' > worker_nodefile.txt
WORKER_HOSTS=$(paste -sd, worker_nodefile.txt)
echo "[INFO] WORKER_HOSTS=${WORKER_HOSTS}"

mkdir -p ./perf
rm -f ./perf/weaviate_running*.txt
rm -f ip_registry.txt
rm -rf ip_parts

# ---------------------------------------------------------------
# Optional: launch profile.py on client node
# ---------------------------------------------------------------
CLIENT_PROFILE_PID=""
if [[ -f "profile.py" ]]; then
    python3 profile.py "client" "${PLATFORM}" &
    CLIENT_PROFILE_PID=$!
    echo "[INFO] Started profile.py on client node (PID=${CLIENT_PROFILE_PID})"
fi

# ---------------------------------------------------------------
# DAOS: launch dfuse on each worker node
# ---------------------------------------------------------------
if [[ "${STORAGE_MEDIUM_UC}" == "DAOS" ]]; then
    JOBID="${PBS_JOBID:-manual.$(date +%Y%m%d_%H%M%S)}"
    DFUSE_MNT="${DFUSE_MOUNT_PARENT}/${DAOS_POOL}/${DAOS_CONT}"
    export DAOS_DFUSE_MNT="${DFUSE_MNT}"
    export DAOS_JOBID="${JOBID}"

    NUM_WORKER_HOSTS=$(wc -l < worker_nodefile.txt)
    echo "[INFO] Launching dfuse on ${NUM_WORKER_HOSTS} worker nodes (pool=${DAOS_POOL} cont=${DAOS_CONT})..."

    for whost in ${WORKER_HOSTS//,/ }; do
        mpirun -n 1 --ppn 1 --no-vni --cpu-bind none --host "${whost}" \
            bash -c "fusermount3 -u '${DFUSE_MNT}' 2>/dev/null || true" || true
    done

    DFUSE_NODEFILE_DIR="$(pwd)/dfuse_nodefiles"
    mkdir -p "${DFUSE_NODEFILE_DIR}"

    for whost in ${WORKER_HOSTS//,/ }; do
        echo "[INFO] Launching dfuse on ${whost}..."
        SINGLE_NODEFILE="${DFUSE_NODEFILE_DIR}/${whost}.txt"
        echo "${whost}" > "${SINGLE_NODEFILE}"

        mpirun -n 1 --ppn 1 --no-vni --cpu-bind none --host "${whost}" \
            bash -lc "
                export PBS_NODEFILE='${SINGLE_NODEFILE}'
                module use /soft/modulefiles 2>/dev/null || true
                module load daos 2>/dev/null || true
                launch-dfuse.sh '${DAOS_POOL}:${DAOS_CONT}'
            " || { echo "[FATAL] dfuse launch failed on ${whost}"; exit 1; }

        echo "[INFO] dfuse launched on ${whost}"
    done

    echo "[INFO] Verifying dfuse mounts..."
    for whost in ${WORKER_HOSTS//,/ }; do
        mpirun -n 1 --ppn 1 --no-vni --cpu-bind none --host "${whost}" \
            bash -c "
                for _i in \$(seq 1 30); do
                    mountpoint -q '${DFUSE_MNT}' 2>/dev/null && break
                    sleep 1
                done
                if mountpoint -q '${DFUSE_MNT}' 2>/dev/null; then
                    echo '[DFUSE] OK on '\$(hostname)': ${DFUSE_MNT}'
                else
                    echo '[DFUSE][FATAL] mount not found on '\$(hostname)': ${DFUSE_MNT}' >&2
                    exit 1
                fi
            " || { echo "[FATAL] dfuse verify failed on ${whost}"; exit 1; }
    done

    echo "[INFO] dfuse mounted on all worker nodes"
fi

# ---------------------------------------------------------------
# Launch Weaviate cluster via MPI
# ---------------------------------------------------------------
if [[ "${CORES}" -eq 208 ]]; then
    mpirun -n "${TOTAL}" \
        --ppn "${WORKERS_PER_NODE}" \
        --no-vni \
        --cpu-bind none \
        --host "${WORKER_HOSTS}" \
        bash ./launchWeaviateNode.sh "${STORAGE_MEDIUM}" "${USEPERF}" "${TOTAL}" &
else
    mpirun -n "${TOTAL}" \
        --ppn "${WORKERS_PER_NODE}" \
        -d "${CORES}" \
        --cpu-bind depth \
        --host "${WORKER_HOSTS}" \
        bash ./launchWeaviateNode.sh "${STORAGE_MEDIUM}" "${USEPERF}" "${TOTAL}" &
fi

MPI_PID=$!

cleanup() {
    echo "[INFO] Cleaning up..."
    touch flag.txt
    if [[ -n "${CLIENT_PROFILE_PID}" ]]; then
        wait "${CLIENT_PROFILE_PID}" 2>/dev/null || true
    fi
    kill "${MPI_PID}" 2>/dev/null || true
    wait "${MPI_PID}" 2>/dev/null || true
}
trap cleanup EXIT TERM INT

echo "[INFO] Waiting for ${TOTAL} Weaviate workers to become ready..."
for r in $(seq 0 "${MAX_RANK}"); do
    wait_for_cluster_ready_signal "./perf/weaviate_running${r}.txt"
done
echo "[INFO] All ${TOTAL} workers are ready"

echo "[INFO] Waiting 60s for Raft stabilization..."
sleep 60

WEAVIATE_IP=$(awk -F, '$1 == 0 {print $3; exit}' ip_registry.txt)
WEAVIATE_HTTP_PORT=$(awk -F, '$1 == 0 {print $4; exit}' ip_registry.txt)
WEAVIATE_GRPC_PORT=$(awk -F, '$1 == 0 {print $8; exit}' ip_registry.txt)

export WEAVIATE_SCHEME="http"
export WEAVIATE_HOST="${WEAVIATE_IP}:${WEAVIATE_HTTP_PORT}"
export WEAVIATE_HTTP_ADDR="${WEAVIATE_SCHEME}://${WEAVIATE_HOST}"
export WORKER_IP="${WEAVIATE_IP}"
export REST_PORT="${WEAVIATE_HTTP_PORT}"
export GRPC_PORT="${WEAVIATE_GRPC_PORT}"
export DATA_FILE="${DATA_FILEPATH}"
export VEC_DIM="${VECTOR_DIM}"
export MEASURE_VECS="${CORPUS_SIZE}"

echo "[INFO] WEAVIATE_HTTP_ADDR=${WEAVIATE_HTTP_ADDR}"

wait_for_all_nodes_healthy 300
create_class_with_consensus

run_simple_task() {
    local bin_path="./go_client/${WEAVIATE_CLIENT_BINARY}"
    require_exe "${bin_path}"
    echo "[INFO] Running ${bin_path} against ${WEAVIATE_HTTP_ADDR}"
    env NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" \
        "${bin_path}"
}

case "${TASK}" in
    query_scaling)
        INGEST_START_EPOCH=$(epoch_now)
        INGEST_START_SECONDS=${SECONDS}

        run_insert_phase
        run_indexing_phase "${INGEST_START_EPOCH}" "${INGEST_START_SECONDS}"

        echo "[INFO] Pre-query shard distribution:"
        fetch_nodes_verbose
        python3 - <<'PY'
import json
with open("nodes_verbose.json") as f:
    data = json.load(f)
for node in sorted(data.get("nodes", []), key=lambda n: n.get("name", "")):
    for shard in node.get("shards", []):
        print(f"  {node['name']}: objects={shard.get('objectCount',0):>12,}  queue={shard.get('vectorQueueLength',0)}  status={shard.get('vectorIndexingStatus','?')}")
PY
        cp nodes_verbose.json "${RESULT_PATH}/nodes_verbose_pre_query.json" 2>/dev/null || true

        run_query_phase
        write_run_summary
        ;;
    insert|index|query_bs|query_core)
        run_simple_task
        ;;
    *)
        echo "Error: unsupported TASK '${TASK}'" >&2
        exit 1
        ;;
esac

echo "[INFO] Workflow complete"

touch flag.txt
sleep 2

echo "[INFO] Stopping worker ranks..."
kill "${MPI_PID}" 2>/dev/null || true
wait "${MPI_PID}" || true
trap - EXIT TERM INT
exit 0
