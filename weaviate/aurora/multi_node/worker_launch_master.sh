#!/usr/bin/env bash
###############################################################################
# worker_launch_master.sh — launch Weaviate cluster across PBS-allocated nodes
#
# KEY DESIGN:
#   - Container has NO shell: invoke `weaviate` binary directly via apptainer
#   - Health checks run on the HOST (curl from node scripts)
#   - Sanity/membership checks run DIRECTLY on this (head) node
#   - Secondary nodes join with staggered delays to reduce Raft election storms
#   - Final verification POLLS with timeout (not a one-shot check)
#   - Explicitly waits for ALL primary-node workers (not just wid=0)
###############################################################################
set -euo pipefail

module use /soft/modulefiles 2>/dev/null || true
module load apptainer       2>/dev/null || true
module load e2fsprogs       2>/dev/null || true

: "${RUN_DIR:?missing RUN_DIR}"
: "${RUN_NAME:?missing RUN_NAME}"
: "${SIF_PATH:?missing SIF_PATH}"
: "${WORKER_NODES:?missing WORKER_NODES}"
: "${TOTAL_WORKERS:?missing TOTAL_WORKERS}"
: "${MAX_WORKERS_PER_NODE:?missing MAX_WORKERS_PER_NODE}"

REST_PORT_BASE="${REST_PORT_BASE:-8080}"
GRPC_PORT_BASE="${GRPC_PORT_BASE:-50051}"
GOSSIP_PORT_BASE="${GOSSIP_PORT_BASE:-7100}"
DATA_PORT_BASE="${DATA_PORT_BASE:-7200}"
RAFT_PORT_BASE="${RAFT_PORT_BASE:-8300}"
RAFT_RPC_PORT_BASE="${RAFT_RPC_PORT_BASE:-8400}"
PPROF_PORT_BASE="${PPROF_PORT_BASE:-6060}"

PERSIST_BASE="${PERSIST_BASE:-/dev/shm/weaviate_${USER}/${RUN_NAME}}"
WIPE_WEAVIATE_DATA="${WIPE_WEAVIATE_DATA:-1}"

# How long to wait for each worker to become healthy (seconds)
WORKER_HEALTH_TIMEOUT="${WORKER_HEALTH_TIMEOUT:-180}"

# Clear proxies on the head node
export NO_PROXY="*"
export no_proxy="*"
export http_proxy=""
export https_proxy=""
export HTTP_PROXY=""
export HTTPS_PROXY=""

[[ -n "${PBS_NODEFILE:-}" && -f "${PBS_NODEFILE}" ]] || {
  echo "[MASTER][FATAL] PBS_NODEFILE missing"; exit 2
}
mapfile -t ALL_NODES < <(sort -u "${PBS_NODEFILE}")

(( ${#ALL_NODES[@]} >= 2 )) || { echo "[MASTER][FATAL] need >=2 nodes"; exit 3; }

CLIENT_NODE_INDEX=0
WORKER_START_INDEX=1

(( WORKER_START_INDEX + WORKER_NODES <= ${#ALL_NODES[@]} )) || { echo "[MASTER][FATAL] not enough nodes"; exit 4; }
(( TOTAL_WORKERS <= WORKER_NODES * MAX_WORKERS_PER_NODE ))  || { echo "[MASTER][FATAL] too many workers"; exit 5; }

CLIENT_NODE="${ALL_NODES[$CLIENT_NODE_INDEX]}"

SIF_ABS="${SIF_PATH}"
[[ "${SIF_ABS}" == /* ]] || SIF_ABS="${RUN_DIR}/${SIF_ABS}"
[[ -f "${SIF_ABS}" ]] || { echo "[MASTER][FATAL] SIF not found: ${SIF_ABS}"; exit 6; }

mkdir -p "${RUN_DIR}/logs"
echo "${CLIENT_NODE}" > "${RUN_DIR}/logs/client_node.txt"

worker_node_index() { echo $(( WORKER_START_INDEX + ($1 / MAX_WORKERS_PER_NODE) )); }
worker_slot()       { echo $(( $1 % MAX_WORKERS_PER_NODE )); }

PRIMARY_NODE_IDX="$(worker_node_index 0)"
PRIMARY_FQDN="${ALL_NODES[$PRIMARY_NODE_IDX]}"
PRIMARY_SHORT="$(echo "${PRIMARY_FQDN}" | cut -d. -f1)"
PRIMARY_REST=$((REST_PORT_BASE + 0))
PRIMARY_GRPC=$((GRPC_PORT_BASE + 0))
PRIMARY_GOSSIP=$((GOSSIP_PORT_BASE + 0))

for name_val in \
  "primary_node_fqdn:${PRIMARY_FQDN}" \
  "primary_node_short:${PRIMARY_SHORT}" \
  "primary_rest_port:${PRIMARY_REST}" \
  "primary_grpc_port:${PRIMARY_GRPC}" \
  "primary_gossip_port:${PRIMARY_GOSSIP}"; do
  echo "${name_val#*:}" > "${RUN_DIR}/logs/${name_val%%:*}.txt"
done

echo "[MASTER] WORKERS=${TOTAL_WORKERS}  WORKER_NODES=${WORKER_NODES}  MAX/NODE=${MAX_WORKERS_PER_NODE}"
echo "[MASTER] PORTS: rest=${REST_PORT_BASE} grpc=${GRPC_PORT_BASE} gossip=${GOSSIP_PORT_BASE} data=${DATA_PORT_BASE} raft=${RAFT_PORT_BASE} raft_rpc=${RAFT_RPC_PORT_BASE} pprof=${PPROF_PORT_BASE}"
echo "[MASTER] PRIMARY wid=0 node=${PRIMARY_SHORT}(${PRIMARY_FQDN}) rest=${PRIMARY_REST}"
echo "[MASTER] CLIENT  node=${CLIENT_NODE} (PBS index ${CLIENT_NODE_INDEX})"

printf 'worker_id\tnode_index\thost_fqdn\thost_short\tslot\trest_port\tgrpc_port\tgossip_port\tdata_port\traft_port\traft_rpc_port\n' \
  > "${RUN_DIR}/logs/workers.tsv"

PRIMARY_URL="http://${PRIMARY_FQDN}:${PRIMARY_REST}"

# ===================================================================
# Helper: dump worker logs on failure
# ===================================================================
dump_worker_logs() {
  echo ""
  echo "[MASTER] ====== DUMPING WORKER LOGS FOR DIAGNOSIS ======"
  for (( wid=0; wid<TOTAL_WORKERS; wid++ )); do
    local nidx hshort
    nidx="$(worker_node_index "${wid}")"
    hshort="$(echo "${ALL_NODES[$nidx]}" | cut -d. -f1)"
    local errfile="${RUN_DIR}/logs/weaviate_${hshort}_w${wid}.err"
    local outfile="${RUN_DIR}/logs/weaviate_${hshort}_w${wid}.out"
    echo "--- wid=${wid} (${hshort}) stderr (last 30 lines) ---"
    tail -n 30 "${errfile}" 2>/dev/null || echo "  (no stderr file)"
    echo "--- wid=${wid} (${hshort}) stdout (last 10 lines) ---"
    tail -n 10 "${outfile}" 2>/dev/null || echo "  (no stdout file)"
  done
  echo "[MASTER] ====== END WORKER LOGS ======"
  echo ""
}

# ===================================================================
# Helper: wait for a single worker's ready flag (polls with timeout)
# ===================================================================
wait_for_worker() {
  local wid="$1"
  local timeout_sec="$2"
  local nidx hshort ready_flag
  nidx="$(worker_node_index "${wid}")"
  hshort="$(echo "${ALL_NODES[$nidx]}" | cut -d. -f1)"
  ready_flag="${RUN_DIR}/logs/weaviate_${hshort}_w${wid}.ready"

  for (( t=1; t<=timeout_sec; t++ )); do
    [[ -f "${ready_flag}" ]] && return 0
    sleep 1
  done
  return 1
}

# ===================================================================
# Group workers by node
# ===================================================================
declare -A NODE_WIDS
for (( wid=0; wid<TOTAL_WORKERS; wid++ )); do
  nidx="$(worker_node_index "${wid}")"
  NODE_WIDS[$nidx]="${NODE_WIDS[$nidx]:-} ${wid}"
done

# ===================================================================
# Record per-worker metadata (workers.tsv)
# ===================================================================
for (( wid=0; wid<TOTAL_WORKERS; wid++ )); do
  nidx="$(worker_node_index "${wid}")"
  hfqdn="${ALL_NODES[$nidx]}"
  hshort="$(echo "${hfqdn}" | cut -d. -f1)"
  slot="$(worker_slot "${wid}")"

  rp=$((REST_PORT_BASE      + slot))
  gp=$((GRPC_PORT_BASE      + slot))
  gosp=$((GOSSIP_PORT_BASE  + slot))
  dp=$((DATA_PORT_BASE      + slot))
  raftp=$((RAFT_PORT_BASE   + slot))
  raftrpc=$((RAFT_RPC_PORT_BASE + slot))

  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "${wid}" "${nidx}" "${hfqdn}" "${hshort}" "${slot}" "${rp}" "${gp}" "${gosp}" "${dp}" "${raftp}" "${raftrpc}" \
    >> "${RUN_DIR}/logs/workers.tsv"
done

# ===================================================================
# For each node: write a node script that launches all its workers
# ===================================================================
for nidx in $(echo "${!NODE_WIDS[@]}" | tr ' ' '\n' | sort -n); do
  hfqdn="${ALL_NODES[$nidx]}"
  hshort="$(echo "${hfqdn}" | cut -d. -f1)"
  node_script="${RUN_DIR}/logs/node_${hshort}.sh"

  read -r -a wids <<< "${NODE_WIDS[$nidx]}"

  echo "[MASTER] generating node script for ${hshort} (idx=${nidx}): workers ${wids[*]}"

  cat > "${node_script}" <<NODE_HEADER
#!/bin/bash
set -eo pipefail

module use /soft/modulefiles 2>/dev/null || true
module load apptainer       2>/dev/null || true
module load e2fsprogs       2>/dev/null || true

export NO_PROXY="*"
export no_proxy="*"
export http_proxy=""
export https_proxy=""
export HTTP_PROXY=""
export HTTPS_PROXY=""

BASE_SCRATCH="/local/scratch"
[[ -d "\${BASE_SCRATCH}" ]] || BASE_SCRATCH="/tmp"
export APPTAINER_TMPDIR="\${BASE_SCRATCH}/${USER}-apptainer-tmp"
export APPTAINER_CACHEDIR="\${BASE_SCRATCH}/${USER}-apptainer-cache"
mkdir -p "\${APPTAINER_TMPDIR}" "\${APPTAINER_CACHEDIR}"

echo "[NODE ${hshort}] starting ${#wids[@]} worker(s): ${wids[*]}"
ALL_PIDS=()
NODE_HEADER

  for wid in "${wids[@]}"; do
    slot="$(worker_slot "${wid}")"
    hdata="${PERSIST_BASE}/${hshort}/w${wid}"
    wv_out="${RUN_DIR}/logs/weaviate_${hshort}_w${wid}.out"
    wv_err="${RUN_DIR}/logs/weaviate_${hshort}_w${wid}.err"
    pidf="${RUN_DIR}/logs/weaviate_${hshort}_w${wid}.pid"
    ready_flag="${RUN_DIR}/logs/weaviate_${hshort}_w${wid}.ready"

    rp=$((REST_PORT_BASE      + slot))
    gp=$((GRPC_PORT_BASE      + slot))
    gosp=$((GOSSIP_PORT_BASE  + slot))
    dp=$((DATA_PORT_BASE      + slot))
    raftp=$((RAFT_PORT_BASE   + slot))
    raftrpc=$((RAFT_RPC_PORT_BASE + slot))
    pprofp=$((PPROF_PORT_BASE + slot))

    CLUSTER_JOIN_ENV=""
    if (( wid > 0 )); then
      CLUSTER_JOIN_ENV="--env CLUSTER_JOIN=${PRIMARY_FQDN}:${PRIMARY_GOSSIP}"
    fi

    cat >> "${node_script}" <<WORKER_BLOCK
# ---- worker ${wid} (slot ${slot}) ----
echo "[NODE ${hshort}] launching wid=${wid} slot=${slot}"

mkdir -p "${hdata}"
if [[ "${WIPE_WEAVIATE_DATA}" == "1" ]]; then
  rm -rf "${hdata}/"* 2>/dev/null || true
fi

apptainer exec --writable-tmpfs \\
  -B "${hdata}:/var/lib/weaviate" \\
  --env "NO_PROXY=*" \\
  --env "no_proxy=*" \\
  --env "http_proxy=" \\
  --env "https_proxy=" \\
  --env "HTTP_PROXY=" \\
  --env "HTTPS_PROXY=" \\
  --env "AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true" \\
  --env "PERSISTENCE_DATA_PATH=/var/lib/weaviate" \\
  --env "DEFAULT_VECTORIZER_MODULE=none" \\
  --env "CLUSTER_HOSTNAME=${hshort}-w${wid}" \\
  --env "CLUSTER_GOSSIP_BIND_PORT=${gosp}" \\
  --env "CLUSTER_DATA_BIND_PORT=${dp}" \\
  --env "GRPC_PORT=${gp}" \\
  --env "GRPC_MAX_MESSAGE_SIZE=500000000" \\
  --env "RAFT_PORT=${raftp}" \\
  --env "RAFT_INTERNAL_RPC_PORT=${raftrpc}" \\
  --env "MONITORING_PORT=${pprofp}" \\
  ${CLUSTER_JOIN_ENV} \\
  "${SIF_ABS}" weaviate --host 0.0.0.0 --port ${rp} --scheme http \\
  >"${wv_out}" 2>"${wv_err}" &

W_PID=\$!
echo \${W_PID} >"${pidf}"
ALL_PIDS+=(\${W_PID})
echo "[NODE ${hshort}] wid=${wid} apptainer pid=\${W_PID}"

# Health check from HOST side
echo "[NODE ${hshort}] waiting for wid=${wid} to be healthy …"
for i in \$(seq 1 ${WORKER_HEALTH_TIMEOUT}); do
  if [[ -f "${ready_flag}" ]]; then
    break
  fi
  if ! kill -0 \${W_PID} 2>/dev/null; then
    echo "[NODE ${hshort}][ERROR] wid=${wid} apptainer died" >&2
    echo "[NODE ${hshort}] stderr tail:" >&2
    tail -n 20 "${wv_err}" >&2 2>/dev/null || true
    break
  fi
  if curl -fsS --max-time 2 "http://127.0.0.1:${rp}/v1/meta" >/dev/null 2>&1; then
    echo "[NODE ${hshort}] wid=${wid} healthy after \${i}s"
    touch "${ready_flag}"
    break
  fi
  sleep 1
done
if [[ ! -f "${ready_flag}" ]]; then
  echo "[NODE ${hshort}][WARN] wid=${wid} not healthy after probe loop" >&2
fi

WORKER_BLOCK
  done

  cat >> "${node_script}" <<NODE_TAIL

echo "[NODE ${hshort}] all workers launched; keeping task alive …"

for pid in "\${ALL_PIDS[@]}"; do
  wait "\${pid}" 2>/dev/null || true
done

echo "[NODE ${hshort}] all workers exited"
NODE_TAIL

  chmod +x "${node_script}"
done

# ===================================================================
# Launch primary node first
# ===================================================================
echo ""
echo "[MASTER] === launching primary node (${PRIMARY_SHORT}, idx=${PRIMARY_NODE_IDX}) ==="

pbsdsh -n "${PRIMARY_NODE_IDX}" -- bash -l "${RUN_DIR}/logs/node_${PRIMARY_SHORT}.sh" &
PRIMARY_PBSDSH_PID=$!

PRIMARY_READY="${RUN_DIR}/logs/weaviate_${PRIMARY_SHORT}_w0.ready"
echo "[MASTER] waiting for primary (wid=0) to be healthy on its node …"
for i in $(seq 1 300); do
  [[ -f "${PRIMARY_READY}" ]] && { echo "[MASTER] primary healthy (localhost check on worker node)"; break; }
  kill -0 "${PRIMARY_PBSDSH_PID}" 2>/dev/null || {
    echo "[MASTER][FATAL] primary pbsdsh died"
    dump_worker_logs
    exit 10
  }
  sleep 1
done
[[ -f "${PRIMARY_READY}" ]] || { echo "[MASTER][FATAL] primary never healthy"; dump_worker_logs; exit 11; }

# ===================================================================
# Verify primary is reachable FROM THIS (head/client) node
# ===================================================================
echo "[MASTER] verifying primary reachable from head node …"
HEAD_CAN_REACH=false
for i in $(seq 1 60); do
  if curl -fsS --max-time 2 "${PRIMARY_URL}/v1/meta" >/dev/null 2>&1; then
    echo "[MASTER] head node can reach primary at ${PRIMARY_URL}"
    HEAD_CAN_REACH=true
    break
  fi
  sleep 1
done
if [[ "${HEAD_CAN_REACH}" != "true" ]]; then
  echo "[MASTER][FATAL] head node CANNOT reach primary at ${PRIMARY_URL}" >&2
  dump_worker_logs
  exit 12
fi

# ===================================================================
# Wait for ALL workers on the primary node (not just wid=0).
# The node script launches them sequentially; they need time.
# ===================================================================
echo ""
echo "[MASTER] === waiting for all primary-node workers ==="
read -r -a primary_wids <<< "${NODE_WIDS[$PRIMARY_NODE_IDX]}"
for wid in "${primary_wids[@]}"; do
  (( wid == 0 )) && continue  # already confirmed
  nidx="$(worker_node_index "${wid}")"
  hshort="$(echo "${ALL_NODES[$nidx]}" | cut -d. -f1)"
  echo -n "[MASTER] waiting for wid=${wid} on ${hshort} … "
  if wait_for_worker "${wid}" "${WORKER_HEALTH_TIMEOUT}"; then
    echo "healthy"
  else
    echo "TIMEOUT (${WORKER_HEALTH_TIMEOUT}s)"
    echo "[MASTER][FATAL] primary-node worker wid=${wid} failed to start" >&2
    dump_worker_logs
    exit 13
  fi
done
echo "[MASTER] all primary-node workers healthy"

# ===================================================================
# Launch secondary nodes one at a time with stabilization checks
# ===================================================================
echo ""
echo "[MASTER] === launching secondary nodes (staggered) ==="
SECONDARY_PIDS=()

SECONDARY_NODE_INDICES=()
for nidx in $(echo "${!NODE_WIDS[@]}" | tr ' ' '\n' | sort -n); do
  (( nidx == PRIMARY_NODE_IDX )) && continue
  SECONDARY_NODE_INDICES+=("${nidx}")
done

if (( ${#SECONDARY_NODE_INDICES[@]} == 0 )); then
  echo "[MASTER] no secondary nodes (all workers on primary node)"
fi

for nidx in "${SECONDARY_NODE_INDICES[@]}"; do
  hshort="$(echo "${ALL_NODES[$nidx]}" | cut -d. -f1)"
  echo "[MASTER] launching node ${hshort} (idx=${nidx})"
  pbsdsh -n "${nidx}" -- bash -l "${RUN_DIR}/logs/node_${hshort}.sh" &
  SECONDARY_PIDS+=($!)

  # Wait for this node's workers to become healthy
  read -r -a node_wids <<< "${NODE_WIDS[$nidx]}"
  echo "[MASTER] waiting for node ${hshort} workers to become healthy …"
  for wid in "${node_wids[@]}"; do
    w_nidx="$(worker_node_index "${wid}")"
    w_hshort="$(echo "${ALL_NODES[$w_nidx]}" | cut -d. -f1)"
    echo -n "[MASTER]   wid=${wid} on ${w_hshort} … "
    if wait_for_worker "${wid}" "${WORKER_HEALTH_TIMEOUT}"; then
      echo "healthy"
    else
      echo "TIMEOUT"
      echo "[MASTER][WARN] wid=${wid} on ${w_hshort} not healthy after ${WORKER_HEALTH_TIMEOUT}s"
    fi
  done

  # Stabilization pause
  echo "[MASTER] pausing 5s for Raft stabilization …"
  sleep 5

  # Re-check primary
  if ! curl -fsS --max-time 5 "${PRIMARY_URL}/v1/meta" >/dev/null 2>&1; then
    echo "[MASTER][FATAL] primary died after node ${hshort} joined!" >&2
    dump_worker_logs
    exit 14
  fi
  echo "[MASTER] primary still healthy after node ${hshort} joined"
done

# ===================================================================
# Final: verify ALL workers healthy (poll with timeout, not one-shot)
# ===================================================================
echo ""
echo "[MASTER] === final verification of all ${TOTAL_WORKERS} workers ==="

ALL_HEALTHY=true
for (( wid=0; wid<TOTAL_WORKERS; wid++ )); do
  nidx="$(worker_node_index "${wid}")"
  hshort="$(echo "${ALL_NODES[$nidx]}" | cut -d. -f1)"
  ready_flag="${RUN_DIR}/logs/weaviate_${hshort}_w${wid}.ready"
  if [[ -f "${ready_flag}" ]]; then
    echo "[MASTER] wid=${wid} on ${hshort}: healthy"
  else
    # One more poll attempt for any stragglers
    echo -n "[MASTER] wid=${wid} on ${hshort}: not yet ready, waiting up to 60s … "
    if wait_for_worker "${wid}" 60; then
      echo "healthy"
    else
      echo "FAILED"
      ALL_HEALTHY=false
    fi
  fi
done

if [[ "${ALL_HEALTHY}" != "true" ]]; then
  echo "[MASTER][FATAL] some workers not healthy — aborting" >&2
  dump_worker_logs
  exit 15
fi

# ===================================================================
# Membership check: run directly on head node
# ===================================================================
echo ""
echo "[MASTER] checking cluster membership …"

MEMBERSHIP_OK=false
for attempt in $(seq 1 90); do
  body=$(curl -fsS --max-time 5 "${PRIMARY_URL}/v1/nodes" 2>/dev/null) || { sleep 2; continue; }
  count=$(echo "${body}" | python3 -c 'import sys,json; print(len(json.load(sys.stdin).get("nodes",[])))' 2>/dev/null) || { sleep 2; continue; }
  if (( count >= TOTAL_WORKERS )); then
    echo "[MASTER] cluster has ${count}/${TOTAL_WORKERS} members — ready"
    MEMBERSHIP_OK=true
    break
  fi
  echo "[MASTER] cluster ${count}/${TOTAL_WORKERS} — waiting …"
  sleep 3
done

if [[ "${MEMBERSHIP_OK}" != "true" ]]; then
  echo "[MASTER][WARN] cluster membership timed out — continuing, some shards may be missing" >&2
fi

# ===================================================================
# Final reachability check
# ===================================================================
echo ""
if curl -fsS --max-time 5 "${PRIMARY_URL}/v1/meta" >/dev/null 2>&1; then
  echo "[MASTER] primary confirmed reachable at ${PRIMARY_URL}"
else
  echo "[MASTER][FATAL] primary NOT reachable in final check" >&2
  dump_worker_logs
  exit 16
fi

echo "[MASTER] all ${TOTAL_WORKERS} workers launched and primary reachable"