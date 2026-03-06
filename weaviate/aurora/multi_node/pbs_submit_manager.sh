#!/bin/bash -l
###############################################################################
# pbs_submit_manager.sh — central controller for Weaviate scaling experiments
#
# Usage:
#   WORKER_COUNTS="1 2 4 8 16 32" QUEUE=debug-scaling ./pbs_submit_manager.sh
#   WORKER_COUNTS="4" QUEUE=debug-scaling ./pbs_submit_manager.sh
#   DRY_RUN=1 WORKER_COUNTS="1 2 4 8 16 32" ./pbs_submit_manager.sh
###############################################################################
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ "${SCRIPT_DIR}" == /var/spool/pbs/* ]] || [[ -n "${PBS_JOBID:-}" ]]; then
  echo "[FATAL] Run interactively, not via qsub." >&2; exit 2
fi
cd "${SCRIPT_DIR}"

# ---------- task ----------
TASK="${TASK:-insert}"
case "${TASK}" in insert|index|query) ;; *) echo "[FATAL] unknown TASK=${TASK}"; exit 1 ;; esac

# ---------- PBS ----------
QUEUE="${QUEUE:-debug-scaling}"
ACCOUNT="${ACCOUNT:-radix-io}"
WALLTIME="${WALLTIME:-06:00:00}"
FILESYSTEMS="${FILESYSTEMS:-home:flare}"

# ---------- cluster geometry ----------
MAX_WORKERS_PER_NODE="${MAX_WORKERS_PER_NODE:-4}"
CLIENTS_PER_WORKER="${CLIENTS_PER_WORKER:-16}"
WORKER_COUNTS_STR="${WORKER_COUNTS:-1 2 4 8 16 32}"
read -r -a WORKER_COUNTS_ARR <<< "${WORKER_COUNTS_STR}"

# ---------- paths ----------
SIF_PATH="${SIF_PATH:-/flare/radix-io/songoh/weaviate/weaviate_latest.sif}"
DATA_FILE="${DATA_FILE:-/lus/flare/projects/AuroraGPT/sockerman/pes2oEmbeddings/embeddings.npy}"
GO_CLIENT_BIN="${GO_CLIENT_BIN:-${SCRIPT_DIR}/go_client/weaviate_client}"

# ---------- workload ----------
VEC_DIM="${VEC_DIM:-2560}"
MEASURE_VECS="${MEASURE_VECS:-10000000}"
START_ROW="${START_ROW:-0}"
INDEX_TYPE="${INDEX_TYPE:-flat}"
CLASS_NAME="${CLASS_NAME:-PES2O}"
BS_POW="${BS_POW:-8}"

# ---------- timeouts ----------
CLIENT_RPC_TIMEOUT_SEC="${CLIENT_RPC_TIMEOUT_SEC:-1800}"
CLIENT_OVERALL_SEC="${CLIENT_OVERALL_SEC:-7200}"
QUEUE_TIMEOUT_SEC="${QUEUE_TIMEOUT_SEC:-7200}"

# ---------- misc ----------
DRY_RUN="${DRY_RUN:-0}"
WIPE_WEAVIATE_DATA="${WIPE_WEAVIATE_DATA:-1}"

# ---------- ports ----------
REST_PORT_BASE="${REST_PORT_BASE:-8080}"
GRPC_PORT_BASE="${GRPC_PORT_BASE:-50051}"
GOSSIP_PORT_BASE="${GOSSIP_PORT_BASE:-7100}"
DATA_PORT_BASE="${DATA_PORT_BASE:-7200}"

# ---------- validation ----------
JOB_SCRIPT="${SCRIPT_DIR}/${TASK}.sh"
[[ -f "${JOB_SCRIPT}" ]]    || { echo "[FATAL] missing ${JOB_SCRIPT}"; exit 3; }
[[ -f "${SIF_PATH}" ]]      || { echo "[FATAL] missing SIF: ${SIF_PATH}"; exit 4; }
[[ -x "${GO_CLIENT_BIN}" ]] || {
  echo "[FATAL] Go client binary not found: ${GO_CLIENT_BIN}"
  echo "  Build: cd go_client && go build -mod=vendor -buildvcs=false -o weaviate_client ./main.go"
  exit 5
}

ceil_div() { echo $(( ($1 + $2 - 1) / $2 )); }
mkdir -p "${SCRIPT_DIR}/runs"

echo "============================================================"
echo " Weaviate scaling sweep"
echo "   TASK              = ${TASK}"
echo "   WORKER_COUNTS     = ${WORKER_COUNTS_STR}"
echo "   MAX_WORKERS/NODE  = ${MAX_WORKERS_PER_NODE}"
echo "   CLIENTS/WORKER    = ${CLIENTS_PER_WORKER}"
echo "   INDEX_TYPE        = ${INDEX_TYPE}"
echo "   MEASURE_VECS      = ${MEASURE_VECS}"
echo "   BS_POW            = ${BS_POW}  (batch=$((1 << BS_POW)))"
echo "   QUEUE             = ${QUEUE}"
echo "   WALLTIME          = ${WALLTIME}"
echo "   DRY_RUN           = ${DRY_RUN}"
echo "============================================================"

SUBMITTED=0

for total_workers in "${WORKER_COUNTS_ARR[@]}"; do
  [[ "${total_workers}" =~ ^[0-9]+$ ]] || { echo "[FATAL] bad worker count: ${total_workers}"; exit 6; }
  (( total_workers > 0 ))              || { echo "[FATAL] worker count must be >0"; exit 7; }

  worker_nodes="$(ceil_div "${total_workers}" "${MAX_WORKERS_PER_NODE}")"
  total_nodes=$(( 1 + worker_nodes ))
  shard_count="${total_workers}"
  total_clients=$(( total_workers * CLIENTS_PER_WORKER ))

  ts="$(date +%Y-%m-%d_%H%M%S)"
  run_name="${TASK}_W${total_workers}_N${worker_nodes}_MWPN${MAX_WORKERS_PER_NODE}_CPW${CLIENTS_PER_WORKER}_${ts}"
  run_dir="${SCRIPT_DIR}/runs/${run_name}"
  mkdir -p "${run_dir}/logs" "${run_dir}/clients"

  cp -f "${JOB_SCRIPT}"                          "${run_dir}/job.sh"
  cp -f "${SCRIPT_DIR}/worker_launch_master.sh"   "${run_dir}/worker_launch_master.sh"
  cp -f "${GO_CLIENT_BIN}"                        "${run_dir}/weaviate_client"
  chmod +x "${run_dir}/job.sh" "${run_dir}/worker_launch_master.sh" "${run_dir}/weaviate_client"

  cat > "${run_dir}/params.env" <<PARAMS
TASK=${TASK}
RUN_NAME=${run_name}
RUN_DIR=${run_dir}
SIF_PATH=${SIF_PATH}
TOTAL_WORKERS=${total_workers}
WORKER_NODES=${worker_nodes}
MAX_WORKERS_PER_NODE=${MAX_WORKERS_PER_NODE}
SHARD_COUNT=${shard_count}
CLIENTS_PER_WORKER=${CLIENTS_PER_WORKER}
TOTAL_CLIENTS=${total_clients}
DATA_FILE=${DATA_FILE}
VEC_DIM=${VEC_DIM}
MEASURE_VECS=${MEASURE_VECS}
START_ROW=${START_ROW}
CLASS_NAME=${CLASS_NAME}
INDEX_TYPE=${INDEX_TYPE}
BS_POW=${BS_POW}
REST_PORT_BASE=${REST_PORT_BASE}
GRPC_PORT_BASE=${GRPC_PORT_BASE}
GOSSIP_PORT_BASE=${GOSSIP_PORT_BASE}
DATA_PORT_BASE=${DATA_PORT_BASE}
CLIENT_RPC_TIMEOUT_SEC=${CLIENT_RPC_TIMEOUT_SEC}
CLIENT_OVERALL_SEC=${CLIENT_OVERALL_SEC}
QUEUE_TIMEOUT_SEC=${QUEUE_TIMEOUT_SEC}
WIPE_WEAVIATE_DATA=${WIPE_WEAVIATE_DATA}
PARAMS

  PASS_VARS=""
  while IFS= read -r line; do
    [[ -z "${line}" || "${line}" == \#* ]] && continue
    [[ -n "${PASS_VARS}" ]] && PASS_VARS+=","
    PASS_VARS+="${line}"
  done < "${run_dir}/params.env"

  echo ""
  echo "[SUBMIT] ${run_name}"
  echo "         workers=${total_workers}  worker_nodes=${worker_nodes}  pbs_nodes=${total_nodes}  shards=${shard_count}  clients=${total_clients}"

  QSUB_CMD=(
    qsub
    -A "${ACCOUNT}" -q "${QUEUE}"
    -l "select=${total_nodes}" -l "place=scatter"
    -l "walltime=${WALLTIME}" -l "filesystems=${FILESYSTEMS}"
    -N "${run_name}"
    -o "${run_dir}/workflow.out" -e "${run_dir}/workflow.err"
    -v "${PASS_VARS}"
    "${run_dir}/job.sh"
  )

  if [[ "${DRY_RUN}" == "1" ]]; then
    echo "         [DRY RUN] ${QSUB_CMD[*]}"
  else
    JOB_ID=$("${QSUB_CMD[@]}")
    echo "         submitted: ${JOB_ID}"
    echo "${JOB_ID}" > "${run_dir}/jobid.txt"
    SUBMITTED=$((SUBMITTED + 1))
  fi
done

echo ""
echo "[SUBMIT] done — ${SUBMITTED} job(s) submitted.  Results → ${SCRIPT_DIR}/runs/"