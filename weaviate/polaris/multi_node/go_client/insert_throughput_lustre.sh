#!/bin/bash
#PBS -N sc_insert_throughput_lustre
#PBS -l select=2:system=polaris
#PBS -l place=scatter
#PBS -l filesystems=home:eagle
#PBS -l walltime=06:00:00
#PBS -q preemptable
#PBS -A SuperBERT
#PBS -o workflow.out
#PBS -e workflow.err

set -euo pipefail

# ---------- paths ----------
BASE_DIR="/eagle/projects/argonne_tpc/songoh/vdb/weaviate/multi_node"
SIF_PATH="${BASE_DIR}/weaviate_latest.sif"
WORKER_SH="${BASE_DIR}/worker_launch_lustre.sh"
GO_DIR="${BASE_DIR}/go_client"

GO_TOOLCHAIN_DIR="${BASE_DIR}/opt/go1.25.0"
export GOTOOLCHAIN=local
export PATH="${GO_TOOLCHAIN_DIR}/bin:${PATH}"

DATA_FILE="/eagle/argonne_tpc/sockerman/pes2oEmbeddings/embeddings.npy"

# ---------- experiment settings ----------
VEC_DIM="${VEC_DIM:-2560}"

MEASURE_VECS="${MEASURE_VECS:-10000000}"
START_ROW="${START_ROW:-0}"

REST_PORT="${REST_PORT:-8080}"
GRPC_PORT="${GRPC_PORT:-50051}"

BS_MIN_POW="${BS_MIN_POW:-10}"
BS_MAX_POW="${BS_MAX_POW:-10}"

CLASS_NAME="${CLASS_NAME:-PES2O}"

READY_WAIT_SEC="${READY_WAIT_SEC:-240}"

CLIENT_OVERALL_SEC="${CLIENT_OVERALL_SEC:-20000}"
CLIENT_RPC_TIMEOUT_SEC="${CLIENT_RPC_TIMEOUT_SEC:-1800}"
QUEUE_DRAIN_TIMEOUT_SEC="${QUEUE_DRAIN_TIMEOUT_SEC:-20000}"
QUEUE_POLL_MS="${QUEUE_POLL_MS:-500}"
QUEUE_STABLE_POLLS="${QUEUE_STABLE_POLLS:-3}"

DYNAMIC_THRESHOLD="${DYNAMIC_THRESHOLD:-10001000}"
export ASYNC_INDEXING="${ASYNC_INDEXING:-true}"

BACKEND_NAME="lustre"

# ---------- modules ----------
module use /soft/modulefiles
module load spack-pe-base/0.10.1
module use /soft/spack/testing/0.10.1/modulefiles
module load apptainer/1.4.1
module load e2fsprogs 2>/dev/null || true

echo "[INFO] Weaviate Go insert sweep (Polaris 2-node, Lustre backend)"
echo "[HOST] $(hostname)"
echo "[DATE] $(date -u)"
echo "[INFO] go version: $(go version || true)"
echo "[INFO] apptainer version: $(apptainer --version || true)"
echo "[INFO] measure=${MEASURE_VECS} start_row=${START_ROW}"
echo "[INFO] bs_pow_range=[${BS_MIN_POW}, ${BS_MAX_POW}]"
echo "[INFO] dynamic_threshold=${DYNAMIC_THRESHOLD}"
echo "[INFO] ASYNC_INDEXING=${ASYNC_INDEXING}"
echo "[INFO] STORAGE BACKEND: lustre"

# ---------- sanity ----------
[[ -x "$(command -v go)" ]] || { echo "[FATAL] go not found at ${GO_TOOLCHAIN_DIR}"; exit 2; }
[[ -f "${SIF_PATH}" ]]      || { echo "[FATAL] missing SIF: ${SIF_PATH}"; exit 3; }
[[ -f "${WORKER_SH}" ]]     || { echo "[FATAL] missing worker script: ${WORKER_SH}"; exit 4; }
[[ -f "${DATA_FILE}" ]]     || { echo "[FATAL] missing data file: ${DATA_FILE}"; exit 5; }
[[ -f "${GO_DIR}/insert_sweep_master.go" ]] || {
  echo "[FATAL] missing go file: ${GO_DIR}/insert_sweep_master.go"
  exit 6
}

# ---------- scratch caches ----------
export BASE_SCRATCH_DIR="/local/scratch/${USER}"
export APPTAINER_TMPDIR="${BASE_SCRATCH_DIR}/apptainer-tmpdir"
export APPTAINER_CACHEDIR="${BASE_SCRATCH_DIR}/apptainer-cachedir"
mkdir -p "${APPTAINER_TMPDIR}" "${APPTAINER_CACHEDIR}"

export GOPATH="${BASE_SCRATCH_DIR}/go-${PBS_JOBID}"
export GOMODCACHE="${GOPATH}/pkg/mod"
export GOCACHE="${GOPATH}/cache"
mkdir -p "${GOMODCACHE}" "${GOCACHE}"

export GOFLAGS="-mod=vendor -buildvcs=false"
export GOWORK=off

# ---------- run directory ----------
TS=$(date +"%y%m%d_%H%M")
RUN_DIR="${BASE_DIR}/sc_insert_throughput_lustre_${TS}"
LOG_DIR="${RUN_DIR}/logs"
mkdir -p "${RUN_DIR}" "${LOG_DIR}"

exec > >(tee -a "${RUN_DIR}/workflow.out") 2> >(tee -a "${RUN_DIR}/workflow.err" >&2)

echo "[INFO] Run dir: ${RUN_DIR}"
echo "[INFO] PBS_NODEFILE:"
cat "${PBS_NODEFILE}"

client_node="$(head -n1 "${PBS_NODEFILE}")"
worker_node="$(sed -n '2p' "${PBS_NODEFILE}")"

[[ -n "${client_node}" ]] || { echo "[FATAL] failed to get client node"; exit 7; }
[[ -n "${worker_node}" ]] || { echo "[FATAL] failed to get worker node"; exit 8; }

echo "[INFO] client_node=${client_node}"
echo "[INFO] worker_node=${worker_node}"

# ---------- worker IP ----------
mpirun -n 1 --ppn 1 --cpu-bind none --host "${worker_node}" \
  bash -lc "hostname -I | awk '{print \$1}'" > "${RUN_DIR}/worker.ip"

worker_ip="$(tr -d '[:space:]' < "${RUN_DIR}/worker.ip" || true)"
[[ -n "${worker_ip}" ]] || { echo "[FATAL] Could not determine worker IP"; exit 10; }
echo "[INFO] Worker IP: ${worker_ip}"

# ---------- explicit Lustre data dir ----------
WEAVIATE_DATA_DIR="${RUN_DIR}/weaviate/node0"
WEAVIATE_HOSTNAME="$(echo "${worker_node}" | cut -d. -f1)"

echo "[INFO] WEAVIATE_DATA_DIR=${WEAVIATE_DATA_DIR}"
echo "[INFO] WEAVIATE_HOSTNAME=${WEAVIATE_HOSTNAME}"

# Pre-create and inspect Lustre dir
mkdir -p "${WEAVIATE_DATA_DIR}"
mpirun -n 1 --ppn 1 --cpu-bind none --host "${worker_node}" bash -lc "
set -e
mkdir -p '${WEAVIATE_DATA_DIR}'
ls -ld '${WEAVIATE_DATA_DIR}' || true
"

# ---------- cleanup ----------
WEAVIATE_LAUNCH_PID=""
cleanup() {
  echo "[INFO] cleanup: stopping weaviate launcher"
  [[ -n "${WEAVIATE_LAUNCH_PID}" ]] && kill "${WEAVIATE_LAUNCH_PID}" 2>/dev/null || true

  mpirun -n 1 --ppn 1 --cpu-bind none --host "${worker_node}" bash -lc "
    pkill -f weaviate || true
  " >/dev/null 2>&1 || true
}
trap cleanup EXIT

# ---------- launch weaviate ----------
echo "[INFO] launching weaviate on ${worker_node} (lustre backend)"
mpirun -n 1 --ppn 1 --cpu-bind none --host "${worker_node}" \
  bash -lc "
    set -e
    cd '${RUN_DIR}'
    export SIF_PATH='${SIF_PATH}'
    export PORT='${REST_PORT}'
    export GRPC_PORT='${GRPC_PORT}'
    export ASYNC_INDEXING='${ASYNC_INDEXING}'
    export BASE_SCRATCH_DIR='${BASE_SCRATCH_DIR}'
    export APPTAINER_TMPDIR='${APPTAINER_TMPDIR}'
    export APPTAINER_CACHEDIR='${APPTAINER_CACHEDIR}'
    export WEAVIATE_DATA_DIR='${WEAVIATE_DATA_DIR}'
    export WEAVIATE_HOSTNAME='${WEAVIATE_HOSTNAME}'
    export WEAVIATE_ADVERTISE_ADDR='${worker_ip}'
    export BACKEND_NAME='${BACKEND_NAME}'
    bash '${WORKER_SH}'
  " \
  > "${LOG_DIR}/weaviate.out" 2> "${LOG_DIR}/weaviate.err" &

WEAVIATE_LAUNCH_PID=$!

# ---------- readiness ----------
echo "[INFO] waiting for readiness: http://${worker_ip}:${REST_PORT}/v1/meta"
ready=0
for i in $(seq 1 "${READY_WAIT_SEC}"); do
  if mpirun -n 1 --ppn 1 --cpu-bind none --host "${client_node}" \
       bash -lc "curl -fsS --max-time 2 http://${worker_ip}:${REST_PORT}/v1/meta >/dev/null" \
       >/dev/null 2>&1; then
    ready=1
    echo "[INFO] ready (after ${i}s)"
    break
  fi
  sleep 1
done

if [[ "${ready}" -ne 1 ]]; then
  echo "[FATAL] Weaviate never became ready."
  echo "------ weaviate.err (tail) ------"
  tail -n 200 "${LOG_DIR}/weaviate.err" || true
  echo "------ weaviate.out (tail) ------"
  tail -n 200 "${LOG_DIR}/weaviate.out" || true
  exit 11
fi

# ---------- build client ----------
BIN_PATH="${RUN_DIR}/insert_sweep_go"
echo "[INFO] building client: ${BIN_PATH}"

mpirun -n 1 --ppn 1 --cpu-bind none --host "${client_node}" bash -lc "
set -e
export PATH='${GO_TOOLCHAIN_DIR}/bin:'\"'\$PATH'\"
export GOPATH='${GOPATH}'
export GOMODCACHE='${GOMODCACHE}'
export GOCACHE='${GOCACHE}'
export GOTOOLCHAIN=local
export GOWORK=off
export GOFLAGS='-mod=vendor -buildvcs=false'
mkdir -p '${GOMODCACHE}' '${GOCACHE}'
cd '${GO_DIR}'
go build -o '${BIN_PATH}' insert_sweep_master.go
" > "${LOG_DIR}/client_build.out" 2> "${LOG_DIR}/client_build.err"

[[ -f "${BIN_PATH}" ]] || {
  echo "[FATAL] build failed"
  tail -n 200 "${LOG_DIR}/client_build.err" || true
  exit 12
}

# ---------- run experiment ----------
OUT_JSON="${RUN_DIR}/throughput.json"
echo "[INFO] running insertion -> ${OUT_JSON}"

set +e
mpirun -n 1 --ppn 1 --cpu-bind none --host "${client_node}" bash -lc "
set -e
export WORKER_IP='${worker_ip}'
export REST_PORT='${REST_PORT}'
export GRPC_PORT='${GRPC_PORT}'
export DATA_FILE='${DATA_FILE}'
export VEC_DIM='${VEC_DIM}'
export MEASURE_VECS='${MEASURE_VECS}'
export START_ROW='${START_ROW}'
export DYNAMIC_THRESHOLD='${DYNAMIC_THRESHOLD}'

NO_PROXY='localhost,127.0.0.1,${worker_ip}' \
http_proxy= https_proxy= HTTP_PROXY= HTTPS_PROXY= \
'${BIN_PATH}' \
  -out '${OUT_JSON}' \
  -minpow '${BS_MIN_POW}' \
  -maxpow '${BS_MAX_POW}' \
  -class '${CLASS_NAME}' \
  -wait '${READY_WAIT_SEC}' \
  -overall_sec '${CLIENT_OVERALL_SEC}' \
  -rpc_sec '${CLIENT_RPC_TIMEOUT_SEC}' \
  -queue_timeout_sec '${QUEUE_DRAIN_TIMEOUT_SEC}' \
  -queue_poll_ms '${QUEUE_POLL_MS}' \
  -queue_stable_polls '${QUEUE_STABLE_POLLS}'
" > "${LOG_DIR}/client.out" 2> "${LOG_DIR}/client.err"
rc=$?
set -e

if [[ $rc -ne 0 ]]; then
  echo "[ERROR] client failed with exit code ${rc}"
  echo "------ client.err (tail) ------"
  tail -n 160 "${LOG_DIR}/client.err" || true
  echo "------ weaviate.err (tail) ------"
  tail -n 160 "${LOG_DIR}/weaviate.err" || true
fi

if [[ -f "${OUT_JSON}" ]]; then
  echo "[DONE] JSON exists:"
  ls -lh "${OUT_JSON}"
else
  echo "[WARN] JSON missing: ${OUT_JSON}"
fi

echo "[DONE] logs: ${LOG_DIR}"
exit $rc