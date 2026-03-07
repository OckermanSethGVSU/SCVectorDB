#!/bin/bash
#PBS -N sc_insert_mem
#PBS -l select=2:system=polaris
#PBS -l place=scatter
#PBS -l filesystems=home:eagle
#PBS -l walltime=06:00:00
#PBS -q capacity
#PBS -A SuperBERT
#PBS -o workflow.out
#PBS -e workflow.err

set -euo pipefail

# ---------- paths ----------
BASE_DIR="/eagle/projects/argonne_tpc/songoh/vdb/weaviate/multi_node"
SIF_PATH="${BASE_DIR}/weaviate_latest.sif"
WORKER_SH="${BASE_DIR}/worker_launch_mem.sh"
GO_DIR="${BASE_DIR}/go_client"
GO_TOOLCHAIN_DIR="${BASE_DIR}/opt/go1.25.0"

export GOTOOLCHAIN=local
export PATH="${GO_TOOLCHAIN_DIR}/bin:${PATH}"

DATA_FILE="/eagle/argonne_tpc/sockerman/pes2oEmbeddings/embeddings.npy"

# ---------- experiment settings ----------
VEC_DIM="${VEC_DIM:-2560}"
MEASURE_VECS="${MEASURE_VECS:-1000000}"
START_ROW="${START_ROW:-0}"

REST_PORT="${REST_PORT:-8080}"
GRPC_PORT="${GRPC_PORT:-50051}"

BS_MIN_POW="${BS_MIN_POW:-2}"
BS_MAX_POW="${BS_MAX_POW:-15}"
CLASS_NAME="${CLASS_NAME:-PES2O}"

READY_WAIT_SEC="${READY_WAIT_SEC:-420}"

CLIENT_OVERALL_SEC="${CLIENT_OVERALL_SEC:-20000}"
CLIENT_RPC_TIMEOUT_SEC="${CLIENT_RPC_TIMEOUT_SEC:-1800}"
QUEUE_DRAIN_TIMEOUT_SEC="${QUEUE_DRAIN_TIMEOUT_SEC:-20000}"
QUEUE_POLL_MS="${QUEUE_POLL_MS:-500}"
QUEUE_STABLE_POLLS="${QUEUE_STABLE_POLLS:-3}"

DYNAMIC_THRESHOLD="${DYNAMIC_THRESHOLD:-10001000}"
ASYNC_INDEXING="${ASYNC_INDEXING:-true}"

BACKEND_NAME="memory"

# ---------- modules ----------
module use /soft/modulefiles
module load spack-pe-base/0.10.1
module use /soft/spack/testing/0.10.1/modulefiles
module load apptainer/1.4.1
module load e2fsprogs 2>/dev/null || true

echo "[INFO] Weaviate Go insert sweep (Polaris 2-node, backend=${BACKEND_NAME})"
echo "[HOST] $(hostname)"
echo "[DATE] $(date -u)"
echo "[INFO] go version: $(go version || true)"
echo "[INFO] apptainer version: $(apptainer --version)"
echo "[INFO] measure=${MEASURE_VECS} start_row=${START_ROW}"
echo "[INFO] bs_pow_range=[${BS_MIN_POW}, ${BS_MAX_POW}]"
echo "[INFO] dynamic_threshold=${DYNAMIC_THRESHOLD}"
echo "[INFO] ASYNC_INDEXING=${ASYNC_INDEXING}"

# ---------- sanity ----------
[[ -x "$(command -v go)" ]] || { echo "[FATAL] go not found at ${GO_TOOLCHAIN_DIR}"; exit 2; }
[[ -f "${SIF_PATH}" ]] || { echo "[FATAL] missing SIF: ${SIF_PATH}"; exit 3; }
[[ -f "${WORKER_SH}" ]] || { echo "[FATAL] missing worker script: ${WORKER_SH}"; exit 4; }
[[ -f "${DATA_FILE}" ]] || { echo "[FATAL] missing data file: ${DATA_FILE}"; exit 5; }
[[ -f "${GO_DIR}/insert_sweep_master.go" ]] || { echo "[FATAL] missing go file: ${GO_DIR}/insert_sweep_master.go"; exit 6; }
[[ -f "${GO_DIR}/go.mod" ]] || { echo "[FATAL] missing go.mod in ${GO_DIR}"; exit 7; }
[[ -d "${GO_DIR}/vendor" ]] || { echo "[FATAL] missing vendor dir in ${GO_DIR}"; exit 8; }
[[ -f "${GO_DIR}/vendor/modules.txt" ]] || { echo "[FATAL] missing vendor/modules.txt in ${GO_DIR}"; exit 9; }

# ---------- job-local scratch / caches ----------
JOB_SCRATCH="/local/scratch/${USER}/${PBS_JOBID}"
export BASE_SCRATCH_DIR="${JOB_SCRATCH}"
export APPTAINER_TMPDIR="${JOB_SCRATCH}/apptainer-tmpdir"
export APPTAINER_CACHEDIR="${JOB_SCRATCH}/apptainer-cachedir"
mkdir -p "${APPTAINER_TMPDIR}" "${APPTAINER_CACHEDIR}"

export GOPATH="${JOB_SCRATCH}/go"
export GOMODCACHE="${GOPATH}/pkg/mod"
export GOCACHE="${GOPATH}/cache"
mkdir -p "${GOMODCACHE}" "${GOCACHE}"

export GOFLAGS="-mod=vendor -buildvcs=false"
export GOWORK=off

# ---------- run directory ----------
TS=$(date +"%y%m%d_%H%M%S")
RUN_DIR="${BASE_DIR}/sc_insert_mem_${TS}"
LOG_DIR="${RUN_DIR}/logs"
mkdir -p "${RUN_DIR}" "${LOG_DIR}"

# Explicit tmpfs-backed data dir for fair comparison
WEAVIATE_DATA_DIR="/dev/shm/${USER}/${PBS_JOBID}/weaviate/node0"

exec > >(tee -a "${RUN_DIR}/workflow.out") 2> >(tee -a "${RUN_DIR}/workflow.err" >&2)

echo "[INFO] Base dir: ${BASE_DIR}"
echo "[INFO] Run dir: ${RUN_DIR}"
echo "[INFO] Backend: ${BACKEND_NAME}"
echo "[INFO] Weaviate data dir: ${WEAVIATE_DATA_DIR}"
echo "[INFO] PBS_NODEFILE:"
cat "${PBS_NODEFILE}"

client_node="$(head -n1 "${PBS_NODEFILE}")"
worker_node="$(sed -n '2p' "${PBS_NODEFILE}")"

[[ -n "${client_node}" ]] || { echo "[FATAL] could not determine client node"; exit 10; }
[[ -n "${worker_node}" ]] || { echo "[FATAL] could not determine worker node"; exit 11; }

echo "[INFO] client_node=${client_node}"
echo "[INFO] worker_node=${worker_node}"

# ---------- worker hostname/IP ----------
mpirun -n 1 --ppn 1 --cpu-bind none --host "${worker_node}" bash -lc '
set -euo pipefail
hn="$(hostname -s)"
ip="$(getent ahostsv4 "${hn}" | awk "{print \$1; exit}")"
if [[ -z "${ip}" ]]; then
  ip="$(hostname -I | awk "{print \$1}")"
fi
echo "${hn}" > "'"${RUN_DIR}"'/worker.hostname"
echo "${ip}" > "'"${RUN_DIR}"'/worker.ip"
' > "${LOG_DIR}/worker_identify.out" 2> "${LOG_DIR}/worker_identify.err"

worker_hostname="$(tr -d '[:space:]' < "${RUN_DIR}/worker.hostname" || true)"
worker_ip="$(tr -d '[:space:]' < "${RUN_DIR}/worker.ip" || true)"

[[ -n "${worker_hostname}" ]] || { echo "[FATAL] Could not determine worker hostname"; exit 12; }
[[ -n "${worker_ip}" ]] || { echo "[FATAL] Could not determine worker IP"; exit 13; }

echo "[INFO] worker_hostname=${worker_hostname}"
echo "[INFO] worker_ip=${worker_ip}"

# ---------- prepare worker tmpfs data dir ----------
mpirun -n 1 --ppn 1 --cpu-bind none --host "${worker_node}" bash -lc "
set -euo pipefail
mkdir -p '${WEAVIATE_DATA_DIR}'
rm -rf '${WEAVIATE_DATA_DIR:?}'/*
df -h /dev/shm || true
" > "${LOG_DIR}/worker_backend_prepare.out" 2> "${LOG_DIR}/worker_backend_prepare.err"

# ---------- cleanup ----------
WEAVIATE_LAUNCH_PID=""
cleanup() {
  echo "[INFO] cleanup: stopping weaviate launcher"
  [[ -n "${WEAVIATE_LAUNCH_PID}" ]] && kill "${WEAVIATE_LAUNCH_PID}" 2>/dev/null || true
  mpirun -n 1 --ppn 1 --cpu-bind none --host "${worker_node}" bash -lc "
pkill -f '/bin/weaviate' || true
pkill -f ' weaviate ' || true
rm -rf '/dev/shm/${USER}/${PBS_JOBID}/weaviate' || true
" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# ---------- launch weaviate ----------
echo "[INFO] launching weaviate on ${worker_node} (${BACKEND_NAME} backend)"
mpirun -n 1 --ppn 1 --cpu-bind none --host "${worker_node}" bash -lc "
set -euo pipefail
cd '${RUN_DIR}'
env \
  BASE_SCRATCH_DIR='${BASE_SCRATCH_DIR}' \
  APPTAINER_TMPDIR='${APPTAINER_TMPDIR}' \
  APPTAINER_CACHEDIR='${APPTAINER_CACHEDIR}' \
  SIF_PATH='${SIF_PATH}' \
  PORT='${REST_PORT}' \
  GRPC_PORT='${GRPC_PORT}' \
  ASYNC_INDEXING='${ASYNC_INDEXING}' \
  WEAVIATE_DATA_DIR='${WEAVIATE_DATA_DIR}' \
  WEAVIATE_HOSTNAME='${worker_hostname}' \
  WEAVIATE_ADVERTISE_ADDR='${worker_ip}' \
  BACKEND_NAME='${BACKEND_NAME}' \
  bash '${WORKER_SH}'
" > "${LOG_DIR}/weaviate.out" 2> "${LOG_DIR}/weaviate.err" &
WEAVIATE_LAUNCH_PID=$!

sleep 5

# ---------- quick worker diagnostics ----------
echo "[INFO] quick worker-side diagnostics"
mpirun -n 1 --ppn 1 --cpu-bind none --host "${worker_node}" bash -lc "
set +e
echo '--- hostname ---'
hostname
echo '--- listening ports (${REST_PORT}/${GRPC_PORT}/7946) ---'
ss -ltnp | egrep ':${REST_PORT}|:${GRPC_PORT}|:7946' || true
echo '--- local curl /v1/meta ---'
curl -v --max-time 5 http://127.0.0.1:${REST_PORT}/v1/meta || true
echo '--- tmpfs usage ---'
df -h /dev/shm || true
du -sh '/dev/shm/${USER}/${PBS_JOBID}/weaviate' 2>/dev/null || true
" > "${LOG_DIR}/worker_diag_initial.out" 2> "${LOG_DIR}/worker_diag_initial.err" || true

# ---------- readiness ----------
echo "[INFO] waiting for local readiness on worker: http://127.0.0.1:${REST_PORT}/v1/meta"
worker_ready=0
for i in $(seq 1 "${READY_WAIT_SEC}"); do
  if mpirun -n 1 --ppn 1 --cpu-bind none --host "${worker_node}" \
       bash -lc "curl -fsS --max-time 2 http://127.0.0.1:${REST_PORT}/v1/meta >/dev/null" \
       >/dev/null 2>&1; then
    worker_ready=1
    echo "[INFO] worker-local readiness OK after ${i}s"
    break
  fi
  sleep 1
done

if [[ "${worker_ready}" -ne 1 ]]; then
  echo "[FATAL] Weaviate never became ready locally on the worker."
  tail -n 200 "${LOG_DIR}/weaviate.err" || true
  tail -n 200 "${LOG_DIR}/weaviate.out" || true
  tail -n 200 "${LOG_DIR}/worker_diag_initial.out" || true
  exit 14
fi

echo "[INFO] waiting for remote readiness from client: http://${worker_ip}:${REST_PORT}/v1/meta"
client_ready=0
for i in $(seq 1 60); do
  if mpirun -n 1 --ppn 1 --cpu-bind none --host "${client_node}" \
       bash -lc "curl -fsS --max-time 2 http://${worker_ip}:${REST_PORT}/v1/meta >/dev/null" \
       >/dev/null 2>&1; then
    client_ready=1
    echo "[INFO] client->worker readiness OK after ${i}s"
    break
  fi
  sleep 1
done

if [[ "${client_ready}" -ne 1 ]]; then
  echo "[FATAL] Weaviate is healthy locally on worker, but not reachable from client node."
  tail -n 200 "${LOG_DIR}/worker_diag_initial.out" || true
  tail -n 200 "${LOG_DIR}/weaviate.err" || true
  exit 15
fi

# ---------- build client ----------
BIN_PATH="${RUN_DIR}/insert_sweep_go"
echo "[INFO] building client: ${BIN_PATH}"

mpirun -n 1 --ppn 1 --cpu-bind none --host "${client_node}" bash -lc "
set -euo pipefail
export PATH='${GO_TOOLCHAIN_DIR}/bin:'\"\$PATH\"
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
  exit 16
}

# ---------- run experiment ----------
OUT_JSON="${RUN_DIR}/bs_sweep.json"
echo "[INFO] running sweep -> ${OUT_JSON}"

set +e
mpirun -n 1 --ppn 1 --cpu-bind none --host "${client_node}" bash -lc "
set -euo pipefail
export WORKER_IP='${worker_ip}'
export REST_PORT='${REST_PORT}'
export GRPC_PORT='${GRPC_PORT}'
export DATA_FILE='${DATA_FILE}'
export VEC_DIM='${VEC_DIM}'
export MEASURE_VECS='${MEASURE_VECS}'
export START_ROW='${START_ROW}'
export DYNAMIC_THRESHOLD='${DYNAMIC_THRESHOLD}'

NO_PROXY='localhost,127.0.0.1,${worker_ip}' \
no_proxy='localhost,127.0.0.1,${worker_ip}' \
http_proxy= https_proxy= HTTP_PROXY= HTTPS_PROXY= \
'${BIN_PATH}' \
  -out '${OUT_JSON}' \
  -minpow '${BS_MIN_POW}' \
  -maxpow '${BS_MAX_POW}' \
  -class '${CLASS_NAME}' \
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
  tail -n 200 "${LOG_DIR}/client.err" || true
  echo "------ weaviate.err (tail) ------"
  tail -n 200 "${LOG_DIR}/weaviate.err" || true
fi

if [[ -f "${OUT_JSON}" ]]; then
  echo "[DONE] JSON exists:"
  ls -lh "${OUT_JSON}"
else
  echo "[WARN] JSON missing: ${OUT_JSON}"
fi

echo "[DONE] logs: ${LOG_DIR}"
exit $rc