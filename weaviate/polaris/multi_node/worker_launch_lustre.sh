#!/bin/bash
set -euo pipefail

module use /soft/modulefiles
module load spack-pe-base/0.10.1
module use /soft/spack/testing/0.10.1/modulefiles
module load apptainer/1.4.1
module load e2fsprogs 2>/dev/null || true

export BASE_SCRATCH_DIR="${BASE_SCRATCH_DIR:-/local/scratch/${USER}}"
export APPTAINER_TMPDIR="${APPTAINER_TMPDIR:-${BASE_SCRATCH_DIR}/apptainer-tmpdir}"
export APPTAINER_CACHEDIR="${APPTAINER_CACHEDIR:-${BASE_SCRATCH_DIR}/apptainer-cachedir}"
mkdir -p "${APPTAINER_TMPDIR}" "${APPTAINER_CACHEDIR}"

SIF="${SIF_PATH:-weaviate_latest.sif}"
PORT="${PORT:-8080}"
GRPC_PORT="${GRPC_PORT:-50051}"
ASYNC_INDEXING="${ASYNC_INDEXING:-true}"

WEAVIATE_DATA_DIR="${WEAVIATE_DATA_DIR:?WEAVIATE_DATA_DIR must be set}"
WEAVIATE_HOSTNAME="${WEAVIATE_HOSTNAME:-$(hostname -s)}"
WEAVIATE_ADVERTISE_ADDR="${WEAVIATE_ADVERTISE_ADDR:-$(getent ahostsv4 "$(hostname -s)" | awk '{print $1; exit}')}"
BACKEND_NAME="${BACKEND_NAME:-lustre}"

mkdir -p "${WEAVIATE_DATA_DIR}"

echo "[WORKER] Host: $(hostname)"
echo "[WORKER] CWD: $(pwd)"
echo "[WORKER] SIF: ${SIF}"
echo "[WORKER] PORT: ${PORT}"
echo "[WORKER] GRPC_PORT: ${GRPC_PORT}"
echo "[WORKER] ASYNC_INDEXING: ${ASYNC_INDEXING}"
echo "[WORKER] BACKEND_NAME: ${BACKEND_NAME}"
echo "[WORKER] WEAVIATE_DATA_DIR: ${WEAVIATE_DATA_DIR}"
echo "[WORKER] WEAVIATE_HOSTNAME: ${WEAVIATE_HOSTNAME}"
echo "[WORKER] WEAVIATE_ADVERTISE_ADDR: ${WEAVIATE_ADVERTISE_ADDR}"
echo "[WORKER] apptainer: $(which apptainer)"
apptainer --version
ls -ld "${WEAVIATE_DATA_DIR}" || true

exec apptainer exec --fakeroot \
  --writable-tmpfs \
  -B "${WEAVIATE_DATA_DIR}:/var/lib/weaviate" \
  --env AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
  --env PERSISTENCE_DATA_PATH=/var/lib/weaviate \
  --env DEFAULT_VECTORIZER_MODULE=none \
  --env ENABLE_MODULES="" \
  --env QUERY_DEFAULTS_LIMIT=20 \
  --env ASYNC_INDEXING="${ASYNC_INDEXING}" \
  --env LIMIT_RESOURCES=true \
  --env CLUSTER_HOSTNAME="${WEAVIATE_HOSTNAME}" \
  --env CLUSTER_ADVERTISE_ADDR="${WEAVIATE_ADVERTISE_ADDR}" \
  --env RAFT_BOOTSTRAP_EXPECT=1 \
  --env RAFT_ENABLE_ONE_NODE_RECOVERY=true \
  --env CLUSTER_GOSSIP_BIND_PORT=7946 \
  --env CLUSTER_DATA_BIND_PORT=7100 \
  --env GRPC_PORT="${GRPC_PORT}" \
  --env GRPC_MAX_MESSAGE_SIZE=500000000 \
  "${SIF}" \
  weaviate --host 0.0.0.0 --port "${PORT}" --scheme http