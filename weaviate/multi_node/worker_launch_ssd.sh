#!/bin/bash
set -euo pipefail

# Load modules on the worker node (match the PBS script)
module use /soft/modulefiles
module load spack-pe-base/0.10.1
module use /soft/spack/testing/0.10.1/modulefiles
module load apptainer/1.4.1
module load e2fsprogs 2>/dev/null || true

export BASE_SCRATCH_DIR=/local/scratch
export APPTAINER_TMPDIR="${BASE_SCRATCH_DIR}/apptainer-tmpdir"
export APPTAINER_CACHEDIR="${BASE_SCRATCH_DIR}/apptainer-cachedir"
mkdir -p "${APPTAINER_TMPDIR}" "${APPTAINER_CACHEDIR}"

SIF="${SIF_PATH:-weaviate_latest.sif}"
PORT="${PORT:-8080}"
GRPC_PORT="${GRPC_PORT:-50051}"
ASYNC_INDEXING="${ASYNC_INDEXING:-true}"

# SSD_DATA_DIR is passed from the PBS script; it points to /local/scratch/...
SSD_DATA_DIR="${SSD_DATA_DIR:?SSD_DATA_DIR must be set}"

echo "[WORKER] Host: $(hostname)"
echo "[WORKER] CWD:  $(pwd)"
echo "[WORKER] SIF:  ${SIF}"
echo "[WORKER] PORT: ${PORT}  GRPC_PORT: ${GRPC_PORT}"
echo "[WORKER] ASYNC_INDEXING: ${ASYNC_INDEXING}"
echo "[WORKER] STORAGE BACKEND: SSD (${SSD_DATA_DIR}/node0)"
echo "[WORKER] apptainer: $(which apptainer)"
apptainer --version

# Ensure the SSD data directory exists
mkdir -p "${SSD_DATA_DIR}/node0"

echo "[WORKER] Starting Weaviate (SSD backend)..."
apptainer exec --fakeroot \
  --writable-tmpfs \
  -B "${SSD_DATA_DIR}/node0":/var/lib/weaviate \
  --env AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
  --env PERSISTENCE_DATA_PATH="/var/lib/weaviate" \
  --env CLUSTER_HOSTNAME=node0 \
  --env GRPC_PORT="${GRPC_PORT}" \
  --env GRPC_MAX_MESSAGE_SIZE=500000000 \
  --env ASYNC_INDEXING="${ASYNC_INDEXING}" \
  "${SIF}" \
  weaviate --host 0.0.0.0 --port "${PORT}" --scheme http