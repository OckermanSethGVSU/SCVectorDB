#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONTAINER_NAME="${QDRANT_LOCAL_NAME:-qdrant-local}"
IMAGE="${QDRANT_LOCAL_IMAGE:-qdrant/qdrant:latest}"
HOST="${QDRANT_LOCAL_HOST:-127.0.0.1}"
HTTP_PORT="${QDRANT_LOCAL_HTTP_PORT:-6333}"
GRPC_PORT="${QDRANT_LOCAL_GRPC_PORT:-6334}"
P2P_PORT="${QDRANT_LOCAL_P2P_PORT:-6335}"
DATA_DIR="${QDRANT_LOCAL_DATA_DIR:-$ROOT_DIR/.local/qdrant/storage}"
COLLECTION_NAME="${QDRANT_LOCAL_COLLECTION:-singleShard}"
DISTANCE="${QDRANT_LOCAL_DISTANCE:-Cosine}"
RANDOM_COUNT="${QDRANT_LOCAL_RANDOM_COUNT:-100}"
RANDOM_NPY_PATH="${QDRANT_LOCAL_RANDOM_NPY_PATH:-$ROOT_DIR/random.npy}"

if command -v docker >/dev/null 2>&1; then
    CONTAINER_RUNTIME="docker"
elif command -v podman >/dev/null 2>&1; then
    CONTAINER_RUNTIME="podman"
else
    echo "Neither docker nor podman is installed. Cannot launch a local Qdrant instance." >&2
    exit 1
fi

mkdir -p "$DATA_DIR"

if ! command -v curl >/dev/null 2>&1; then
    echo "curl is required to initialize the local Qdrant collection." >&2
    exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
    echo "python3 is required to infer vector dimensions and generate random.npy." >&2
    exit 1
fi

if [[ -n "${QDRANT_LOCAL_VECTOR_DIM:-}" ]]; then
    VECTOR_DIM="$QDRANT_LOCAL_VECTOR_DIM"
elif [[ -n "${QUERY_FILEPATH:-}" && -f "${QUERY_FILEPATH}" ]]; then
    VECTOR_DIM="$(python3 - "${QUERY_FILEPATH}" <<'PY'
import sys
import numpy as np
path = sys.argv[1]
arr = np.load(path)
if arr.ndim != 2:
    raise SystemExit(f"{path} must be a 2D array, got shape {arr.shape}")
print(arr.shape[1])
PY
)"
elif [[ -n "${DATA_FILEPATH:-}" && -f "${DATA_FILEPATH}" ]]; then
    VECTOR_DIM="$(python3 - "${DATA_FILEPATH}" <<'PY'
import sys
import numpy as np
path = sys.argv[1]
arr = np.load(path)
if arr.ndim != 2:
    raise SystemExit(f"{path} must be a 2D array, got shape {arr.shape}")
print(arr.shape[1])
PY
)"
else
    VECTOR_DIM=128
fi

if "$CONTAINER_RUNTIME" ps --format '{{.Names}}' | grep -Fxq "$CONTAINER_NAME"; then
    echo "Qdrant container '$CONTAINER_NAME' is already running."
else
    if "$CONTAINER_RUNTIME" ps -a --format '{{.Names}}' | grep -Fxq "$CONTAINER_NAME"; then
        echo "Starting existing Qdrant container '$CONTAINER_NAME'..."
        "$CONTAINER_RUNTIME" start "$CONTAINER_NAME" >/dev/null
    else
        echo "Launching Qdrant container '$CONTAINER_NAME' from image '$IMAGE'..."
        "$CONTAINER_RUNTIME" run -d \
            --name "$CONTAINER_NAME" \
            -p "${HTTP_PORT}:6333" \
            -p "${GRPC_PORT}:6334" \
            -p "${P2P_PORT}:6335" \
            -v "${DATA_DIR}:/qdrant/storage" \
            "$IMAGE" >/dev/null
    fi
fi

printf '0,%s,%s\n' "$HOST" "$P2P_PORT" > "$ROOT_DIR/ip_registry.txt"

echo "Waiting for Qdrant health check on http://${HOST}:${HTTP_PORT}/healthz ..."
for _ in {1..30}; do
    if curl -fsS "http://${HOST}:${HTTP_PORT}/healthz" >/dev/null; then
        echo "Qdrant is ready."
        echo "REST: http://${HOST}:${HTTP_PORT}"
        echo "gRPC: ${HOST}:${GRPC_PORT}"
        echo "P2P: ${HOST}:${P2P_PORT}"
        echo "Registry written to ${ROOT_DIR}/ip_registry.txt"
        break
    fi
    sleep 1
done

if ! curl -fsS "http://${HOST}:${HTTP_PORT}/healthz" >/dev/null; then
    echo "Qdrant did not become healthy within 30 seconds." >&2
    echo "Inspect logs with: ${CONTAINER_RUNTIME} logs ${CONTAINER_NAME}" >&2
    exit 1
fi

echo "Recreating collection '${COLLECTION_NAME}' with vector dim ${VECTOR_DIM} and distance ${DISTANCE}..."
curl -fsS -X DELETE "http://${HOST}:${HTTP_PORT}/collections/${COLLECTION_NAME}" >/dev/null || true
curl -fsS -X PUT "http://${HOST}:${HTTP_PORT}/collections/${COLLECTION_NAME}" \
    -H 'Content-Type: application/json' \
    -d "{
      \"vectors\": {
        \"size\": ${VECTOR_DIM},
        \"distance\": \"${DISTANCE}\"
      },
      \"shard_number\": 1,
      \"replication_factor\": 1,
      \"optimizers_config\": {
        \"indexing_threshold\": 0
      }
    }" >/dev/null

echo "Generating ${RANDOM_NPY_PATH} with shape (${RANDOM_COUNT}, ${VECTOR_DIM})..."
python3 - "${RANDOM_NPY_PATH}" "${RANDOM_COUNT}" "${VECTOR_DIM}" <<'PY'
import sys
import numpy as np

path = sys.argv[1]
count = int(sys.argv[2])
dim = int(sys.argv[3])
rng = np.random.default_rng(42)
arr = rng.random((count, dim), dtype=np.float32)
np.save(path, arr)
print(path)
PY

echo "Collection '${COLLECTION_NAME}' is ready."
echo "random.npy: ${RANDOM_NPY_PATH}"
echo "If your Rust code uses ip_registry.txt, it should now point to this local node."
