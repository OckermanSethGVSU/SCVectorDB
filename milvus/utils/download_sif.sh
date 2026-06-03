#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MILVUS_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
VERSION="${1:-latest}"
if [[ "$VERSION" != latest && "$VERSION" != v* ]]; then
    VERSION="v$VERSION"
fi

SIF_DIR="${MILVUS_SIF_DIR:-$MILVUS_DIR/sifs}"
SAFE_VERSION="${VERSION//[^A-Za-z0-9_.-]/_}"
SIF_NAME="${MILVUS_SIF_NAME:-milvus_${SAFE_VERSION}.sif}"
IMAGE_URI="${MILVUS_IMAGE_URI:-docker://milvusdb/milvus:$VERSION}"
SIF_PATH="$SIF_DIR/$SIF_NAME"
TMP_PATH="$SIF_PATH.tmp"

if (( $# > 1 )); then
    echo "Usage: $0 [latest|VERSION]" >&2
    echo "Example: $0 v2.6.15" >&2
    exit 1
fi

if ! command -v apptainer >/dev/null 2>&1; then
    echo "Error: apptainer is required to download the Milvus SIF." >&2
    echo "Load it first, for example: module load apptainer" >&2
    exit 1
fi

mkdir -p "$SIF_DIR"

echo "Downloading Milvus image:"
echo "  source: $IMAGE_URI"
echo "  target: $SIF_PATH"

rm -f "$TMP_PATH"
apptainer pull --force "$TMP_PATH" "$IMAGE_URI"
mv "$TMP_PATH" "$SIF_PATH"

echo "Milvus SIF is ready at: $SIF_PATH"