#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SIF_DIR="${QDRANT_SIF_DIR:-$SCRIPT_DIR/sifs}"
SIF_NAME="${QDRANT_SIF_NAME:-qdrant.sif}"
IMAGE_URI="${QDRANT_IMAGE_URI:-docker://qdrant/qdrant:latest}"
SIF_PATH="$SIF_DIR/$SIF_NAME"
TMP_PATH="$SIF_PATH.tmp"

if ! command -v apptainer >/dev/null 2>&1; then
    echo "Error: apptainer is required to download the Qdrant SIF." >&2
    echo "Load it first, for example: module load apptainer" >&2
    exit 1
fi

mkdir -p "$SIF_DIR"

echo "Downloading Qdrant image:"
echo "  source: $IMAGE_URI"
echo "  target: $SIF_PATH"

rm -f "$TMP_PATH"
apptainer pull --force "$TMP_PATH" "$IMAGE_URI"
mv "$TMP_PATH" "$SIF_PATH"

echo "Qdrant SIF is ready at: $SIF_PATH"
