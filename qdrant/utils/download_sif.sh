#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
QDRANT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
VERSION="${1:-latest}"
if [[ "$VERSION" != latest && "$VERSION" != v* ]]; then
    VERSION="v$VERSION"
fi

SIF_DIR="${QDRANT_SIF_DIR:-$QDRANT_DIR/sifs}"
SAFE_VERSION="${VERSION//[^A-Za-z0-9_.-]/_}"
SIF_NAME="${QDRANT_SIF_NAME:-qdrant_${SAFE_VERSION}.sif}"
IMAGE_URI="${QDRANT_IMAGE_URI:-docker://qdrant/qdrant:$VERSION}"
SIF_PATH="$SIF_DIR/$SIF_NAME"
TMP_PATH="$SIF_PATH.tmp"

if (( $# > 1 )); then
    echo "Usage: $0 [latest|VERSION]" >&2
    echo "Example: $0 v1.13.4" >&2
    exit 1
fi

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
