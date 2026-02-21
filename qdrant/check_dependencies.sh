#!/bin/bash

set -u

MISSING_ONLY=false
if [[ "${1:-}" == "--missing-only" ]]; then
    MISSING_ONLY=true
fi

required_paths=(
    "qdrant.sif"
    "qdrant"
    "perf/perf"
    "qdrantSetup/launchQdrantNode.sh"
    "qdrantSetup/launch.sh"
    "generalPython/gen_dirs.py"
    "generalPython/mapping.py"
    "generalPython/profile.py"
    "generalPython/configureTopo.py"
    "insert/main.sh"
    "insert/multi_client_summary.py"
    "rustCode/multiClientUpload/multiClientUpload"
    "rustCode/multiClientUpload/src/main.rs"
)

present=()
missing=()

for path in "${required_paths[@]}"; do
    if [[ -e "$path" ]]; then
        present+=("$path")
    else
        missing+=("$path")
    fi
done

if [[ "$MISSING_ONLY" == "false" ]]; then
    echo "=== Qdrant dependency check ==="
    echo "Present (${#present[@]}):"
    for path in "${present[@]}"; do
        echo "  - $path"
    done
fi

if (( ${#missing[@]} > 0 )); then
    echo "Missing (${#missing[@]}):"
    for path in "${missing[@]}"; do
        echo "  - $path"
    done
    exit 1
fi

if [[ "$MISSING_ONLY" == "false" ]]; then
    echo "Missing (0): none"
fi

echo "All required Qdrant dependencies are present."
