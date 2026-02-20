#!/bin/bash

set -u

MISSING_ONLY=false
if [[ "${1:-}" == "--missing-only" ]]; then
    MISSING_ONLY=true
fi

required_paths=(
    "qdrant.sif"
    "qdrant"
    "perf"
    "qdrantSetup/launch.sh"
    "qdrantSetup/launchQdrantNode.sh"
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

if (( ${#missing[@]} == 0 )); then
    exit 0
fi

if [[ "$MISSING_ONLY" == "false" ]]; then
    echo "Missing dependencies detected for Qdrant."
    echo "Present (${#present[@]}):"
    for path in "${present[@]}"; do
        echo "  - $path"
    done
fi

echo "Missing (${#missing[@]}):"
for path in "${missing[@]}"; do
    echo "  - $path"
done

exit 1
