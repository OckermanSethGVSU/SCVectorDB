#!/bin/bash

set -u

MISSING_ONLY=false
if [[ "${1:-}" == "--missing-only" ]]; then
    MISSING_ONLY=true
fi

required_paths=(
    "milvus.sif"
    "etcd_v3.5.18.sif"
    "minio.sif"
    "cpuMilvus"
    "milvusSetup/execute.sh"
    "milvusSetup/launch_etcd.sh"
    "milvusSetup/launch_minio.sh"
    "milvusSetup/launch_milvus_part.sh"
    "milvusSetup/standaloneLaunch.sh"
    "generalPython/net_mapping.py"
    "generalPython/replace.py"
    "generalPython/profile.py"
    "generalPython/poll.py"
    "generalPython/status.py"
    "insert/main.sh"
    "insert/setup_collection.py"
    "insert/multi_client_summary.py"
    "goCode/multiClientInsert/multiClientInsert"
    "goCode/multiClientInsert/main.go"
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
    echo "Missing dependencies detected for Milvus."
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
