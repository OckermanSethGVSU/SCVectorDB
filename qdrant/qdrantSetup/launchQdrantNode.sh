#!/bin/bash

# get passed in variables
RANK=${1:?Usage: $0 <rank>}
RANK=$((RANK))
STORAGE_MEDIUM=${2:?Usage: $0 <rank> <storage_medium>}

# get ipv4
python3 mapping.py --rank $RANK
group=$(( RANK / 4 ))
IP_ADDR=$(jq -r '.hsn0.ipv4[0]' interfaces${group}.json)
P2P_PORT=$((6335 + RANK * 100))

# register IP,port into file
OUTPUT_FILE="ip_registry.txt"
echo "${RANK},${IP_ADDR},${P2P_PORT}" >> $OUTPUT_FILE


if [[ "$STORAGE_MEDIUM" == "memory" ]]; then
    TARGET_BASE="/dev/shm/qdrantDir"
    (( RANK == 0 )) && echo "Using memory for persistence"

APPTAINER_ARGS=()
elif [[ "$STORAGE_MEDIUM" == "DAOS" ]]; then
    DAOS_POOL="radix-io"
    DAOS_CONT="vectorDBTesting"
    TARGET_BASE="/tmp/${DAOS_POOL}/${DAOS_CONT}/${myDIR}/qdrantDir"
    echo $TARGET_BASE
    (( RANK == 0 )) && echo "Using DAOS for persistence"
    APPTAINER_ARGS+=(
        --bind "/home/treewalker/daos_lib64:/opt/daos/lib64:ro"
        --env LD_LIBRARY_PATH=/opt/daos/lib64
    )

elif [[ "$STORAGE_MEDIUM" == "lustre" ]]; then
    TARGET_BASE="./qdrantDir"
    (( RANK == 0 )) && echo "Using lustre for persistence"

elif [[ "$STORAGE_MEDIUM" == "SSD" ]]; then
    TARGET_BASE="/local/scratch/qdrantDir"
    (( RANK == 0 )) && echo "Using SSD for persistence"

else
    (( RANK == 0 )) && echo "Error: unknown STORAGE_MEDIUM '$STORAGE_MEDIUM'" >&2
    exit 1
fi


GPU_ARGS=()
if [[ "$GPU_INDEX" == "True" ]]; then
    GPU_ARGS+=(
        --env QDRANT__GPU__INDEXING=1
        --nv
    )
else
    GPU_ARGS+=(--env QDRANT__GPU__INDEXING=0)

fi

BUILD_ARGS=()
if [ -n "$QDRANT_EXECUTABLE" ]; then
    BUILD_ARGS+=(--bind ./qdrant:/qdrant/qdrant)
fi 


# === Launch Qdrant Nodes ===
apptainer exec \
    --fakeroot \
    --writable-tmpfs \
    --pwd /qdrant \
    --bind ./perf/:/perf/ \
    --bind ./ip_registry.txt:/ip_registry.txt \
    --bind ./launch.sh:/qdrant/launch.sh \
    --bind ${TARGET_BASE}/data/node$RANK:/qdrant/storage \
    --bind ${TARGET_BASE}/config/node$RANK:/qdrant/config \
    --bind ${TARGET_BASE}/snapshots/node$RANK:/qdrant/snapshots \
    --env PERF=$PERF \
    "${BUILD_ARGS[@]}" \
    "${APPTAINER_ARGS[@]}" \
    "${GPU_ARGS[@]}" \
    qdrant.sif bash launch.sh $IP_ADDR $P2P_PORT $RANK > "rank${RANK}.out" 2>&1 &
PID=$! 
wait $PID
