#!/bin/bash

# get passed in variables
RANK=${1:?Usage: $0 <rank>}
RANK=$((RANK))
STORAGE_MEDIUM=${2:?Usage: $0 <rank> <storage_medium>}
USEPERF=${3:?Usage: $0 <rank> <storage_medium>}

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

DAOS_ARGS=()

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

else
    (( RANK == 0 )) && echo "Error: unknown STORAGE_MEDIUM '$STORAGE_MEDIUM'" >&2
    exit 1
fi



# === Launch Qdrant Nodes ===
apptainer exec \
    --fakeroot \
    --writable-tmpfs \
    --env QDRANT__GPU__INDEXING=0 \
    --pwd /qdrant \
    --bind ./perf/:/perf/ \
    --bind ./ip_registry.txt:/ip_registry.txt \
    --bind ./qdrant:/qdrant/qdrant \
    --bind ./launch.sh:/qdrant/launch.sh \
    "${APPTAINER_ARGS[@]}" \
    --bind ${TARGET_BASE}/data/node$RANK:/qdrant/storage \
    --bind ${TARGET_BASE}/config/node$RANK:/qdrant/config \
    --bind ${TARGET_BASE}/snapshots/node$RANK:/qdrant/snapshots \
    qdrant.sif bash launch.sh $IP_ADDR $P2P_PORT $RANK $USEPERF > "rank${RANK}.out" 2>&1 &
PID=$! 
wait $PID

# --bind ./qdrantDIR/data/node$RANK:/qdrant/storage \
# --bind ./qdrantDIR/config/node$RANK:/qdrant/config \
# --bind ./qdrantDIR/snapshots/node$RANK:/qdrant/snapshots \

# apptainer shell \
#     --fakeroot \
#     --writable-tmpfs \
#     --nv \
#     --env QDRANT__GPU__INDEXING=0 \
#     --pwd /qdrant \
#     --bind ./perf/:/perf/ \
#     --bind ./ip_registry.txt:/ip_registry.txt \
#     --bind ./qdrant:/qdrant/qdrant \
#     --bind ./qdrantDIR/data/node0:/qdrant/storage \
#     --bind ./qdrantDIR/config/node0:/qdrant/config \
#     --bind ./qdrantDIR/snapshots/node0:/qdrant/snapshots \
#     --bind ./launch.sh:/qdrant/launch.sh \
#     qdrant.sif
# # If you are not rank 0, bootstrap on rank 0

# if [[ $RANK -eq 0 ]]; then
#     # /home/treewalker/bin/perf record --delay 280000 -F 99 --call-graph dwarf -g --proc-map-timeout 5000 -o perf.data.qdrant -- \
#     apptainer exec \
#                 --fakeroot \
#                 --writable-tmpfs \
#                 --nv \
#                 --env QDRANT__GPU__INDEXING=0 \
#                 --pwd /qdrant \
#                 --bind ./perf/:/perf/ \
#                 --bind ./qdrant:/qdrant/qdrant \
#                 --bind /local/scratch/qdrant/data/node$RANK:/qdrant/storage \
#                 --bind /local/scratch/qdrant/config/node$RANK:/qdrant/config \
#                 --bind /local/scratch/qdrant/snapshots/node$RANK:/qdrant/snapshots \
#                 --bind ./launch.sh:/qdrant/launch.sh \
#                 qdrant.sif bash launch.sh $IP_ADDR $P2P_PORT > "rank${RANK}.out" 2>&1 &

#                 # bash -lc "/usr/bin/apt-get update && /usr/bin/apt-get install -y linux-perf && perf record -F 99 --call-graph fp -g --proc-map-timeout 5000 -o /mnt/perfData/perf.data -- ./qdrant --uri "http://${IP_ADDR}:${P2P_PORT}" --config-path /qdrant/config/config.yaml"  > "rank${RANK}.out" 2>&1 &
#                 # bash -lc "/usr/bin/apt-get update && /usr/bin/apt-get install -y linux-perf && perf record --delay 330000 -F 99 --call-graph fp -g --proc-map-timeout 5000 -o /mnt/perfData/perf.data -- ./qdrant --uri "http://${IP_ADDR}:${P2P_PORT}" --config-path /qdrant/config/config.yaml"  > "rank${RANK}.out" 2>&1 &
#                 # bash -lc "/usr/bin/apt-get update && /usr/bin/apt-get install -y strace && strace -c --summary-wall-clock ./qdrant --uri "http://${IP_ADDR}:${P2P_PORT}" --config-path /qdrant/config/config.yaml"  > "rank${RANK}.out" 2>&1 &
#                 # bash -lc "/usr/bin/apt-get update && /usr/bin/apt-get install -y linux-perf && perf stat --delay 330000 -e  page-faults,major-faults,minor-faults,context-switches,migrations,user_time,system_time,instructions,cycles,cache-misses,cache-references  -- ./qdrant --uri "http://${IP_ADDR}:${P2P_PORT}" --config-path /qdrant/config/config.yaml"  > "rank${RANK}.out" 2>&1 &
#     PID=$!  
# else 
#     bootstrapIP=$(sed -n '1p' ip_registry.txt | cut -d',' -f2)   
#     apptainer exec \
#     --fakeroot \
#     --pwd /qdrant \
#     --bind /local/scratch/qdrant/data/node$RANK:/qdrant/storage \
#     --bind /local/scratch/qdrant/config/node$RANK:/qdrant/config \
#     --bind /local/scratch/qdrant/snapshots/node$RANK:/qdrant/snapshots \
#     qdrant.sif \
#     ./qdrant --uri "http://${IP_ADDR}:${P2P_PORT}" --bootstrap http://$bootstrapIP:6335 --config-path /qdrant/config/config.yaml > "rank${RANK}.out" 2>&1 &
    
#     PID=$!

# fi

# wait $PID

# while true; do
#   if [ -f stop.txt ]; then
#     echo "stop.txt detected, sending SIGTERM to $PID"
#     pkill -INT -P "$PID"
#     break
#   fi

#   # Also break if the process already exited
#   if ! kill -0 "$PID" 2>/dev/null; then
#     echo "Process $PID exited on its own"
#     break
#   fi

#   sleep 1
# done


# while true; do
#   if [ -f stop.txt ]; then
#     echo "stop.txt detected, sending SIGTERM to $PID"
#     # QDRANT_PIDS=$(pgrep -fa './qdrant' | grep -v 'perf'  | awk '{print $1}')
#     QDRANT_PIDS=$(pgrep -fa './qdrant' | grep -v 'strace'  | awk '{print $1}')
#     echo $QDRANT_PIDS
#     kill -INT $QDRANT_PIDS
#     # pkill -INT -P "$PID"
#     break
#   fi

#   # Also break if the process already exited
#   if ! kill -0 "$PID" 2>/dev/null; then
#     echo "Process $PID exited on its own"
#     break
#   fi

#   sleep 1
# done




# apptainer shell --fakeroot --writable-tmpfs --pwd /qdrant --bind "./perfData:/mnt/perfData"  --bind ./qdrant:/qdrant/qdrant qdrant.sif

# apptainer shell --fakeroot --writable-tmpfs --pwd /qdrant qdrant.sif