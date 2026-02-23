#!/bin/bash

STORAGE_MEDIUM=${1:?Usage: $0 <storage_medium>}
RANK="${PMI_RANK:-${PMIX_RANK:-${OMPI_COMM_WORLD_RANK:-}}}"




if [[ "$STORAGE_MEDIUM" == "memory" ]]; then
    TARGET_BASE="/dev/shm/"
    
    # (( RANK == 0 )) && echo "Minio using memory for persistence"
    echo "Minio ${RANK} using memory for persistence"

DAOS_ARGS=()
elif [[ "$STORAGE_MEDIUM" == "DAOS" ]]; then
    DAOS_POOL="radix-io"
    DAOS_CONT="vectorDBTesting"
    TARGET_BASE="/tmp/${DAOS_POOL}/${DAOS_CONT}/${myDIR}/milvusDir"
    (( RANK == 0 )) && echo "Minio using DAOS for persistence"
    APPTAINER_ARGS+=(
        --bind "/home/treewalker/daos_lib64:/opt/daos/lib64:ro"
        --env LD_LIBRARY_PATH=/opt/daos/lib64
    )


elif [[ "$STORAGE_MEDIUM" == "lustre" ]]; then
    TARGET_BASE="./milvusDir"

    (( RANK == 0 )) && echo "Minio using lustre for persistence"
    echo "Minio ${RANK} using lustre for persistence"

elif [[ "$STORAGE_MEDIUM" == "SSD" ]]; then
    TARGET_BASE="/local/scratch/milvusDir"

    (( RANK == 0 )) && echo "Minio using SSD for persistence"

else
    (( RANK == 0 )) && echo "Error: unknown STORAGE_MEDIUM '$STORAGE_MEDIUM'" >&2
    exit 1
fi


python3 net_mapping.py --rank ${RANK} --name minio
sleep 3

if [[ "$MINIO_MODE" == "stripped" ]]; then

    until \
        [[ -f minio0.json ]] && jq empty minio0.json >/dev/null 2>&1 && \
        [[ -f minio1.json ]] && jq empty minio1.json >/dev/null 2>&1 && \
        [[ -f minio2.json ]] && jq empty minio2.json >/dev/null 2>&1 && \
        [[ -f minio3.json ]] && jq empty minio3.json >/dev/null 2>&1
    do
        sleep 1
    done

    MY_IP_ADDR=$(jq -er ".hsn${RANK}.ipv4[0]" "minio${RANK}.json")

    sleep $((RANK * 5))
    DATA_PORT=$((9000 + 100 * RANK))
    CONSOLE_PORT=$((9001 + 100 * RANK))
    OUTPUT_FILE="minio_registry.txt"
    echo "${RANK},${MY_IP_ADDR},${DATA_PORT}" >> $OUTPUT_FILE


    IP_ADDR0=$(jq -er '.hsn0.ipv4[0]' minio0.json)
    IP_ADDR1=$(jq -er '.hsn1.ipv4[0]' minio1.json)
    IP_ADDR2=$(jq -er '.hsn2.ipv4[0]' minio2.json)
    IP_ADDR3=$(jq -er '.hsn3.ipv4[0]' minio3.json)
    ENDPOINTS=(
    "http://${IP_ADDR0}:9000/data0"
    "http://${IP_ADDR1}:9100/data1"
    "http://${IP_ADDR2}:9200/data2"
    "http://${IP_ADDR3}:9300/data3"
    )
    

    rm -fr $TARGET_BASE/volumes/minio_volume${RANK}
    mkdir -p $TARGET_BASE/volumes/minio_volume${RANK}
    apptainer exec --fakeroot \
    --writable-tmpfs \
    --env http_proxy= --env https_proxy= --env HTTP_PROXY= --env HTTPS_PROXY= \
    --env NO_PROXY= --env no_proxy= \
    --env MINIO_ROOT_USER=minioadmin \
    --env MINIO_ROOT_PASSWORD=minioadmin \
    -B $TARGET_BASE/volumes/minio_volume${RANK}:/data${RANK} \
    minio.sif \
    minio server "${ENDPOINTS[@]}" \
    --address ${MY_IP_ADDR}:${DATA_PORT} --console-address ${MY_IP_ADDR}:${CONSOLE_PORT} > minio${RANK}.out 2>&1

elif [[ "$MINIO_MODE" == "single" ]]; then
    MY_IP_ADDR=$(jq -er '.hsn0.ipv4[0]' "minio${RANK}.json")
    OUTPUT_FILE="minio_registry.txt"
    echo "${RANK},${MY_IP_ADDR},9000" >> $OUTPUT_FILE

    rm -fr $TARGET_BASE/volumes/minio_volume${RANK}
    mkdir -p $TARGET_BASE/volumes/minio_volume${RANK}
    apptainer exec --fakeroot \
    --writable-tmpfs \
    --env http_proxy= --env https_proxy= --env HTTP_PROXY= --env HTTPS_PROXY= \
    --env NO_PROXY= --env no_proxy= \
    --env MINIO_ROOT_USER=minioadmin \
    --env MINIO_ROOT_PASSWORD=minioadmin \
    -B $TARGET_BASE/volumes/minio_volume${RANK}:/data$ \
    minio.sif \
    minio server /data \
    --address ${MY_IP_ADDR}:9000 --console-address ${MY_IP_ADDR}:9001 > minio${RANK}.out 2>&1

else
    echo "Unrecognized MINIO_MODE: ${MINIO_MODE}"
fi