#!/bin/bash

STORAGE_MEDIUM=${1:?Usage: $0 <storage_medium>}
TYPE=${2:?Usage: $0 <storage_medium> <type>}

RANK="${PMI_RANK:-${PMIX_RANK:-${OMPI_COMM_WORLD_RANK:-}}}"

if [[ "$STORAGE_MEDIUM" == "memory" ]]; then
    TARGET_BASE="/dev/shm/milvusDir"
    (( RANK == 0 )) && echo "${TYPE} using memory for persistence"

DAOS_ARGS=()
elif [[ "$STORAGE_MEDIUM" == "DAOS" ]]; then
    DAOS_POOL="radix-io"
    DAOS_CONT="vectorDBTesting"
    TARGET_BASE="/tmp/${DAOS_POOL}/${DAOS_CONT}/${myDIR}/milvusDir"
    (( RANK == 0 )) && echo "${TYPE} using DAOS for persistence"
    
    APPTAINER_ARGS+=(
        --bind "/home/treewalker/daos_lib64:/opt/daos/lib64:ro"
        --env LD_LIBRARY_PATH=/opt/daos/lib64
    )

elif [[ "$STORAGE_MEDIUM" == "lustre" ]]; then
    TARGET_BASE="./milvusDir"

    (( RANK == 0 )) && echo "Minio using lustre for persistence"
elif [[ "$STORAGE_MEDIUM" == "SSD" ]]; then
    TARGET_BASE="/local/scratch/milvusDir"

    (( RANK == 0 )) && echo "${TYPE} using SSD for persistence"

else
    (( RANK == 0 )) && echo "Error: unknown STORAGE_MEDIUM '$STORAGE_MEDIUM'" >&2
    exit 1
fi

mkdir -p ${TYPE}

python3 net_mapping.py --rank ${RANK} --name ${TYPE}
MY_IP_ADDR=$(jq -er '.hsn0.ipv4[0]' "${TYPE}${RANK}.json")
mv "${TYPE}${RANK}.json" ${TYPE}/

sleep $((RANK * 5))
OUTPUT_FILE="${TYPE}_registry.txt"
echo "${RANK},${MY_IP_ADDR},9000" >> $OUTPUT_FILE


# create configuration for micro-service component (basically just set its IP in the config file)
rm -fr $TARGET_BASE/${TYPE}${RANK}/
mkdir -p $TARGET_BASE/${TYPE}${RANK}/

python3 replace.py --mode ${TYPE} --rank $RANK
cp -r ./configs/ $TARGET_BASE/${TYPE}${RANK}/

mkdir -p ./workerOut/

apptainer exec --fakeroot \
  --writable-tmpfs \
  --pwd /milvus \
  --env TYPE=$TYPE \
  --env MILVUSCONF=/milvus/configs/ \
  -B ./execute.sh:/milvus/app_execute.sh \
  -B ./workerOut/:/workerOut/ \
  -B ${BASE_DIR}/cpuMilvus/:/milvus/ \
  -B $TARGET_BASE/${TYPE}${RANK}/:/var/lib/milvus \
  -B $TARGET_BASE/${TYPE}${RANK}/configs/${TYPE}${RANK}.yaml:/milvus/configs/milvus.yaml \
  milvus.sif bash app_execute.sh FALSE $RANK > ${TYPE}/${TYPE}${RANK}.out 2>&1


