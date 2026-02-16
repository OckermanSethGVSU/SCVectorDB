#!/bin/bash

STORAGE_MEDIUM=${1:?Usage: $0 <storage_medium>}

if [[ "$STORAGE_MEDIUM" == "memory" ]]; then
    TARGET_BASE="/dev/shm/"
    ETCD_BASE=$TARGET_BASE
    
    (( RANK == 0 )) && echo "ETCD using memory for persistence"

DAOS_ARGS=()
elif [[ "$STORAGE_MEDIUM" == "DAOS" ]]; then
    DAOS_POOL="radix-io"
    DAOS_CONT="vectorDBTesting"
    TARGET_BASE="/tmp/${DAOS_POOL}/${DAOS_CONT}/${myDIR}/milvusDir"
    (( RANK == 0 )) && echo "ETCD using memory for persistence"
    APPTAINER_ARGS+=(
        --bind "/home/treewalker/daos_lib64:/opt/daos/lib64:ro"
        --env LD_LIBRARY_PATH=/opt/daos/lib64
    )
    ETCD_BASE=./milvusDir/


elif [[ "$STORAGE_MEDIUM" == "lustre" ]]; then
    TARGET_BASE="./milvusDir"
    ETCD_BASE=$TARGET_BASE

    (( RANK == 0 )) && echo "ETCD using memory for persistence"
elif [[ "$STORAGE_MEDIUM" == "SSD" ]]; then
    TARGET_BASE="/local/scratch/milvusDir"
    ETCD_BASE=$TARGET_BASE

    (( RANK == 0 )) && echo "ETCD using memory for persistence"

else
    (( RANK == 0 )) && echo "Error: unknown STORAGE_MEDIUM '$STORAGE_MEDIUM'" >&2
    exit 1
fi

python3 net_mapping.py --rank 0 --name etcd
IP_ADDR=$(jq -r '.hsn0.ipv4[0]' etcd.json)

mkdir -p $ETCD_BASE/volumes/etcd_volume
apptainer exec --fakeroot \
  --writable-tmpfs \
  --env http_proxy= --env https_proxy= --env HTTP_PROXY= --env HTTPS_PROXY= \
  --env NO_PROXY= --env no_proxy= \
  --env MILVUSCONF=/milvus/configs/ \
  --env ETCD_AUTO_COMPACTION_MODE=revision \
  --env ETCD_AUTO_COMPACTION_RETENTION=1000 \
  --env ETCD_QUOTA_BACKEND_BYTES=4294967296 \
  -B $ETCD_BASE/volumes/etcd_volume:/etcd \
   etcd_v3.5.18.sif \
  etcd \
    --advertise-client-urls=http://${IP_ADDR}:2379 \
    --listen-client-urls=http://0.0.0.0:2379 \
    --data-dir /etcd > etcd.out 2>&1 

