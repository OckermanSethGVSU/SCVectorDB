#!/bin/bash

RANK=${1:?Usage: $0 <rank>}
RANK=$((RANK))
STORAGE_MEDIUM=${2:?Usage: $0 <rank> <storage_medium>}
PLATFORM=${3:?Usage: $0 <rank> <storage_medium> <platform>}
TYPE=${4:?Usage: $0 <rank> <storage_medium> <platform> <type>}
WAL=${5:?Usage: $0 <rank> <storage_medium> <platform> <type> <WAL>}


ETCD_FLAG="--env ETCD_DATA_DIR=/var/lib/milvus/etcd"
if [[ "$STORAGE_MEDIUM" == "memory" ]]; then
    TARGET_BASE="/dev/shm/"
    (( RANK == 0 )) && echo "Using memory for persistence"

DAOS_ARGS=()
elif [[ "$STORAGE_MEDIUM" == "DAOS" ]]; then
    DAOS_POOL="radix-io"
    DAOS_CONT="vectorDBTesting"
    TARGET_BASE="/tmp/${DAOS_POOL}/${DAOS_CONT}/${myDIR}/milvusDir"
    (( RANK == 0 )) && echo "Using DAOS for persistence"
    APPTAINER_ARGS+=(
        --bind "/home/treewalker/daos_lib64:/opt/daos/lib64:ro"
        --env LD_LIBRARY_PATH=/opt/daos/lib64
    )
    ETCD_FLAG="--env ETCD_DATA_DIR=/dev/shm/var/lib/milvus/etcd"
    # ETCD_FLAG="--env     ETCD_FLAG="--env ETCD_DATA_DIR=/dev/shm/var/lib/milvus/etcd"


elif [[ "$STORAGE_MEDIUM" == "lustre" ]]; then
    TARGET_BASE="./milvusDir"
    ETCD_FLAG="--env ETCD_DATA_DIR=/dev/shm/var/lib/milvus/etcd"
    (( RANK == 0 )) && echo "Using lustre for persistence"
elif [[ "$STORAGE_MEDIUM" == "SSD" ]]; then
    TARGET_BASE="/local/scratch/milvusDir"
    ETCD_FLAG="--env ETCD_DATA_DIR=/dev/shm/var/lib/milvus/etcd"
    (( RANK == 0 )) && echo "Using SSD for persistence"

else
    (( RANK == 0 )) && echo "Error: unknown STORAGE_MEDIUM '$STORAGE_MEDIUM'" >&2
    exit 1
fi

# get ipv4
python3 net_mapping.py --rank $RANK
group=$(( RANK / 4 ))
IP_ADDR=$(jq -r '.hsn0.ipv4[0]' interfaces${group}.json)
echo $IP_ADDR > worker.ip


mkdir -p ${TARGET_BASE}/milvus/
mkdir -p ./workerOut/


POLARIS_BINDS=()
if [[ "$PLATFORM" == "AURORA" ]]; then
    base=$BASE_DIR
elif [[ "$PLATFORM" == "POLARIS" ]]; then
    base=$BASE_DIR
    POLARIS_BINDS+=(
        -B "/eagle/projects/argonne_tpc/sockerman/buildingFromSource/gpuMilvus/cuda-merged:/usr/local/cuda:ro"
        -B "/opt/nvidia/hpc_sdk:/opt/nvidia/hpc_sdk:ro"
        --env PLATFORM=POLARIS
    )
    
fi
# Create and pass in modified config #####
cp -r ${base}/${MILVUS_CONFIG_DIR}/configs/ .

cat << EOF > ./configs/user.yaml
# Extra config to override default milvus.yaml
EOF
cat << EOF > ./configs/embedEtcd.yaml
listen-client-urls: http://${IP_ADDR}:2379
advertise-client-urls: http://${IP_ADDR}:2379
quota-backend-bytes: 4294967296
auto-compaction-mode: revision
auto-compaction-retention: '1000'
EOF

python3 replace_unified.py --mode standalone
cp -r ./configs/ $TARGET_BASE/



PROXY_PORT=20001
METRICS_PORT=9091
STANDALONE_STORAGE_TYPE="local"
MINIO_ENV_ARGS=()

if [[ "$MINIO_MODE" == "single" ]]; then
    MINIO_IP=""
    for _ in $(seq 1 60); do
        if [[ -f minio_registry.txt ]]; then
            MINIO_IP=$(awk -F, '$1 == "0" { print $2; exit }' minio_registry.txt)
            if [[ -n "$MINIO_IP" ]]; then
                break
            fi
        fi
        sleep 1
    done

    if [[ -z "$MINIO_IP" ]]; then
        echo "Timed out waiting for minio_registry.txt to contain rank 0" >&2
        exit 1
    fi

    STANDALONE_STORAGE_TYPE="remote"
    MINIO_ENV_ARGS+=(
        --env MINIO_ADDRESS=${MINIO_IP}:9000
        --env MINIO_ACCESS_KEY_ID=minioadmin
        --env MINIO_SECRET_ACCESS_KEY=minioadmin
    )
elif [[ "$MINIO_MODE" != "off" ]]; then
    echo "Unsupported MINIO_MODE '$MINIO_MODE' for standalone" >&2
    exit 1
fi

# create proxy registry for the go insert
echo "0,${IP_ADDR},${PROXY_PORT},${METRICS_PORT}" > PROXY_registry.txt

GPU_ARGS=()
if [[ "$GPU_INDEX" == "True" ]]; then

    GPU_ARGS+=()
else
    GPU_ARGS+=(
        --env CUDA_VISIBLE_DEVICES="" 
    )
fi

CPU_ARGS=()
if [[ "$CORES" -eq 112 ]]; then                 
    CPU_ARGS+=()
else
    CPU_ARGS+=(
        --env GOMAXPROCS=$CORES
    )
fi

BUILD_ARGS=()
if [ -n "$MILVUS_BUILD_DIR" ]; then
    BUILD_ARGS+=(
        -B ${base}/${MILVUS_BUILD_DIR}/:/milvus/
        -B /usr/lib64/libatomic.so.1:/usr/lib64/libatomic.so.1
    )
fi 


if [ -n "$RESTORE_DIR" ]; then
    echo "Restoring Milvus from ${RESTORE_DIR}"

    mkdir -p ${TARGET_BASE}/milvus/data/
    cp -r $RESTORE_DIR/data/cache ${TARGET_BASE}/milvus/data/ & 
    cp -r $RESTORE_DIR/data/index_files ${TARGET_BASE}/milvus/data/ &
    cp -r $RESTORE_DIR/data/insert_log ${TARGET_BASE}/milvus/data/ &
    cp -r $RESTORE_DIR/data/stats_log ${TARGET_BASE}/milvus/data/ &
    cp -r $RESTORE_DIR/data/wp ${TARGET_BASE}/milvus/data/ &

    cp -r $RESTORE_DIR/etcd/ ${TARGET_BASE}/milvus/ &

    wait
fi 


apptainer exec --no-home --fakeroot --writable-tmpfs --nv \
    --pwd /milvus \
    --env MILVUSCONF=/milvus/configs/ \
    --env ETCD_USE_EMBED=true \
    $ETCD_FLAG \
    --env ETCD_CONFIG_PATH=/milvus/configs/embedEtcd.yaml \
    --env COMMON_STORAGETYPE=$STANDALONE_STORAGE_TYPE \
    --env DEPLOY_MODE=STANDALONE \
    --env TYPE=$TYPE \
    --env PERF=$PERF \
    --env PERF_EVENTS=$PERF_EVENTS \
    --env MILVUS_BUILD_DIR=$MILVUS_BUILD_DIR \
    --env WORKER_IP=$IP_ADDR \
    --env MILVUS_HEALTH_HOST=$IP_ADDR \
    --env MILVUS_HEALTH_PORT=$METRICS_PORT \
    --env RESTORE_DIR=$RESTORE_DIR \
    -B ./execute.sh:/milvus/app_execute.sh \
    -B ${TARGET_BASE}/configs/:/milvus/configs/ \
    -B ${base}/perfDir/:/perfDir/ \
    -B ./workerOut/:/workerOut/ \
    -B ${TARGET_BASE}/milvus:/var/lib/milvus \
    "${BUILD_ARGS[@]}" \
    "${POLARIS_BINDS[@]}" \
    "${GPU_ARGS[@]}" \
    "${CPU_ARGS[@]}" \
    "${MINIO_ENV_ARGS[@]}" \
    milvus.sif bash app_execute.sh standalone > standalone.out 2>&1

# apptainer shell --no-home --fakeroot --writable-tmpfs --nv \
#     --pwd /milvus \
#     --env MILVUSCONF=/milvus/configs/ \
#     --env ETCD_USE_EMBED=true \
#     --env ETCD_CONFIG_PATH=/milvus/configs/embedEtcd.yaml \
#     --env COMMON_STORAGETYPE=local \
#     --env DEPLOY_MODE=STANDALONE \
#     --env MILVUS_BUILD_DIR=$MILVUS_BUILD_DIR \
#     -B ./execute.sh:/milvus/app_execute.sh \
#     -B ./configs/:/milvus/configs/ \
#     -B /lus/flare/projects/radix-io/sockerman/temp/milvus/cpuMilvus/:/milvus/ \
#     -B /usr/lib64/libatomic.so.1:/usr/lib64/libatomic.so.1 \
#     milvus.sif





# cudaPath="/eagle/projects/argonne_tpc/sockerman/buildingFromSource/gpuMilvus"
# apptainer shell --no-home --fakeroot --writable-tmpfs --nv \
#     --pwd /milvus \
#     --env MILVUSCONF=/milvus/configs/ \
#     --env ETCD_USE_EMBED=true \
#     --env ETCD_DATA_DIR=/var/lib/milvus/etcd \
#     --env ETCD_CONFIG_PATH=/milvus/configs/embedEtcd.yaml \
#     --env COMMON_STORAGETYPE=local \
#     --env DEPLOY_MODE=STANDALONE \
#     -B ${base}/perfDir/:/perfDir/ \
#     -B ./workerOut/:/workerOut/ \
#     -B "./volumes/milvus:/var/lib/milvus" \
#     -B "./config/user.yaml:/milvus/configs/user.yaml" \
#     -B "./config/embedEtcd.yaml:/milvus/configs/embedEtcd.yaml" \
#     milvus.sif 

    # -B ${base}/milvus/:/milvus/ \
    # -B ${cudaPath}/cuda-merged:/usr/local/cuda:ro \
    # -B /opt/nvidia/hpc_sdk:/opt/nvidia/hpc_sdk:ro \


# ETCD_USE_EMBED=true -> have Milvus use an internal ETCD instance rather than connect to an outside one
# COMMON_STORAGETYPE=local -> store on local disk rather than sending data to remote (minio)


# base="/eagle/projects/radix-io/sockerman/vectorEval/milvus/singleWorker"
# apptainer shell --no-home --fakeroot --writable-tmpfs \
# --env MILVUSCONF=/milvus/configs/ \
# --env ETCD_USE_EMBED=true \
# --env ETCD_DATA_DIR=/var/lib/milvus/etcd \
# --env ETCD_CONFIG_PATH=/milvus/configs/embedEtcd.yaml \
# --env COMMON_STORAGETYPE=local \
# --env DEPLOY_MODE=STANDALONE \
# -B ${base}/milvus/:/milvus/ \
# -B ${base}/perfDir/:/perfDir/ \
# -B "./volumes/milvus:/var/lib/milvus" \
# -B "./config/user.yaml:/milvus/configs/user.yaml" \
# -B "./config/embedEtcd.yaml:/milvus/configs/embedEtcd.yaml" \
# -B ./workerOut/:/workerOut/ \
# milvus.sif 




# -B ./localDownloads/conan/:/local/conan/ \
# apptainer exec --fakeroot \
#   --env http_proxy= --env https_proxy= --env HTTP_PROXY= --env HTTPS_PROXY= \
#   --env MILVUSCONF=/milvus/configs/ \
#   --env NO_PROXY= --env no_proxy= \
#   --env ETCD_USE_EMBED=true \
#   --env ETCD_DATA_DIR=/var/lib/milvus/etcd \
#   --env ETCD_CONFIG_PATH=/milvus/configs/embedEtcd.yaml \
#   --env COMMON_STORAGETYPE=local \
#   --env DEPLOY_MODE=STANDALONE \
#   --writable-tmpfs \
#   docker://milvusdb/milvus \
#   milvus run standalone \
#   > ./milvus.out 2>&1 &
