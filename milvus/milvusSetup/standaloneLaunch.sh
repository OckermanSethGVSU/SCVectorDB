#!/bin/bash

RANK=${1:?Usage: $0 <rank>}
RANK=$((RANK))
STORAGE_MEDIUM=${2:?Usage: $0 <rank> <storage_medium>}
USEPERF=${3:?Usage: $0 <rank> <storage_medium> <perf>}
PLATFORM=${4:?Usage: $0 <rank> <storage_medium> <perf> <platform>}


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
    ETCD_FLAG="--env ETCD_DATA_DIR=$RESULT_PATH/var/lib/milvus/etcd"

elif [[ "$STORAGE_MEDIUM" == "lustre" ]]; then
    TARGET_BASE="./milvusDir"
    (( RANK == 0 )) && echo "Using lustre for persistence"
elif [[ "$STORAGE_MEDIUM" == "SSD" ]]; then
    TARGET_BASE="/local/scratch/milvusDir"
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


mkdir -p ${TARGET_BASE}/volumes/milvus/
mkdir -p ./workerOut/


POLARIS_BINDS=()
if [[ "$PLATFORM" == "AURORA" ]]; then
    base="/lus/flare/projects/radix-io/sockerman/temp/milvus"
elif [[ "$PLATFORM" == "POLARIS" ]]; then
    base="/lus/eagle/projects/radix-io/sockerman/SCVectorDB/milvus"
    POLARIS_BINDS+=(
        -B "/eagle/projects/argonne_tpc/sockerman/buildingFromSource/gpuMilvus/cuda-merged:/usr/local/cuda:ro"
        -B "/opt/nvidia/hpc_sdk:/opt/nvidia/hpc_sdk:ro"
        --env PLATFORM=POLARIS
    )
    
fi
# Create and pass in modified config #####

cp -r ${base}/cpuMilvus/configs/ .

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

python3 replace.py
cp -r ./configs/ $TARGET_BASE/
#####


apptainer exec --no-home --fakeroot --writable-tmpfs --nv \
    --pwd /milvus \
    --env MILVUSCONF=/milvus/configs/ \
    --env ETCD_USE_EMBED=true \
    $ETCD_FLAG \
    --env ETCD_CONFIG_PATH=/milvus/configs/embedEtcd.yaml \
    --env COMMON_STORAGETYPE=local \
    --env DEPLOY_MODE=STANDALONE \
    --env CUDA_VISIBLE_DEVICES="" \
    -B ./execute.sh:/milvus/app_execute.sh \
    -B ${base}/cpuMilvus/:/milvus/ \
    -B ${TARGET_BASE}/configs/:/milvus/configs/ \
    -B ${base}/perfDir/:/perfDir/ \
    -B ./workerOut/:/workerOut/ \
    -B ${TARGET_BASE}/volumes/milvus:/var/lib/milvus \
    "${POLARIS_BINDS[@]}" \
    milvus.sif bash app_execute.sh




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
