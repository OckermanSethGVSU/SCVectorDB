

launch_role() {
    local role="$1"                  # e.g., STREAMING, QUERY, DATA
    local total_ranks="$2"           # e.g., $STREAMING_NODES
    local ranks_per_node="$3"        # e.g., $STREAMING_NODES_PER_CN
    local storage_medium="$4"        # e.g., $STORAGE_MEDIUM
    local script="$5"                # e.g., ./launch_milvus_part.sh

    if (( total_ranks <= 0 || ranks_per_node <= 0 )); then
        echo "ERROR [$role]: ranks and ranks_per_node must be > 0" >&2
        exit 1
    fi

    # ceil(total_ranks / ranks_per_node)
    local nodes_needed=$(( (total_ranks + ranks_per_node - 1) / ranks_per_node ))

    # NODES[0] is reserved
    local available=$(( ${#NODES[@]} - 1 ))

    if (( nodes_needed > available )); then
        echo "ERROR [$role]: need $nodes_needed nodes but only $available available (excluding NODES[0])" >&2
        exit 1
    fi

    # Build hostlist from NODES[1..nodes_needed]
    local hostlist=""
    for ((i=1; i<=nodes_needed; i++)); do
        hostlist+="${NODES[i]},"
    done
    hostlist="${hostlist%,}"

    echo "Launching $role:"
    echo "  total ranks      = $total_ranks"
    echo "  ranks per node   = $ranks_per_node"
    echo "  nodes needed     = $nodes_needed"
    echo "  hosts            = $hostlist"
    echo "  storage medium   = $storage_medium"

    mpirun -n "$total_ranks" \
        --ppn "$ranks_per_node" \
        --cpu-bind none \
        --host "$hostlist" \
        "$script" "$storage_medium" "$role" &
}

PYTHON_ENV_VARS=(
    NO_PROXY=""
    no_proxy=""
    http_proxy=""
    https_proxy=""
    HTTP_PROXY=""
    HTTPS_PROXY=""
)

cd $BASE_DIR/$myDIR
export BASE_DIR=$BASE_DIR
export myDIR=$myDIR
export RESULT_PATH=$BASE_DIR/$myDIR
export ETCD_MODE=$ETCD_MODE
export MODE=$MODE
export DML_CHANNELS=$DML_CHANNELS
export TASK=$TASK
export VECTOR_DIM=$VECTOR_DIM
export DISTANCE_METRIC=$DISTANCE_METRIC

if [[ "$PLATFORM" == "POLARIS" ]]; then
    ml use /soft/modulefiles
    ml spack-pe-base/0.8.1
    ml use /soft/spack/testing/0.8.1/modulefiles
    ml apptainer/main
    ml load e2fsprogs

    module use /soft/modulefiles; module load conda; conda activate base
    exec > >(tee output.log) 2>&1

elif [[ "$PLATFORM" == "AURORA" ]]; then
    module load apptainer
    module load frameworks
    
    PYTHON_ENV_VARS+=(NUMEXPR_NUM_THREADS=108 NUMEXPR_MAX_THREADS=108)
fi

# Activate Python env
source $ENV_PATH/bin/activate




cat $PBS_NODEFILE > all_nodefile.txt



if [[ "$STORAGE_MEDIUM" == "DAOS" || "$MINIO_MEDIUM" == "DAOS" ]]; then
    module use /soft/modulefiles
    module load daos
    DAOS_POOL="radix-io"
    DAOS_CONT="vectorDBTesting"

    launch-dfuse.sh ${DAOS_POOL}:${DAOS_CONT}
    mkdir -p /tmp/${DAOS_POOL}/${DAOS_CONT}/$myDIR
fi


if [[ "$MODE" == "STANDALONE" ]]; then
    second_node=$(sed -n '2p' "$PBS_NODEFILE")
    mpirun -n 1 --ppn 1 --cpu-bind none --host $second_node ./standaloneLaunch.sh 0 $STORAGE_MEDIUM $USEPERF $PLATFORM STANDALONE $WAL &
    # launch profiling on worker and client nodes
    mpirun -n 1 --ppn 1 --cpu-bind none --host $second_node python3 profile.py worker_0 $PLATFORM &
    python3 profile.py client_node $PLATFORM & 

    TARGET="./workerOut/milvus_running.txt"
    while [ ! -e "$TARGET" ]; do
    sleep 0.1
    done

    env "${PYTHON_ENV_VARS[@]}" python3 poll.py
    IP_ADDR=$(jq -r '.hsn0.ipv4[0]' interfaces0.json)


elif [[ "$MODE" == "DISTRIBUTED" ]]; then
    export MINIO_MODE=$MINIO_MODE
    export ETCD_MODE=$ETCD_MODE    
    mapfile -t NODES < <(awk '!seen[$0]++' "$PBS_NODEFILE")


    # spreads etcd evenly on up to 3 nodes
    if [[ "$ETCD_MODE" == "replicated" ]]; then        
        ETCD_INSTANCES=3
        TOTAL_NODES=${#NODES[@]}
        if (( TOTAL_NODES < 2 )); then
            echo "ERROR: Need at least 2 nodes (NODES[0] reserved) for ETCD_MODE=replicated"
            exit 1
        fi

        ETCD_SPREAD=$((TOTAL_NODES - 1))
        (( ETCD_SPREAD > 3 )) && ETCD_SPREAD=3

        HOSTS=""
        for ((i=1; i<=ETCD_SPREAD; i++)); do
            HOSTS+="${HOSTS:+,}${NODES[$i]}"
        done
        
        # enough ranks per node to fit 3 total
        PPN=$(( (ETCD_INSTANCES + ETCD_SPREAD - 1) / ETCD_SPREAD ))
        mpirun -n "$ETCD_INSTANCES" --ppn "$PPN" --cpu-bind none --host "$HOSTS" \
            ./launch_etcd.sh "$STORAGE_MEDIUM" &

    # Launch 1 etcd instance
    elif [[ "$ETCD_MODE" == "single" ]]; then
        mpirun -n 1 --ppn 1 --cpu-bind none --host ${NODES[1]} ./launch_etcd.sh $STORAGE_MEDIUM &
    fi
    
    # spread MINIO on up to 4 nodes
    if [[ "$MINIO_MODE" == "stripped" ]]; then
        TOTAL_NODES=${#NODES[@]}
        MINIO_INSTANCES="${MINIO_INSTANCES:-4}"
        MINIO_SPREAD=$((TOTAL_NODES - 1))
        
        if [[ "$MINIO_SPREAD" -gt 4 ]]; then
            MINIO_SPREAD=4
        fi

        HOSTS=""
        for ((r=0; r<MINIO_INSTANCES; r++)); do
            # skip index 0
            node_idx=$(( 1 + (r % MINIO_SPREAD) ))
            host="${NODES[$node_idx]}"
            HOSTS+="${HOSTS:+,}${host}"
        done
        PPN=$(( (MINIO_INSTANCES + MINIO_SPREAD - 1) / MINIO_SPREAD ))
        
        mpirun -n 4 --ppn $PPN --no-vni --cpu-bind none --host "$HOSTS" \
        ./launch_minio.sh  $MINIO_MEDIUM & # must be lustre or DAOS for erasure coding to work

    elif [[ "$MINIO_MODE" == "single" ]]; then
        # Launch 1 Minio instance
        mpirun -n 1 --ppn 1 --no-vni --cpu-bind none --host "${NODES[1]}"  ./launch_minio.sh $MINIO_MEDIUM &
    fi

    
    # setup ETCD/Minio info which all parts will need
    cp -r ${BASE_DIR}/cpuMilvus/configs/ .
    rm ./configs/milvus.yaml
    python3 replace.py --mode distributed --wal $WAL
    

    # Launch cordinator
    mpirun -n 1 --ppn 1 --cpu-bind none --host ${NODES[1]} ./launch_milvus_part.sh $STORAGE_MEDIUM COORDINATOR &
    
    # Launch streaming nodes
    launch_role STREAMING "$STREAMING_NODES" "$STREAMING_NODES_PER_CN" "$STORAGE_MEDIUM" ./launch_milvus_part.sh
    # mpirun -n $STREAMING_NODES --ppn $STREAMING_NODES_PER_CN --cpu-bind none --host ${NODES[1]} ./launch_milvus_part.sh $STORAGE_MEDIUM STREAMING &

    # Launch query nodes
    mpirun -n 1 --ppn 1 --cpu-bind none --host ${NODES[1]} ./launch_milvus_part.sh $STORAGE_MEDIUM QUERY &

    # Launch data nodes
    mpirun -n 1 --ppn 1 --cpu-bind none --host ${NODES[1]} ./launch_milvus_part.sh $STORAGE_MEDIUM DATA &
    
    # Launch proxy
    launch_role PROXY "$NUM_PROXIES" "$NUM_PROXIES_PER_CN" "$STORAGE_MEDIUM" ./launch_milvus_part.sh
    # mpirun -n 1 --ppn 1 --cpu-bind none --host ${NODES[1]} ./launch_milvus_part.sh $STORAGE_MEDIUM PROXY &

    # verify milvus is running
    TARGET="./workerOut/proxy$((NUM_PROXIES - 1))_running.txt"
    while [ ! -e "$TARGET" ]; do
    sleep 0.1
    done
    IP_ADDR=$(jq -r '.hsn0.ipv4[0]' PROXY/PROXY0.json)
    echo $IP_ADDR > worker.ip
    env "${PYTHON_ENV_VARS[@]}" python3 poll.py
fi



env "${PYTHON_ENV_VARS[@]}" python3 setup_collection.py


export UPLOAD_BALANCE_STRATEGY=${UPLOAD_BALANCE_STRATEGY}
export CORPUS_SIZE=$CORPUS_SIZE
export NUM_PROXIES=$NUM_PROXIES
export UPLOAD_CLIENTS_PER_PROXY=$UPLOAD_CLIENTS_PER_PROXY
export DATA_FILEPATH=$DATA_FILEPATH
export UPLOAD_BATCH_SIZE=$UPLOAD_BATCH_SIZE

if [[ "$TASK" == "insert" ]]; then
    touch ./workerOut/workflow_start.txt
fi
sleep 5

NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" ./multiClientInsert

# # NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" python3 convert_to_gpu_cargra.py
if [[ "$TASK" == "insert" ]]; then
    touch ./workerOut/workflow_end.txt
    touch flag.txt
fi

env "${PYTHON_ENV_VARS[@]}" python3 insert_multi_client_summary.py

mv times.csv insert_times.txt
mv summary.csv insert_summary.txt
mkdir -p uploadNPY
mv *.npy uploadNPY

if [[ "$TASK" == "index" ]]; then
    touch ./workerOut/workflow_start.txt
    NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" python3 convert_to_hnsw.py
fi

if [[ "$TASK" == "index" ]]; then
    touch ./workerOut/workflow_end.txt
    touch flag.txt
fi


sleep 60

if [[ "$STORAGE_MEDIUM" == "DAOS" ]]; then
    DAOS_POOL="radix-io"
    DAOS_CONT="vectorDBTesting"
    rm -fr /tmp/${DAOS_POOL}/${DAOS_CONT}/$myDIR
elif [[ "$STORAGE_MEDIUM" == "lustre" || "$MODE" == "DISTRIBUTED" ]]; then
    rm -fr ./milvusDir/
fi