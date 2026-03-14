

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

wait_for_signal_files() {
    local missing=1

    while (( missing )); do
        missing=0
        for signal_file in "$@"; do
            if [[ ! -e "$signal_file" ]]; then
                missing=1
                break
            fi
        done

        if (( missing )); then
            sleep 0.1
        fi
    done
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
export CORES=$CORES
export BASE_DIR=$BASE_DIR
export PLATFORM=$PLATFORM
export myDIR=$myDIR
export RESULT_PATH=$BASE_DIR/$myDIR
export ETCD_MODE=$ETCD_MODE
export MODE=$MODE
export WAL=$WAL
export DML_CHANNELS=$DML_CHANNELS
export TASK=$TASK
export VECTOR_DIM=$VECTOR_DIM
export DISTANCE_METRIC=$DISTANCE_METRIC
export GPU_INDEX=$GPU_INDEX
export MILVUS_BUILD_DIR=$MILVUS_BUILD_DIR
export TRACING=$TRACING
export PERF=$PERF
export MILVUS_CONFIG_DIR=$MILVUS_CONFIG_DIR
export DEBUG=$DEBUG

if [[ "$PLATFORM" == "POLARIS" ]]; then
    ml use /soft/modulefiles
    ml spack-pe-base/0.8.1
    ml use /soft/spack/testing/0.8.1/modulefiles
    ml apptainer/main
    ml load e2fsprogs

    module use /soft/modulefiles; module load conda; conda activate base
    exec > >(tee workflow.log) 2>&1

elif [[ "$PLATFORM" == "AURORA" ]]; then
    module load apptainer
    module load frameworks
    
    PYTHON_ENV_VARS+=(NUMEXPR_NUM_THREADS=108 NUMEXPR_MAX_THREADS=108)
fi

# Activate Python env
source $ENV_PATH/bin/activate
cat $PBS_NODEFILE > all_nodefile.txt



if [[ "$STORAGE_MEDIUM" == "DAOS" || ( "$MODE" == "DISTRIBUTED" && "$MINIO_MEDIUM" == "DAOS" ) ]]; then
    module use /soft/modulefiles
    module load daos
    DAOS_POOL="radix-io"
    DAOS_CONT="vectorDBTesting"

    launch-dfuse.sh ${DAOS_POOL}:${DAOS_CONT}
    mkdir -p /tmp/${DAOS_POOL}/${DAOS_CONT}/$myDIR
fi

if [[ "$TRACING" == "True" ]]; then
    ready=0
    bash launch_otel.sh &
    python3 net_mapping.py --rank 0 --name otel
    IP_ADDR=$(jq -r '.hsn0.ipv4[0]' "otel0.json")
    echo $IP_ADDR > otel.ip
    rm otel0.json
    for i in $(seq 1 90); do
        if env "${PYTHON_ENV_VARS[@]}" curl -fsS "http://${IP_ADDR}:13133/" >/dev/null 2>&1; then
            ready=1
            export OTLP_GRPC_ENDPOINT="${IP_ADDR}:4317"
            sleep 10 # buffer 
            echo "Collector is ready."
            break
        fi

        if [ $((i % 10)) -eq 0 ]; then
            echo "  still waiting (${i}s elapsed)..."
        fi
        sleep 1
    done
    
    if [ "${ready}" = "0" ]; then
        echo "Collector did not become healthy within 90s."
        exit 1
    fi
fi


if [[ "$MODE" == "STANDALONE" ]]; then
    second_node=$(sed -n '2p' "$PBS_NODEFILE")

    if [[ "$CORES" -eq 112 ]]; then
        echo "Launching standalone: unrestricted cores"

        mpirun -n 1 --ppn 1 --cpu-bind none --host $second_node ./standaloneLaunch.sh 0 $STORAGE_MEDIUM $PLATFORM STANDALONE $WAL &
    else
        echo "Launching standalone: ${CORES} cores"
        mpirun -n 1 --ppn 1 -d $CORES --cpu-bind depth  --host $second_node ./standaloneLaunch.sh 0 $STORAGE_MEDIUM $PLATFORM STANDALONE $WAL &
    fi

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
    python3 replace_unified.py --mode distributed

    # Launch cordinator
    mpirun -n 1 --ppn 1 --cpu-bind none --host ${NODES[1]} ./launch_milvus_part.sh $STORAGE_MEDIUM COORDINATOR &
    
    # Launch streaming nodes
    launch_role STREAMING "$STREAMING_NODES" "$STREAMING_NODES_PER_CN" "$STORAGE_MEDIUM" ./launch_milvus_part.sh

    # Launch query nodes
    mpirun -n 1 --ppn 1 --cpu-bind none --host ${NODES[1]} ./launch_milvus_part.sh $STORAGE_MEDIUM QUERY &

    # Launch data nodes
    mpirun -n 1 --ppn 1 --cpu-bind none --host ${NODES[1]} ./launch_milvus_part.sh $STORAGE_MEDIUM DATA &
    
    # Launch proxy
    launch_role PROXY "$NUM_PROXIES" "$NUM_PROXIES_PER_CN" "$STORAGE_MEDIUM" ./launch_milvus_part.sh

    # execute.sh only writes these markers after each component passes its health check.
    SIGNAL_FILES=(
        "./workerOut/cord0_running.txt"
        "./workerOut/query0_running.txt"
        "./workerOut/data0_running.txt"
    )

    for ((rank=0; rank<STREAMING_NODES; rank++)); do
        SIGNAL_FILES+=("./workerOut/streaming${rank}_running.txt")
    done

    for ((rank=0; rank<NUM_PROXIES; rank++)); do
        SIGNAL_FILES+=("./workerOut/proxy${rank}_running.txt")
    done

    wait_for_signal_files "${SIGNAL_FILES[@]}"
    IP_ADDR=$(jq -r '.hsn0.ipv4[0]' PROXY/PROXY0.json)
    echo $IP_ADDR > worker.ip
    sleep 30
    
    env "${PYTHON_ENV_VARS[@]}" python3 poll.py
fi



env "${PYTHON_ENV_VARS[@]}" python3 setup_collection.py


export UPLOAD_BALANCE_STRATEGY=${UPLOAD_BALANCE_STRATEGY}
export CORPUS_SIZE=$CORPUS_SIZE
export NUM_PROXIES=$NUM_PROXIES
export UPLOAD_CLIENTS_PER_PROXY=$UPLOAD_CLIENTS_PER_PROXY
export DATA_FILEPATH=$DATA_FILEPATH
export UPLOAD_BATCH_SIZE=$UPLOAD_BATCH_SIZE


NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" ./multiClientInsert

if [[ "$TASK" == "insert" ]]; then
    touch flag.txt
fi

env "${PYTHON_ENV_VARS[@]}" python3 insert_multi_client_summary.py

mv times.csv insert_times.txt
mv summary.csv insert_summary.txt
mkdir -p uploadNPY
mv *.npy uploadNPY

if [[ "$TASK" == "index" ]]; then
    touch ./workerOut/workflow_start.txt

    NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" python3 index_data.py
    
    touch ./workerOut/workflow_end.txt
    touch flag.txt
fi



sleep 60

if [[ "$TRACING" == "True" ]]; then
    python3 analyze_traces.py > analysis.txt
fi
if [[ "$STORAGE_MEDIUM" == "DAOS" ]]; then
    DAOS_POOL="radix-io"
    DAOS_CONT="vectorDBTesting"
    rm -fr /tmp/${DAOS_POOL}/${DAOS_CONT}/$myDIR
elif [[ "$STORAGE_MEDIUM" == "lustre" || "$MODE" == "DISTRIBUTED" ]]; then
    rm -fr ./milvusDir/
fi
