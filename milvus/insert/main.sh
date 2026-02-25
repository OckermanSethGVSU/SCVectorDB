
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




TOTAL=$((NODES * WORKERS_PER_NODE))
cat $PBS_NODEFILE > all_nodefile.txt



if [[ "$STORAGE_MEDIUM" == "DAOS" ]]; then
    module use /soft/modulefiles
    module load daos
    DAOS_POOL="radix-io"
    DAOS_CONT="vectorDBTesting"

    launch-dfuse.sh ${DAOS_POOL}:${DAOS_CONT}
    mkdir -p /tmp/${DAOS_POOL}/${DAOS_CONT}/$myDIR
fi


if [[ "$MODE" == "STANDALONE" ]]; then
    second_node=$(sed -n '2p' "$PBS_NODEFILE")
    mpirun -n 1 --ppn 1 --cpu-bind none --host $second_node ./standaloneLaunch.sh 0 $STORAGE_MEDIUM $USEPERF $PLATFORM STANDALONE &
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
        
        mpirun -n 4 --ppn $PPN --cpu-bind none --host "$HOSTS" \
        ./launch_minio.sh lustre & # must be lustre for erasure coding to work: TODO test DAOS

    elif [[ "$MINIO_MODE" == "single" ]]; then
        # Launch 1 Minio instance
        mpirun -n 1 --ppn 1 --cpu-bind none --host "${NODES[1]}"  ./launch_minio.sh memory &
    fi

    
    # setup ETCD/Minio info which all parts will need
    cp -r ${BASE_DIR}/cpuMilvus/configs/ .
    rm ./configs/milvus.yaml
    python3 replace.py --mode distributed --wal $WAL
    

    # Launch cordinator
    mpirun -n 1 --ppn 1 --cpu-bind none --host ${NODES[1]} ./launch_milvus_part.sh $STORAGE_MEDIUM COORDINATOR &
    
    # Launch streaming nodes
    mpirun -n 2 --ppn 2 --cpu-bind none --host ${NODES[1]} ./launch_milvus_part.sh $STORAGE_MEDIUM STREAMING &

    # Launch query nodes
    mpirun -n 1 --ppn 1 --cpu-bind none --host ${NODES[1]} ./launch_milvus_part.sh $STORAGE_MEDIUM QUERY &

    # Launch data nodes
    mpirun -n 1 --ppn 1 --cpu-bind none --host ${NODES[1]} ./launch_milvus_part.sh $STORAGE_MEDIUM DATA &
    
    # Launch proxy
    mpirun -n 1 --ppn 1 --cpu-bind none --host ${NODES[1]} ./launch_milvus_part.sh $STORAGE_MEDIUM PROXY &

    # verify milvus is running
    TARGET="./workerOut/proxy0_running.txt"
    while [ ! -e "$TARGET" ]; do
    sleep 0.1
    done
    IP_ADDR=$(jq -r '.hsn0.ipv4[0]' PROXY0.json)
    echo $IP_ADDR > worker.ip
    env "${PYTHON_ENV_VARS[@]}" python3 poll.py
fi



env "${PYTHON_ENV_VARS[@]}" python3 setup_collection.py


export MILVUS_HOST=${IP_ADDR}
export CORPUS_SIZE=$CORPUS_SIZE
export UPLOAD_CLIENTS_PER_WORKER=$UPLOAD_CLIENTS_PER_WORKER
export N_WORKERS=$TOTAL
export DATA_FILEPATH=$DATA_FILEPATH
export UPLOAD_BATCH_SIZE=$UPLOAD_BATCH_SIZE
touch ./workerOut/workflow_start.txt
sleep 5

NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" ./multiClientInsert

# # NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" python3 convert_to_gpu_cargra.py
touch ./workerOut/workflow_end.txt
touch flag.txt
env "${PYTHON_ENV_VARS[@]}" python3 multi_client_summary.py

# sleep 5

# # NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" python3 convert_to_hnsw.py
# # NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" python3 drop_collection.py

# # python3 
# # python3 gen_summary.py
# python3 uneven_last_gen_summary.py
# # pbsdsh -v bash -lc 'echo "===== $(hostname) ====="; dmesg -T | tail -n 2000'  > dmesg.all.txt

# # chmod 777 ./go/temp/
# # rm -fr ./go/temp/
sleep 60

if [[ "$STORAGE_MEDIUM" == "DAOS" ]]; then
    DAOS_POOL="radix-io"
    DAOS_CONT="vectorDBTesting"
    rm -r /tmp/${DAOS_POOL}/${DAOS_CONT}/$myDIR
elif [[ "$STORAGE_MEDIUM" == "lustre" ]]; then
    rm -r ./milvusDir/
fi