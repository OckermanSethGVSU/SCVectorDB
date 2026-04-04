
export myDIR=$myDIR
export VECTOR_DIM=$VECTOR_DIM
export DISTANCE_METRIC=$DISTANCE_METRIC
export GPU_INDEX=$GPU_INDEX
export QDRANT_EXECUTABLE=$QDRANT_EXECUTABLE
export PERF=$PERF
export PERF_EVENTS=$PERF_EVENTS
export RESTORE_DIR=$RESTORE_DIR
export INSERT_TRACE=$INSERT_TRACE
export QUERY_TRACE=$QUERY_TRACE
export INSERT_STREAMING=$INSERT_STREAMING
export QUERY_STREAMING=$QUERY_STREAMING


if [[ "$PLATFORM" == "POLARIS" ]]; then
    ml use /soft/modulefiles
    ml spack-pe-base/0.8.1
    ml use /soft/spack/testing/0.8.1/modulefiles
    ml apptainer/main
    ml load e2fsprogs
    module use /soft/modulefiles; module load conda; conda activate base
    source /eagle/projects/radix-io/sockerman/cleanQdrant/qdrantEnv/bin/activate

    cd /eagle/projects/radix-io/sockerman/SCVectorDB/qdrant/$myDIR
    exec > >(tee output.log) 2>&1
elif [[ "$PLATFORM" == "AURORA" ]]; then
    module load apptainer
    module load frameworks
    source /lus/flare/projects/radix-io/sockerman/temp/qdrant/newEnv/bin/activate
    cd /lus/flare/projects/radix-io/sockerman/temp/qdrant/$myDIR
fi


if [[ "$STORAGE_MEDIUM" == "DAOS" ]]; then
    DAOS_POOL="radix-io"
    DAOS_CONT="vectorDBTesting"
    module use /soft/modulefiles
    module load daos
    
    launch-dfuse.sh ${DAOS_POOL}:${DAOS_CONT}
    mkdir -p /tmp/${DAOS_POOL}/${DAOS_CONT}/$myDIR
fi


TOTAL=$((NODES * WORKERS_PER_NODE))
MAX_RANK=$((TOTAL - 1))
export N_WORKERS=$TOTAL

rm -f ip_registry.txt
rm -rf ip_registry.d
> ip_registry.txt

tail -n +2 $PBS_NODEFILE > worker_nodefile.txt
cat $PBS_NODEFILE > all_nodefile.txt

# create configs for each rank, 1 launched per node
mpirun -n $TOTAL --ppn $WORKERS_PER_NODE --cpu-bind none --hostfile worker_nodefile.txt  \
    python3 gen_dirs.py --storage_medium $STORAGE_MEDIUM --path /tmp/${DAOS_POOL}/${DAOS_CONT}/$myDIR

# launch qdrant nodes
for ((i=0; i<NODES; i++)); do
    # +1 b/c it uses 1 indexing and +1 b/c we are using the first node for clients
    line_num=$((i + 2))
    entry=$(sed -n "${line_num}p" "$PBS_NODEFILE")
    for ((j=0; j<WORKERS_PER_NODE; j++)); do
        index=$(((i * WORKERS_PER_NODE) + j))
        echo "Launching node ${index} with cores ${CORES}"

        # don't use binding if we are using all cores, else set it
        if [[ "$CORES" -eq 112 ]]; then            
            mpirun -n 1 --ppn 1 --cpu-bind none --host $entry ./launchQdrantNode.sh $index $STORAGE_MEDIUM &
        else
            mpirun -n 1 --ppn 1 -d $CORES --cpu-bind depth --host $entry ./launchQdrantNode.sh $index $STORAGE_MEDIUM &
        fi
        sleep 0.5
    done
done



# Launch profiling on each node
allNodes=$((NODES + 1))
for ((i=0; i<allNodes; i++)); do
    
    # +1 b/c it uses 1 indexing
    line_num=$((i + 1))
    entry=$(sed -n "${line_num}p" "$PBS_NODEFILE")
    echo "Launching profiling for node ${i}"

    if [[ $i -eq 0 ]]; then
        profile_arg="client_node"
    else
        profile_arg="worker_$((i - 1))"
    fi

    mpirun -n 1 --ppn 1 --cpu-bind none --host $entry python3 profile.py $profile_arg $PLATFORM &
    sleep 1
    
done

# Wait until all of the Qdrant ranks are running
while true; do
  all_running=1
  for ((rank=0; rank<=MAX_RANK; rank++)); do
    if [ ! -e "./perf/qdrant_running${rank}.txt" ]; then
      all_running=0
      break
    fi
  done

  if [[ "$all_running" -eq 1 ]]; then
    break
  fi

  sleep 0.1
done

while true; do
  registry_count=$(find ./ip_registry.d -maxdepth 1 -type f | wc -l)
  if [[ "$registry_count" -eq "$TOTAL" ]]; then
    break
  fi
  sleep 0.1
done

sort -t, -k1,1n ./ip_registry.d/* > ip_registry.txt
echo "Qdrant Cluster setup"
mkdir interfaces
mv interfaces*.json interfaces/

sleep 30


########## Workflow ###############
line=$(head -n 1 ip_registry.txt)
IFS=',' read -r id ip port <<< "$line"
port=$((port - 1))

if [ -z "$RESTORE_DIR" ]; then
    
    # Setup the cluster 
    TARGET_FILE="ready.flag"
    while [[ ! -e "$TARGET_FILE" ]]; do
        NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" python3 configureTopo.py
        sleep 30
    done
    rm $TARGET_FILE
    sleep 3

    export INSERT_CORPUS_SIZE=$INSERT_CORPUS_SIZE
    export INSERT_CLIENTS_PER_WORKER=$INSERT_CLIENTS_PER_WORKER
    export INSERT_FILEPATH=$INSERT_FILEPATH
    export INSERT_BATCH_SIZE=$INSERT_BATCH_SIZE
    export INSERT_BALANCE_STRATEGY=$INSERT_BALANCE_STRATEGY
    export INSERT_STREAMING=$INSERT_STREAMING
    export ACTIVE_TASK="INSERT"
    NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" ./multiClientOP
    
    # tell the profs to close and give them time to do so
    if [[ "$TASK" == "INSERT" ]]; then
        touch flag.txt
        touch ./perf/flag.txt
        sleep 30
        mkdir systemStats/
        mv *_system_*.csv systemStats/
    fi

    python3 multi_client_summary.py

    mkdir -p uploadNPY
    mv *.npy uploadNPY
   
   
    if [[ "$TASK" == "INDEX" ]]; then

        # TODO: parameterize index
        NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" python3 index.py
        
        touch flag.txt
        touch ./perf/flag.txt
        sleep 30
        mkdir systemStats/
        mv *_system_*.csv systemStats/
    fi
else
    export EXPECTED_CORPUS_SIZE=$EXPECTED_CORPUS_SIZE
    NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" python3 status.py
fi


if [[ "$TASK" == "QUERY" ]]; then
    # index the data
    NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" python3 index.py

    export QUERY_CORPUS_SIZE=$QUERY_CORPUS_SIZE
    export QUERY_CLIENTS_PER_WORKER=$QUERY_CLIENTS_PER_WORKER
    export TOTAL_QUERY_CLIENTS=$TOTAL_QUERY_CLIENTS
    export QUERY_FILEPATH=$QUERY_FILEPATH
    export QUERY_BATCH_SIZE=$QUERY_BATCH_SIZE
    export QUERY_BALANCE_STRATEGY=$QUERY_BALANCE_STRATEGY
    export QUERY_STREAMING=$QUERY_STREAMING
    export ACTIVE_TASK="QUERY"
    NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" ./multiClientOP

    python3 multi_client_summary.py

    touch flag.txt
    touch ./perf/flag.txt
    sleep 30
    mkdir systemStats/
    mv *_system_*.csv systemStats/

    mkdir -p queryNPY
    mv *.npy queryNPY
fi


if [[ "$TASK" == "MIXED" ]]; then


    if [[ -z "$RESTORE_DIR"  ]]; then
        # index the data
        NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" python3 index.py
    fi

    # Reuses these vars from insert
    export INSERT_CLIENTS_PER_WORKER=$INSERT_CLIENTS_PER_WORKER
    export INSERT_BATCH_SIZE=$INSERT_BATCH_SIZE
    export INSERT_BALANCE_STRATEGY=$INSERT_BALANCE_STRATEGY

    # Reuses these query vars
    export QUERY_CORPUS_SIZE=$QUERY_CORPUS_SIZE
    export QUERY_CLIENTS_PER_WORKER=$QUERY_CLIENTS_PER_WORKER
    export QUERY_FILEPATH=$QUERY_FILEPATH
    export QUERY_BATCH_SIZE=$QUERY_BATCH_SIZE
    export QUERY_BALANCE_STRATEGY=$QUERY_BALANCE_STRATEGY

    # Actual important mixed vars
    export MIXED_CORPUS_SIZE=$MIXED_CORPUS_SIZE
    export MIXED_DATA_FILEPATH=$MIXED_DATA_FILEPATH
    export MIXED_QUERY_CLIENTS_PER_WORKER=$MIXED_QUERY_CLIENTS_PER_WORKER
    export MIXED_INSERT_CLIENTS_PER_WORKER=$MIXED_INSERT_CLIENTS_PER_WORKER
    export RESULT_PATH=$RESULT_PATH
    export INSERT_MODE=$INSERT_MODE
    export INSERT_OPS_PER_SEC=$INSERT_OPS_PER_SEC
    export INSERT_START_ID=$INSERT_START_ID
    export QUERY_MODE=$QUERY_MODE
    export QUERY_OPS_PER_SEC=$QUERY_OPS_PER_SEC
    
    # optimal vars included for completeness 
    export COLLECTION_NAME=$COLLECTION_NAME
    export TOP_K=$TOP_K
    export QUERY_EF_SEARCH=$QUERY_EF_SEARCH
    export RPC_TIMEOUT=$RPC_TIMEOUT
    export QDRANT_REGISTRY_PATH=$QDRANT_REGISTRY_PATH
    export INSERT_BATCH_MIN=$INSERT_BATCH_MIN
    export INSERT_BATCH_MAX=$INSERT_BATCH_MAX
    export QUERY_BATCH_MIN=$QUERY_BATCH_MIN
    export QUERY_BATCH_MAX=$QUERY_BATCH_MAX

    export ACTIVE_TASK="MIXED"
    NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" ./mixedRunner

    MIXED_TIMELINE_METRIC="dot"
    if [[ "$DISTANCE_METRIC" == "COSINE" ]]; then
        MIXED_TIMELINE_METRIC="cosine"
    elif [[ "$DISTANCE_METRIC" == "L2" ]]; then
        MIXED_TIMELINE_METRIC="l2"
    fi

    MIXED_TIMELINE_ARGS=(
        mixed_timeline.py
        --log-dir "$RESULT_PATH"
        --insert-vectors "$MIXED_DATA_FILEPATH"
        --insert-max-rows "$MIXED_CORPUS_SIZE"
        --query-vectors "$QUERY_FILEPATH"
        --query-max-rows "$QUERY_CORPUS_SIZE"
        --metric "$MIXED_TIMELINE_METRIC"
        --insert-id-offset "$INSERT_START_ID"
    )
    if [[ -z "$RESTORE_DIR" ]]; then
        MIXED_TIMELINE_ARGS+=(
            --init-vectors "$INSERT_FILEPATH"
            --init-max-rows "$INSERT_CORPUS_SIZE"
        )
    fi
    NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" python3 "${MIXED_TIMELINE_ARGS[@]}"

    touch flag.txt
    touch ./perf/flag.txt
    sleep 30
    mkdir systemStats/
    mv *_system_*.csv systemStats/
fi

if [[ "$STORAGE_MEDIUM" == "DAOS" ]]; then

    # techincally optional but still good to do
    clean-dfuse.sh  ${DAOS_POOL}:${DAOS_CONT}
fi

mkdir workerOut
mv rank*.out workerOut
