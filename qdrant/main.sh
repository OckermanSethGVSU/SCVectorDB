
export myDIR=$myDIR
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
    source /lus/flare/projects/radix-io/sockerman/qdrant/qEnv/bin/activate
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
            mpirun -n 1 --ppn 1 --cpu-bind none --host $entry ./launchQdrantNode.sh $index $STORAGE_MEDIUM $USEPERF &
        else
            mpirun -n 1 --ppn 1 -d $CORES --cpu-bind depth --host $entry ./launchQdrantNode.sh $index $STORAGE_MEDIUM $USEPERF &
        fi
        sleep 3
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
TARGET="./perf/qdrant_running${MAX_RANK}.txt"
while [ ! -e "$TARGET" ]; do
  sleep 0.1
done
echo "Qdrant Cluster setup"
sleep 120

# Setup the cluster 
TARGET_FILE="ready.flag"
while [[ ! -e "$TARGET_FILE" ]]; do
    NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" python3 configureTopo.py
    sleep 30
done
rm $TARGET_FILE

if [[ "$TASK" == "insert" ]]; then
    touch ./perf/workflow_start.txt
fi
sleep 3

########## Workflow ###############
line=$(head -n 1 ip_registry.txt)
IFS=',' read -r id ip port <<< "$line"
port=$((port - 1))

export CORPUS_SIZE=$CORPUS_SIZE
export UPLOAD_CLIENTS_PER_WORKER=$UPLOAD_CLIENTS_PER_WORKER
export N_WORKERS=$TOTAL
export DATA_FILEPATH=$DATA_FILEPATH
export UPLOAD_BATCH_SIZE=$UPLOAD_BATCH_SIZE
export UPLOAD_BALANCE_STRATEGY=$UPLOAD_BALANCE_STRATEGY
NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" ./multiClientUpload


# tell the profs to close and give them time to do so
if [[ "$TASK" == "insert" ]]; then
    touch ./perf/workflow_stop.txt
    touch flag.txt
    sleep 30
    mkdir systemStats/
    mv *_system_*.csv systemStats/
fi

python3 insert_multi_client_summary.py
mv times.csv insert_times.csv

mkdir -p uploadNPY
mv *.npy uploadNPY

if [[ "$TASK" == "index" ]]; then

    touch ./perf/workflow_start.txt
    sleep 5
    # TODO: parameterize index
    NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" python3 index.py
    
    touch ./perf/workflow_stop.txt
    touch flag.txt
    sleep 30
    mkdir systemStats/
     mv *_system_*.csv systemStats/
fi


if [[ "$STORAGE_MEDIUM" == "DAOS" ]]; then

    # techincally optional but still good to do
    clean-dfuse.sh  ${DAOS_POOL}:${DAOS_CONT}
fi