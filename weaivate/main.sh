
export myDIR=$myDIR
export VECTOR_DIM=$VECTOR_DIM
export DISTANCE_METRIC=$DISTANCE_METRIC
export GPU_INDEX=$GPU_INDEX

if [[ "$PLATFORM" == "POLARIS" ]]; then
    ml use /soft/modulefiles
    ml spack-pe-base/0.8.1
    ml use /soft/spack/testing/0.8.1/modulefiles
    ml apptainer/main
    ml load e2fsprogs
    module use /soft/modulefiles; module load conda; conda activate base
    source /eagle/projects/radix-io/sockerman/cleanQdrant/qdrantEnv/bin/activate

    cd /eagle/projects/radix-io/sockerman/tempo
    exec > >(tee output.log) 2>&1
elif [[ "$PLATFORM" == "AURORA" ]]; then
    module load apptainer
    module load frameworks
    source /lus/flare/projects/radix-io/sockerman/qdrant/qEnv/bin/activate
    cd /lus/flare/projects/radix-io/sockerman/temp/weaviate/$myDIR
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

# Use unique worker hosts (skip first PBS node reserved for client)
tail -n +2 "$PBS_NODEFILE" | awk '!seen[$0]++' > worker_nodefile.txt
cat $PBS_NODEFILE > all_nodefile.txt
WORKER_HOSTS=$(paste -sd, all_nodefile.txt)

if [[ -z "$WORKER_HOSTS" ]]; then
    echo "Error: no worker hosts derived from PBS_NODEFILE" >&2
    exit 1
fi


# launch all ranks in a single mpirun
echo "Launching ${TOTAL} Weaviate ranks in one mpirun on hosts: $WORKER_HOSTS"
if [[ "$CORES" -eq 112 ]]; then
    mpirun -n $TOTAL --ppn $WORKERS_PER_NODE --no-vni --cpu-bind none --host "$WORKER_HOSTS" \
        ./launchWeaviateNode.sh $STORAGE_MEDIUM $USEPERF $TOTAL &
else
    mpirun -n $TOTAL --ppn $WORKERS_PER_NODE -d $CORES --cpu-bind depth --host "$WORKER_HOSTS" \
        ./launchWeaviateNode.sh $STORAGE_MEDIUM $USEPERF $TOTAL &
fi