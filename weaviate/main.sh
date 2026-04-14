
export myDIR=$myDIR
export VECTOR_DIM=$VECTOR_DIM
export DISTANCE_METRIC=$DISTANCE_METRIC
export GPU_INDEX=$GPU_INDEX
export WEAVIATE_CLIENT_BINARY=${WEAVIATE_CLIENT_BINARY:-test}

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


# wait for the cluster to signal that it is up
TARGET="./perf/weaviate_running${MAX_RANK}.txt"
while [ ! -e "$TARGET" ]; do
  sleep 0.1
done

REGISTRY_FILE="ip_registry.txt"
if [[ ! -f "$REGISTRY_FILE" ]]; then
    echo "Error: expected registry file '$REGISTRY_FILE' not found" >&2
    exit 1
fi

WEAVIATE_IP=$(awk -F, '$1 == 0 {print $3; exit}' "$REGISTRY_FILE")
WEAVIATE_HTTP_PORT=$(awk -F, '$1 == 0 {print $4; exit}' "$REGISTRY_FILE")
if [[ -z "$WEAVIATE_IP" || -z "$WEAVIATE_HTTP_PORT" ]]; then
    echo "Error: failed to read rank 0 HTTP endpoint from '$REGISTRY_FILE'" >&2
    cat "$REGISTRY_FILE" >&2
    exit 1
fi

export WEAVIATE_SCHEME="http"
export WEAVIATE_HOST="${WEAVIATE_IP}:${WEAVIATE_HTTP_PORT}"
echo "WEAVIATE_HOST=${WEAVIATE_HOST}"
if [[ ! -x "./${WEAVIATE_CLIENT_BINARY}" ]]; then
    echo "Error: expected Weaviate client binary './${WEAVIATE_CLIENT_BINARY}' in $(pwd)" >&2
    exit 1
fi

NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" "./${WEAVIATE_CLIENT_BINARY}"
