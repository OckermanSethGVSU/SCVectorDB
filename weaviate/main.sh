if [[ -f ./run_config.env ]]; then
    set -a
    source ./run_config.env
    set +a
fi



if [[ -z "${BASE_DIR:-}" ]]; then
    BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fi

RUN_DIR="${RUN_DIR:-$BASE_DIR/$myDIR}"


if [[ "$PLATFORM" == "POLARIS" ]]; then
    ml use /soft/modulefiles
    ml spack-pe-base/0.8.1
    ml use /soft/spack/testing/0.8.1/modulefiles
    ml apptainer/main
    ml load e2fsprogs
    module use /soft/modulefiles; module load conda; conda activate base

elif [[ "$PLATFORM" == "AURORA" ]]; then
    module load apptainer
    module load frameworks
fi

if [[ -n "${ENV_PATH:-}" ]]; then
    echo "Activating Python environment: $ENV_PATH"
    source "$ENV_PATH/bin/activate"
    echo "ENV_PATH not set; using current Python environment: $(command -v python3)"
fi



TOTAL=$((NODES * WORKERS_PER_NODE))
MAX_RANK=$((TOTAL - 1))
# Use unique worker hosts (skip first PBS node reserved for client)
tail -n +2 "$PBS_NODEFILE" | awk '!seen[$0]++' > worker_nodefile.txt
cat $PBS_NODEFILE > all_nodefile.txt
WORKER_HOSTS=$(paste -sd, worker_nodefile.txt)

# ---------------------------------------------------------------
# Launch Weaviate cluster via MPI
# ---------------------------------------------------------------
if [[ -z "${CORES:-}" ]]; then
    mpirun -n $TOTAL --ppn $WORKERS_PER_NODE --no-vni \
     --cpu-bind none --host "$WORKER_HOSTS" \
    ./launchWeaviateNode.sh $STORAGE_MEDIUM $USEPERF $TOTAL &
    
else
    mpirun -n $TOTAL --ppn $WORKERS_PER_NODE --no-vni \
     --cpu-bind depth -d $CORES --host "$WORKER_HOSTS" \
    ./launchWeaviateNode.sh $STORAGE_MEDIUM $USEPERF $TOTAL &
fi
MPI_PID=$!

echo "[INFO] Waiting for ${TOTAL} Weaviate workers to become ready..."
for r in $(seq 0 "${MAX_RANK}"); do
    target="./perf/weaviate_running${r}.txt"
     while [[ ! -e "${target}" ]]; do
        sleep 0.5
     done 
done
echo "[INFO] All ${TOTAL} workers are ready"


NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" python3 health_check.py
