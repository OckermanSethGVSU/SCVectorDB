
cd /lus/flare/projects/radix-io/sockerman/temp/milvus/$myDIR
export myDIR=$myDIR
export RESULT_PATH=/lus/flare/projects/radix-io/sockerman/temp/milvus/$myDIR

TOTAL=$((NODES * WORKERS_PER_NODE))
cat $PBS_NODEFILE > all_nodefile.txt
second_node=$(sed -n '2p' "$PBS_NODEFILE")

module load apptainer
module load frameworks

if [[ "$STORAGE_MEDIUM" == "DAOS" ]]; then
    module use /soft/modulefiles
    module load daos
    DAOS_POOL="radix-io"
    DAOS_CONT="vectorDBTesting"

    launch-dfuse.sh ${DAOS_POOL}:${DAOS_CONT}
    mkdir -p /tmp/${DAOS_POOL}/${DAOS_CONT}/$myDIR
fi



mpirun -n 1 --ppn 1 --cpu-bind none --host $second_node ./workerLaunch.sh 0 $STORAGE_MEDIUM $USEPERF &


# launch profiling on worker and client nodes
mpirun -n 1 --ppn 1 --cpu-bind none --host $second_node python3 profile.py worker_0 &

python3 profile.py client_node & 


TARGET="./workerOut/milvus_running.txt"
while [ ! -e "$TARGET" ]; do
  sleep 0.1
done

source /lus/flare/projects/radix-io/sockerman/milvusEnv/bin/activate

NUMEXPR_NUM_THREADS=108 NUMEXPR_MAX_THREADS=108 \
NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" \
python3 poll.py

NUMEXPR_NUM_THREADS=108 NUMEXPR_MAX_THREADS=108 \
NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" \
python3 setup_collection.py



IP_ADDR=$(jq -r '.hsn0.ipv4[0]' interfaces0.json)
export MILVUS_HOST=${IP_ADDR}
export CORPUS_SIZE=$CORPUS_SIZE
export UPLOAD_CLIENTS_PER_WORKER=$UPLOAD_CLIENTS_PER_WORKER
export N_WORKERS=$TOTAL
export DATA_FILEPATH=$DATA_FILEPATH
export UPLOAD_BATCH_SIZE=$UPLOAD_BATCH_SIZE


touch ./workerOut/workflow_start.txt
sleep 5

export GOPATH=/home/treewalker/go
export GOMODCACHE=/home/treewalker/go/pkg/mod
export GOCACHE=/home/treewalker/.cache/go-build
echo "About to run go"
NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" ./multiClientInsert

# # NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" python3 convert_to_gpu_cargra.py
touch ./workerOut/workflow_end.txt
touch flag.txt
python3 multi_client_summary.py

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