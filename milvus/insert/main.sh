
cd /lus/flare/projects/radix-io/sockerman/temp/milvus/$myDIR
export $myDIR

cat $PBS_NODEFILE > all_nodefile.txt
second_node=$(sed -n '2p' "$PBS_NODEFILE")

# download go dependices before we get rid of proxy
cd go
mkdir -p temp
export GOMODCACHE=$(pwd)/temp
go mod download
cd ..


module load apptainer
module load frameworks


mpirun -n 1 --ppn 1 --cpu-bind none --host $second_node ./workerLaunch.sh 0 &


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
python3 status.py
# python3 setup_collection.py



IP_ADDR=$(jq -r '.hsn0.ipv4[0]' interfaces0.json)

export MILVUS_HOST=${IP_ADDR}
export BATCH_SIZE=$BATCH_SIZE
export NClients=$NClients
export N_WORKERS=$NClients
# export CORPUS_SIZE=$CORPUS_SIZE

# NUMEXPR_NUM_THREADS=108 NUMEXPR_MAX_THREADS=108 \
# NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" \
# python3 mpInsert.py


# NUMEXPR_NUM_THREADS=108 NUMEXPR_MAX_THREADS=108 \
# NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" \
# python3 convert_to_hnsw.py

touch ./workerOut/perf_start.txt
sleep 5

cd go
export BATCH_SIZE=1
export NClients=1
NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" go run .

# NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY=""  python3 query.py
cd .. 

# # NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" python3 convert_to_gpu_cargra.py
touch ./workerOut/perf_stop.txt
touch flag.txt

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