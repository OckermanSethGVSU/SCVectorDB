#!/bin/bash

USEPERF=$1
# LD_PRELOADS
export LD_PRELOAD=""
LIBDIR=/milvus/internal/core/output/lib && \
export LD_LIBRARY_PATH="$LIBDIR:${LD_LIBRARY_PATH}" && \
export LD_PRELOAD="/milvus/internal/core/output/lib/libjemalloc.so"

# export CUDAToolkit_ROOT=/usr/local/cuda
# export CUDA_TOOLKIT_ROOT_DIR=/usr/local/cuda
# export PATH="/usr/local/cuda/bin:$PATH"
# export LD_LIBRARY_PATH="/usr/local/cuda/lib64:$LD_LIBRARY_PATH"
# export PATH="/usr/local/cuda/bin:$PATH"


apt-get update && apt-get install -y libatomic1 libelf-dev libdw-dev libslang2-dev libperl-dev python3-dev libnuma-dev libtraceevent-dev
# apt-get update && apt-get install -y libatomic1 libelf-dev libdw-dev libslang2-dev libperl-dev python3-dev libnuma-dev libtraceevent-dev
# apt-get install -y \
    # build-essential ca-certificates pkg-config \
    # libssl-dev zlib1g-dev libcurl4-openssl-dev gnupg

echo "Install Complete"
sleep 3

cd /milvus





# # NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" ./bin/milvus run standalone > /workerOut/standalone.txt 2>&1 &
# NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" \
#  /perfDir/perf record  -F 99 -g --call-graph fp --proc-map-timeout 5000 -o /workerOut/perf.data -- ./bin/milvus run standalone > /workerOut/standalone.txt 2>&1 &
# # NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" \
# # /perfDir/perf record  -F 99 -g --call-graph fp --proc-map-timeout 5000 -o /workerOut/perf.data -- ./bin/milvus run standalone > /workerOut/standalone.txt 2>&1 &

# NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" \
#  /perfDir/perf record  -F 99 -g --no-buildid-cache --call-graph dwarf --proc-map-timeout 5000 -o /workerOut/perf.data -- ./bin/milvus run standalone > /workerOut/standalone.txt 2>&1 &
# # /perfDir/perf probe -x milvus -F > probe.txt

NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" \
 ./bin/milvus run standalone > /workerOut/standalone.txt 2>&1 &
MILVUS_PID=$!

# NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" milvus run standalone > /workerOut/standalone.txt 2>&1
sleep 10

touch /workerOut/milvus_running.txt

# wait until the file exists
TARGET="/workerOut/workflow_start.txt"
while [ ! -e "$TARGET" ]; do
  sleep 0.1
done

if [[ "$USEPERF" == "true" ]]; then
    echo "Rank ${RANK} Launching perf"
    perf record -F 99 --call-graph fp -g --proc-map-timeout 5000 -o /perf/perf${RANK}.data  -p "$MILVUS_PID" &
    PERF_PID=$!
fi



# wait until the file exists
TARGET="/workerOut/workflow_stop.txt"
while [ ! -e "$TARGET" ]; do
  sleep 0.1
done

# stop perf cleanly (same as Ctrl-C)
if [[ "$USEPERF" == "true" ]]; then
    echo "Rank ${RANK} stopping perf"
    kill -INT "$PERF_PID"
    wait "$PERF_PID"
fi

echo "Rank ${RANK} done"
sleep 15 


# while true; do
#   if [ -f /workerOut/stop.txt ]; then
#     echo "stop.txt detected, sending SIGTERM."
#     # QDRANT_PIDS=$(pgrep -fa './qdrant' | grep -v 'perf'  | awk '{print $1}')
#     PIDS=$(pgrep -fa './milvus' | grep -v 'perf'  | awk '{print $1}')
#     echo "Target PIDs: ${PIDS}"
#     kill -2 $PIDS

#     sleep 60
#     exit
  
#   fi

# done

# /perfDir/perf record -F 99 --call-graph dwarf -g --proc-map-timeout 5000 -o /workerOut/perf.data -- ./bin/milvus run standalone &
# & > /workerOut/standalone.txt 2>&1 &
# /perfDir/perf record -F 99 --call-graph dwarf -g --proc-map-timeout 5000 -o /workerOut/perf.data -- ./bin/milvus run standalone  > /workerOut/standalone.txt 2>&1 