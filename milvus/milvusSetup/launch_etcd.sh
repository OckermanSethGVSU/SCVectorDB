#!/bin/bash

STORAGE_MEDIUM=${1:?Usage: $0 <storage_medium>}
RANK="${PMI_RANK:-${PMIX_RANK:-${OMPI_COMM_WORLD_RANK:-}}}"

if [[ "$STORAGE_MEDIUM" == "memory" ]]; then
    TARGET_BASE="/dev/shm/"
    ETCD_BASE=$TARGET_BASE
    
    (( RANK == 0 )) && echo "ETCD using memory for persistence"

DAOS_ARGS=()
elif [[ "$STORAGE_MEDIUM" == "DAOS" ]]; then
    DAOS_POOL="radix-io"
    DAOS_CONT="vectorDBTesting"
    TARGET_BASE="/tmp/${DAOS_POOL}/${DAOS_CONT}/${myDIR}/milvusDir"
    (( RANK == 0 )) && echo "ETCD using memory for persistence"
    APPTAINER_ARGS+=(
        --bind "/home/treewalker/daos_lib64:/opt/daos/lib64:ro"
        --env LD_LIBRARY_PATH=/opt/daos/lib64
    )
    ETCD_BASE=./milvusDir/


elif [[ "$STORAGE_MEDIUM" == "lustre" ]]; then
    TARGET_BASE="./milvusDir"
    ETCD_BASE=$TARGET_BASE

    (( RANK == 0 )) && echo "ETCD using memory for persistence"
elif [[ "$STORAGE_MEDIUM" == "SSD" ]]; then
    TARGET_BASE="/local/scratch/milvusDir"
    ETCD_BASE=$TARGET_BASE

    (( RANK == 0 )) && echo "ETCD using memory for persistence"

else
    (( RANK == 0 )) && echo "Error: unknown STORAGE_MEDIUM '$STORAGE_MEDIUM'" >&2
    exit 1
fi

python3 net_mapping.py --rank $RANK --name etcd
IP_ADDR=$(jq -r '.hsn0.ipv4[0]' etcd${RANK}.json)
sleep $((RANK * 5))

OUTPUT_FILE="etcd_registry.txt"
# rank0 clears it at the beginning
if [[ "$RANK" -eq 0 ]]; then
  rm -f "$OUTPUT_FILE"
fi

# stagger writes slightly
sleep $((RANK * 2))
echo "${RANK},${IP_ADDR},2379" >> "$OUTPUT_FILE"

# wait for all 3 entries
expected=3
for _ in $(seq 1 60); do
  lines=$(awk 'NF' "$OUTPUT_FILE" 2>/dev/null | wc -l || true)
  if [[ "$lines" -ge "$expected" ]]; then
    break
  fi
  sleep 1
done

lines=$(awk 'NF' "$OUTPUT_FILE" 2>/dev/null | wc -l || true)
if [[ "$lines" -lt "$expected" ]]; then
  echo "Error: timed out waiting for $expected etcd registry entries; saw $lines" >&2
  echo "Current $OUTPUT_FILE:" >&2
  cat "$OUTPUT_FILE" >&2 || true
  exit 1
fi

# build initial cluster string (rank-sorted)
IP0=$(awk -F, '$1==0{print $2}' "$OUTPUT_FILE")
IP1=$(awk -F, '$1==1{print $2}' "$OUTPUT_FILE")
IP2=$(awk -F, '$1==2{print $2}' "$OUTPUT_FILE")
if [[ -z "$IP0" || -z "$IP1" || -z "$IP2" ]]; then
  echo "Error: missing IPs in registry: IP0='$IP0' IP1='$IP1' IP2='$IP2'" >&2
  cat "$OUTPUT_FILE" >&2
  exit 1
fi
INITIAL_CLUSTER="etcd-0=http://${IP0}:2380,etcd-1=http://${IP1}:2380,etcd-2=http://${IP2}:2380"



ETCD_VOL="$ETCD_BASE/volumes/etcd_volume_${RANK}"
rm -rf "$ETCD_VOL"
mkdir -p "$ETCD_VOL"

apptainer exec --fakeroot \
  --writable-tmpfs \
  --env http_proxy= --env https_proxy= --env HTTP_PROXY= --env HTTPS_PROXY= \
  --env NO_PROXY= --env no_proxy= \
  --env MILVUSCONF=/milvus/configs/ \
  --env ETCD_AUTO_COMPACTION_MODE=revision \
  --env ETCD_AUTO_COMPACTION_RETENTION=1000 \
  --env ETCD_QUOTA_BACKEND_BYTES=4294967296 \
  -B "$ETCD_VOL:/etcd"\
   etcd_v3.5.18.sif \
    etcd \
        --name "etcd-${RANK}" \
        --advertise-client-urls "http://${IP_ADDR}:2379" \
        --listen-client-urls "http://0.0.0.0:2379" \
        --initial-advertise-peer-urls "http://${IP_ADDR}:2380" \
        --listen-peer-urls "http://0.0.0.0:2380" \
        --initial-cluster "${INITIAL_CLUSTER}" \
        --initial-cluster-state new \
        --data-dir /etcd  > etcd${RANK}.out 2>&1 

