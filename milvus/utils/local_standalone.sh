#!/usr/bin/env bash


# Modified version of https://github.com/milvus-io/milvus/blob/master/scripts/standalone_embed.sh

# Licensed to the LF AI & Data foundation under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "${SCRIPT_DIR}/.." && pwd)
DEFAULT_VECTOR_COUNT="${VECTOR_COUNT:-1000}"
DEFAULT_VECTOR_DIM="${VECTOR_DIM:-200}"
DEFAULT_DISTANCE_METRIC="${DISTANCE_METRIC:-L2}"
DEFAULT_COLLECTION_NAME="${COLLECTION_NAME:-standalone}"
DEFAULT_VECTOR_FILE="${VECTOR_FILEPATH:-${SCRIPT_DIR}/random.npy}"
DEFAULT_PROXY_HOST="${MILVUS_HOST:-127.0.0.1}"
DEFAULT_PROXY_PORT="${MILVUS_GRPC_PORT:-19530}"
DEFAULT_METRICS_PORT="${MILVUS_HEALTH_PORT:-9091}"

write_standalone_registry_files() {
    local proxy_host="${1:-$DEFAULT_PROXY_HOST}"
    local proxy_port="${2:-$DEFAULT_PROXY_PORT}"
    local metrics_port="${3:-$DEFAULT_METRICS_PORT}"
    local -a target_dirs=(
        "$PWD"
        "$REPO_ROOT"
        "$REPO_ROOT/clients/batch_client"
    )
    local dir
    local registry_line="0,${proxy_host},${proxy_port},${metrics_port}"
    declare -A seen=()

    for dir in "${target_dirs[@]}"; do
        if [[ -n "${seen[$dir]}" ]]; then
            continue
        fi
        seen[$dir]=1

        printf '%s\n' "$proxy_host" > "${dir}/worker.ip"
        printf '%s\n' "$registry_line" > "${dir}/PROXY_registry.txt"
    done
}

ensure_random_vector_file() {
    local vector_path="${1:-$DEFAULT_VECTOR_FILE}"
    local vector_count="${2:-$DEFAULT_VECTOR_COUNT}"
    local vector_dim="${3:-$DEFAULT_VECTOR_DIM}"

    VECTOR_PATH="$vector_path" VECTOR_COUNT="$vector_count" VECTOR_DIM="$vector_dim" python3 - <<'PY'
import os
from pathlib import Path

import numpy as np

vector_path = Path(os.environ["VECTOR_PATH"])
vector_count = int(os.environ["VECTOR_COUNT"])
vector_dim = int(os.environ["VECTOR_DIM"])

if vector_count <= 0:
    raise ValueError(f"VECTOR_COUNT must be positive, got {vector_count}")

if vector_dim <= 0:
    raise ValueError(f"VECTOR_DIM must be positive, got {vector_dim}")

vector_path.parent.mkdir(parents=True, exist_ok=True)

if not vector_path.exists():
    rng = np.random.default_rng()
    vectors = rng.random((vector_count, vector_dim), dtype=np.float32)
    np.save(vector_path, vectors)
    print(f"Created {vector_path} with shape {vectors.shape}.")
else:
    data = np.load(vector_path, mmap_mode="r")
    print(f"Using existing {vector_path} with shape {data.shape}.")
PY
}

setup_collection_from_vector_file() {
    local vector_path="${1:-$DEFAULT_VECTOR_FILE}"
    local collection_name="${2:-$DEFAULT_COLLECTION_NAME}"
    local metric="${3:-$DEFAULT_DISTANCE_METRIC}"

    VECTOR_PATH="$vector_path" COLLECTION_NAME="$collection_name" DISTANCE_METRIC="$metric" python3 - <<'PY'
import os

import numpy as np
from pymilvus import CollectionSchema, DataType, FieldSchema, MilvusClient

vector_path = os.environ["VECTOR_PATH"]
collection_name = os.environ["COLLECTION_NAME"]
distance_metric = os.environ["DISTANCE_METRIC"].strip().lower()

vectors = np.load(vector_path, mmap_mode="r")
if vectors.ndim == 1:
    vector_dim = int(vectors.shape[0])
elif vectors.ndim >= 2:
    vector_dim = int(vectors.shape[-1])
else:
    raise ValueError(f"Unsupported vector shape: {vectors.shape}")

match distance_metric:
    case "dot" | "ip" | "innerproduct":
        metric = "IP"
    case "cosine":
        metric = "COSINE"
    case "euclidan" | "l2":
        metric = "L2"
    case _:
        raise ValueError(f"Unknown distance metric: {distance_metric}")

client = MilvusClient("http://localhost:19530", token=os.getenv("MILVUS_TOKEN", "root:Milvus"))

desired_schema = CollectionSchema(
    fields=[
        FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=False),
        FieldSchema(name="vector", dtype=DataType.FLOAT_VECTOR, dim=vector_dim),
    ]
)

index_params = client.prepare_index_params()
index_params.add_index(
    field_name="vector",
    index_type="FLAT",
    metric_type=metric,
    params={},
)

if client.has_collection(collection_name):
    desc = client.describe_collection(collection_name=collection_name)
    existing_dim = None
    for field in desc.get("fields", []):
        if field.get("name") == "vector":
            params = field.get("params", {})
            existing_dim = params.get("dim") or field.get("dim")
            break

    if existing_dim is not None and int(existing_dim) == vector_dim:
        print(
            f"Collection '{collection_name}' already exists with vector dim {vector_dim}; leaving it unchanged."
        )
        raise SystemExit(0)

    client.drop_collection(collection_name)
    print(
        f"Dropped existing collection '{collection_name}' to recreate it with vector dim {vector_dim}."
    )

client.create_collection(
    collection_name=collection_name,
    schema=desired_schema,
    index_params=index_params,
)
print(f"Created collection '{collection_name}' with vector dim {vector_dim}.")
PY
}

run_embed() {
    cat << EOF > embedEtcd.yaml
listen-client-urls: http://0.0.0.0:2379
advertise-client-urls: http://0.0.0.0:2379
quota-backend-bytes: 4294967296
auto-compaction-mode: revision
auto-compaction-retention: '1000'
EOF

    cat << EOF > user.yaml
# Extra config to override default milvus.yaml
EOF
    if [ ! -f "./embedEtcd.yaml" ]
    then
        echo "embedEtcd.yaml file does not exist. Please try to create it in the current directory."
        exit 1
    fi

    if [ ! -f "./user.yaml" ]
    then
        echo "user.yaml file does not exist. Please try to create it in the current directory."
        exit 1
    fi
    
    sudo docker run -d \
        --name milvus-standalone \
        --security-opt seccomp:unconfined \
        -e ETCD_USE_EMBED=true \
        -e ETCD_DATA_DIR=/var/lib/milvus/etcd \
        -e ETCD_CONFIG_PATH=/milvus/configs/embedEtcd.yaml \
        -e COMMON_STORAGETYPE=local \
        -e DEPLOY_MODE=STANDALONE \
        -v $(pwd)/volumes/milvus:/var/lib/milvus \
        -v $(pwd)/embedEtcd.yaml:/milvus/configs/embedEtcd.yaml \
        -v $(pwd)/user.yaml:/milvus/configs/user.yaml \
        -p 19530:19530 \
        -p 9091:9091 \
        -p 2379:2379 \
        --health-cmd="curl -f http://localhost:9091/healthz" \
        --health-interval=30s \
        --health-start-period=90s \
        --health-timeout=20s \
        --health-retries=3 \
        milvusdb/milvus:v2.6.12 \
        milvus run standalone  1> /dev/null
}

wait_for_milvus_running() {
    echo "Wait for Milvus Starting..."
    while true
    do
        res=`sudo docker ps|grep milvus-standalone|grep healthy|wc -l`
        if [ $res -eq 1 ]
        then
            echo "Start successfully."
            echo "To change the default Milvus configuration, add your settings to the user.yaml file and then restart the service."
            break
        fi
        sleep 1
    done
}

start() {
    res=`sudo docker ps|grep milvus-standalone|grep healthy|wc -l`
    if [ $res -eq 1 ]
    then
        echo "Milvus is running."
        write_standalone_registry_files "$DEFAULT_PROXY_HOST" "$DEFAULT_PROXY_PORT" "$DEFAULT_METRICS_PORT"
        ensure_random_vector_file "$DEFAULT_VECTOR_FILE" "$DEFAULT_VECTOR_COUNT" "$DEFAULT_VECTOR_DIM"
        setup_collection_from_vector_file "$DEFAULT_VECTOR_FILE" "$DEFAULT_COLLECTION_NAME" "$DEFAULT_DISTANCE_METRIC"
        exit 0
    fi

    res=`sudo docker ps -a|grep milvus-standalone|wc -l`
    if [ $res -eq 1 ]
    then
        sudo docker start milvus-standalone 1> /dev/null
    else
        run_embed
    fi

    if [ $? -ne 0 ]
    then
        echo "Start failed."
        exit 1
    fi

    wait_for_milvus_running
    write_standalone_registry_files "$DEFAULT_PROXY_HOST" "$DEFAULT_PROXY_PORT" "$DEFAULT_METRICS_PORT"
    ensure_random_vector_file "$DEFAULT_VECTOR_FILE" "$DEFAULT_VECTOR_COUNT" "$DEFAULT_VECTOR_DIM"
    setup_collection_from_vector_file "$DEFAULT_VECTOR_FILE" "$DEFAULT_COLLECTION_NAME" "$DEFAULT_DISTANCE_METRIC"
}

stop() {
    sudo docker stop milvus-standalone 1> /dev/null

    if [ $? -ne 0 ]
    then
        echo "Stop failed."
        exit 1
    fi
    echo "Stop successfully."

}

delete_container() {
    res=`sudo docker ps|grep milvus-standalone|wc -l`
    if [ $res -eq 1 ]
    then
        echo "Please stop Milvus service before delete."
        exit 1
    fi
    sudo docker rm milvus-standalone 1> /dev/null
    if [ $? -ne 0 ]
    then
        echo "Delete milvus container failed."
        exit 1
    fi
    echo "Delete milvus container successfully."
}

delete() {
    read -p "Please confirm if you'd like to proceed with the delete. This operation will delete the container and data. Confirm with 'y' for yes or 'n' for no. > " check
    if [ "$check" == "y" ] ||[ "$check" == "Y" ];then
        delete_container
        sudo rm -rf $(pwd)/volumes
        sudo rm -rf $(pwd)/embedEtcd.yaml
        sudo rm -rf $(pwd)/user.yaml
        echo "Delete successfully."
    else
        echo "Exit delete"
        exit 0
    fi
}

upgrade() {
    read -p "Please confirm if you'd like to proceed with the upgrade. The default will be to the latest version. Confirm with 'y' for yes or 'n' for no. > " check
    if [ "$check" == "y" ] ||[ "$check" == "Y" ];then
        res=`sudo docker ps -a|grep milvus-standalone|wc -l`
        if [ $res -eq 1 ]
        then
            stop
            delete_container
        fi

        curl -sfL https://raw.githubusercontent.com/milvus-io/milvus/master/scripts/standalone_embed.sh -o standalone_embed_latest.sh && \
        bash standalone_embed_latest.sh start 1> /dev/null && \
        echo "Upgrade successfully."
    else
        echo "Exit upgrade"
        exit 0
    fi
}

case $1 in
    restart)
        stop
        start
        ;;
    start)
        start
        ;;
    stop)
        stop
        ;;
    upgrade)
        upgrade
        ;;
    delete)
        delete
        ;;
    *)
        echo "please use bash standalone_embed.sh restart|start|stop|upgrade|delete"
        ;;
esac
