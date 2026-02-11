import os
import yaml
import argparse
import shutil
from mpi4py import MPI


parser = argparse.ArgumentParser()
# parser.add_argument(
#     "--search_threads",
#     type=int,
#     default=0,
#     help="Number of search threads (default: 0)",
# )


# parser.add_argument(
#     "--search_threads",
#     type=int,
#     default=0,
#     help="Number of search threads (default: 0)",
# )

parser.add_argument(
    "--storage_medium",
    type=str,
    default="memory",
    help="What storage medium to use for persistence",
)

parser.add_argument(
    "--path",
    type=str,
    default="",
    help="The path for DAOS",
)

args = parser.parse_args()
# search_threads = args.search_threads
# number_segments = args.number_segments
storage_medium = args.storage_medium
DAOS_PATH = args.path

comm = MPI.COMM_WORLD
rank = comm.Get_rank()


base_ports = {
    "http": 6333,
    "grpc": 6334,
    "p2p": 6335
}
tick_period_ms = 100

# === Ensure config directory exists ===

if storage_medium == "memory":
    targetBase = "/dev/shm/qdrantDir"
    print("Using memory for persistence",flush=True)
elif storage_medium == "DAOS":
    print("Using DAOS for persistence",flush=True)
    targetBase = f"{DAOS_PATH}/qdrantDir"

elif storage_medium == "lustre":
    print("Using lustre for persistence",flush=True)
    targetBase = "./qdrantDir"

dirs = ["config","data","snapshots/"]
for dir in dirs:
    path  = f"{targetBase}/{dir}/node{rank}"

    # path  = f"/dev/shm/qdrantDIR/{dir}/node{rank}"
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path, exist_ok=True)


# === Generate configs ===

# node_dir = f"/dev/shm/qdrantDIR/config/node{rank}"
node_dir = f"{targetBase}/config/node{rank}"
os.makedirs(node_dir, exist_ok=True)

config = {
    "log_level": "ERROR",
    "service": {
        "http_port": base_ports["http"] + rank * 100,
        "grpc_port": base_ports["grpc"] + rank * 100,
        "max_request_size_mb": 1024 
    },
    "cluster": {
        "enabled": True,
        "p2p": {
            "port": base_ports["p2p"] + rank * 100
        },
        # "consensus": {
        #     "tick_period_ms": tick_period_ms
        # }
    },
    # "storage": {
    #     "performance": {
    #         "max_search_threads" : search_threads
    #     },
    #     # "optimizers":{
    #     #     "default_segment_number" : number_segments,
    #     #     # "max_segment_size_kb" : 160000000
    #     # }

    # }
}

config_path = os.path.join(node_dir, "config.yaml")
with open(config_path, "w") as f:
    yaml.dump(config, f, sort_keys=False)




config_path = os.path.join("./", "config.yaml")
with open(config_path, "w") as f:
    yaml.dump(config, f, sort_keys=False)


# === Create new data and snapshot directories ===
node_dir = f"{targetBase}/snapshots/node{rank}"
os.makedirs(node_dir, exist_ok=True)
# node_dir = f"./qdrantDIR/data/node{rank}"
# os.makedirs(node_dir, exist_ok=True)
# node_dir = f"/dev/shm/qdrantDIR/data/node{rank}"
# node_dir = f"./qdrantDIR/snapshots/node{rank}"
# node_dir = f"/dev/shm/qdrantDIR/snapshots/node{rank}"
# os.makedirs(node_dir, exist_ok=True)


print(f"✅ Generated dirs in {targetBase} for node {rank}", flush=True)
