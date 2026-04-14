import os
import yaml
import argparse
import shutil
from mpi4py import MPI


parser = argparse.ArgumentParser()
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

parser.add_argument(
    "--log_level",
    type=str,
    default="ERROR",
    help="Qdrant log level to write into generated config files",
)

args = parser.parse_args()

storage_medium = args.storage_medium
DAOS_PATH = args.path
log_level = args.log_level
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
elif storage_medium == "SSD":
    print("Using SDD for persistence",flush=True)
    targetBase = "/local/scratch/qdrantDir"

dirs = ["config","data","snapshots/"]
for dir in dirs:
    path  = f"{targetBase}/{dir}/node{rank}"

    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path, exist_ok=True)


# === Generate configs ===
node_dir = f"{targetBase}/config/node{rank}"
os.makedirs(node_dir, exist_ok=True)

config = {
    "log_level": log_level,
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
    },

}

if storage_medium == "DAOS":
    config.setdefault("cluster", {}).setdefault("consensus", {})
    config["cluster"]["consensus"]["consensus_wal_path"] = f"/dev/shm/node{rank}/consensus_wal"


config_path = os.path.join(node_dir, "config.yaml")
with open(config_path, "w") as f:
    yaml.dump(config, f, sort_keys=False)

config_path = os.path.join("./", "config.yaml")
with open(config_path, "w") as f:
    yaml.dump(config, f, sort_keys=False)


# === Create snapshot directories ===
node_dir = f"{targetBase}/snapshots/node{rank}"
os.makedirs(node_dir, exist_ok=True)


print(f"✅ Generated dirs in {targetBase} for node {rank}", flush=True)
