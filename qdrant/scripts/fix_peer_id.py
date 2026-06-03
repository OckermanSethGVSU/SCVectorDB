import json
import os
import shutil
import argparse
from urllib.parse import urlparse


def update_qdrant_raft_state(path, new_ip, new_port):
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")

    with open(path, "r") as f:
        data = json.load(f)

    this_peer_id = str(data["this_peer_id"])
    peer_map = data["peer_address_by_id"]

    if this_peer_id not in peer_map:
        raise KeyError(f"this_peer_id={this_peer_id} not found in peer_address_by_id")

    old_addr = peer_map[this_peer_id]

    parsed = urlparse(old_addr)
    scheme = parsed.scheme or "http"

    new_addr = f"{scheme}://{new_ip}:{new_port}/"

    if old_addr == new_addr:
        print(f"[info] Address already correct: {old_addr}")
        return

    # backup
    backup_path = path + ".bak"
    shutil.copy2(path, backup_path)
    print(f"[backup] wrote backup to {backup_path}")

    # update
    peer_map[this_peer_id] = new_addr

    with open(path, "w") as f:
        json.dump(data, f, indent=2)

    print(f"[done] Updated peer {this_peer_id}")
    print(f"       old: {old_addr}")
    print(f"       new: {new_addr}")


def main():
    parser = argparse.ArgumentParser(description="Update Qdrant raft_state.json peer IP/port")
    parser.add_argument("--path", required=True, help="Path to raft_state.json")
    parser.add_argument("--ip", required=True, help="New IP address")
    parser.add_argument("--port", required=True, type=int, help="New port")

    args = parser.parse_args()

    update_qdrant_raft_state(
        path=args.path,
        new_ip=args.ip,
        new_port=args.port,
    )


if __name__ == "__main__":
    main()