import subprocess
import argparse
import json

def get_running_interface_ips(include_loopback=False):
    """
    Returns a dict mapping interface name -> {"ipv4": [..], "ipv6": [..]}
    Only includes interfaces that are currently UP (aka 'running').

    Set include_loopback=True if you want 'lo' included.
    """
    # 1) Get interfaces that are UP
    try:
        link_out = subprocess.check_output(
            ["ip", "-o", "link", "show", "up"],
            text=True
        )
    except (FileNotFoundError, subprocess.CalledProcessError) as e:
        raise RuntimeError("Failed to run 'ip link'. Is iproute2 installed?") from e

    running_ifaces = set()
    for line in link_out.splitlines():
        # Format: "2: eth0: <BROADCAST,MULTICAST,UP,LOWER_UP> ..."
        # interface name is the part between the first and second colon
        parts = line.split(":", 2)
        if len(parts) >= 2:
            name = parts[1].strip()
            if include_loopback or name != "lo":
                running_ifaces.add(name)

    if not running_ifaces:
        return {}

    # 2) Get addresses for all interfaces (one line per address)
    try:
        addr_out = subprocess.check_output(
            ["ip", "-o", "addr", "show"],
            text=True
        )
    except subprocess.CalledProcessError as e:
        raise RuntimeError("Failed to run 'ip addr'.") from e

    result = {}
    for line in addr_out.splitlines():
        # Example (ipv4):
        # "2: eth0    inet 192.168.1.10/24 brd 192.168.1.255 scope global eth0 ..."
        # Example (ipv6):
        # "2: eth0    inet6 fe80::a00:27ff:fe4e:66a1/64 scope link ..."
        tokens = line.split()
        if len(tokens) < 4:
            continue

        ifname = tokens[1]
        family = tokens[2]  # "inet" or "inet6" (there are also "link/..." lines we ignore)
        if ifname not in running_ifaces or family not in ("inet", "inet6"):
            continue

        cidr = tokens[3]  # e.g. "192.168.1.10/24" or "fe80::1/64"
        ip = cidr.split("/")[0]

        # Determine scope if available (prefer 'global' addresses)
        scope = None
        if "scope" in tokens:
            try:
                scope = tokens[tokens.index("scope") + 1]
            except IndexError:
                scope = None

        # Initialize per-interface containers
        bucket = result.setdefault(ifname, {"ipv4": [], "ipv6": []})

        if family == "inet":
            # If scope not present, accept; otherwise prefer 'global'
            if scope in (None, "global") or (scope not in ("global", "host", "link")):
                bucket["ipv4"].append(ip)
        elif family == "inet6":
            if scope in (None, "global") or (scope not in ("global", "host", "link")):
                bucket["ipv6"].append(ip)

    # 3) (Optional) If you want to keep only 'global' when available, fall back to any:
    def prefer_global(addresses, family_label):
        # Nothing to do here because we already preferred 'global' above by filtering.
        # But you could implement extra logic if needed.
        return addresses

    for ifname, ips in result.items():
        ips["ipv4"] = prefer_global(ips["ipv4"], "ipv4")
        ips["ipv6"] = prefer_global(ips["ipv6"], "ipv6")

    # Drop interfaces that ended up with no IPs after filtering
    result = {k: v for k, v in result.items() if v["ipv4"] or v["ipv6"]}

    return result


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Example script that takes a --rank argument")
    parser.add_argument(
        "--rank",
        type=int,              # or str if you expect a non-numeric rank
        required=True,         # make it mandatory; remove this if optional
        help="Rank number to use (integer)"
    )

    args = parser.parse_args()
    rank = args.rank

    if rank % 4 == 0:
        mapping = get_running_interface_ips(include_loopback=False)

        with open(f"interfaces{rank}.json", "w") as f:
            json.dump(mapping, f, indent=4)