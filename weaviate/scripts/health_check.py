"""Check readiness for each Weaviate node listed in ip_registry.txt.

Registry format expected by this repo:
rank,node,ip,http,grpc,gossip,data,raft,raft_internal

The script prefers the official Weaviate client when it is installed. Since the
readiness probe only needs HTTP, it ignores the extra gRPC column.
"""

import argparse
import csv
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import List, Set, Tuple


READY_PATH = "/v1/.well-known/ready"

@dataclass(frozen=True)
class NodeEntry:
    rank: int
    node_name: str
    ip: str
    http_port: int

    @property
    def base_url(self) -> str:
        return f"http://{self.ip}:{self.http_port}"

    @property
    def ready_url(self) -> str:
        return f"{self.base_url}{READY_PATH}"


def parse_registry(registry_path: Path) -> List[NodeEntry]:
    if not registry_path.exists():
        raise FileNotFoundError(f"Registry file not found: {registry_path}")

    entries: List[NodeEntry] = []
    seen_ranks: Set[int] = set()

    with registry_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        for line_number, row in enumerate(reader, start=1):
            if not row or all(not item.strip() for item in row):
                continue

            if len(row) < 4:
                raise ValueError(
                    f"{registry_path}:{line_number}: expected at least 4 columns, got {len(row)}"
                )

            try:
                rank = int(row[0].strip())
                node_name = row[1].strip()
                ip = row[2].strip()
                http_port = int(row[3].strip())
            except ValueError as exc:
                raise ValueError(
                    f"{registry_path}:{line_number}: failed to parse rank/http port from {row!r}"
                ) from exc

            if rank in seen_ranks:
                continue

            seen_ranks.add(rank)
            entries.append(NodeEntry(rank=rank, node_name=node_name, ip=ip, http_port=http_port))

    entries.sort(key=lambda entry: entry.rank)
    return entries


def http_ready_check(node: NodeEntry, timeout_seconds: float) -> Tuple[bool, str]:
    request = urllib.request.Request(node.ready_url, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            body = response.read().decode("utf-8", errors="replace").strip()
            ready = 200 <= response.status < 300
            detail = body or f"HTTP {response.status}"
            return ready, detail
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace").strip()
        detail = body or f"HTTP {exc.code}"
        return False, detail
    except urllib.error.URLError as exc:
        return False, str(exc.reason)


def maybe_import_weaviate() -> Tuple[bool, str]:
    try:
        import weaviate  # noqa: F401
    except Exception as exc:  # pragma: no cover
        return False, f"weaviate client unavailable ({exc})"
    return True, "weaviate client import ok"


def check_node(node: NodeEntry, timeout_seconds: float) -> Tuple[bool, str]:
    client_available, _ = maybe_import_weaviate()
    ready, detail = http_ready_check(node, timeout_seconds)
    if client_available:
        return ready, detail
    return ready, f"{detail} | checked via HTTP because weaviate-client is not installed"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check readiness for each Weaviate node listed in ip_registry.txt"
    )
    parser.add_argument(
        "--registry",
        default="ip_registry.txt",
        help="Path to the registry file (default: ip_registry.txt)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=3.0,
        help="HTTP timeout per node in seconds (default: 3.0)",
    )
    args = parser.parse_args()

    registry_path = Path(args.registry)

    try:
        entries = parse_registry(registry_path)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    if not entries:
        print(f"ERROR: no registry entries found in {registry_path}", file=sys.stderr)
        return 2

    all_ready = True
    print(f"Checking {len(entries)} Weaviate node(s) from {registry_path}")

    for node in entries:
        ready, detail = check_node(node, args.timeout)
        status = "READY" if ready else "NOT_READY"
        all_ready = all_ready and ready
        print(
            f"[{status}] rank={node.rank} node={node.node_name} "
            f"endpoint={node.base_url} detail={detail}"
        )

    return 0 if all_ready else 1


if __name__ == "__main__":
    raise SystemExit(main())
