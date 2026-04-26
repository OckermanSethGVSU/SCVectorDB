#!/usr/bin/env python3
"""Create a minimal Weaviate collection for UUID + self-provided vectors.

This repo's `ip_registry.txt` stores the HTTP port but not the gRPC port.
`launchWeaviateNode.sh` defines the gRPC port as `HTTP_PORT + 2`, so this
script derives it the same way when connecting with the Python client.
"""

import argparse
import csv
import pprint
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List, Set

import weaviate
import weaviate.classes as wvc


@dataclass(frozen=True)
class NodeEntry:
    rank: int
    node_name: str
    ip: str
    http_port: int

    @property
    def grpc_port(self) -> int:
        return self.http_port + 2


def parse_registry(registry_path: Path) -> List[NodeEntry]:
    if not registry_path.exists():
        raise FileNotFoundError("Registry file not found: {0}".format(registry_path))

    entries = []
    seen_ranks = set()  # type: Set[int]

    with registry_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        for line_number, row in enumerate(reader, start=1):
            if not row or all(not item.strip() for item in row):
                continue

            if len(row) < 4:
                raise ValueError(
                    "{0}:{1}: expected at least 4 columns, got {2}".format(
                        registry_path, line_number, len(row)
                    )
                )

            try:
                rank = int(row[0].strip())
                node_name = row[1].strip()
                ip = row[2].strip()
                http_port = int(row[3].strip())
            except ValueError as exc:
                raise ValueError(
                    "{0}:{1}: failed to parse rank/http port from {2!r}".format(
                        registry_path, line_number, row
                    )
                ) from exc

            if rank in seen_ranks:
                continue

            seen_ranks.add(rank)
            entries.append(NodeEntry(rank=rank, node_name=node_name, ip=ip, http_port=http_port))

    entries.sort(key=lambda entry: entry.rank)
    return entries


def connect_from_registry(registry_path: Path, rank: int):
    entries = parse_registry(registry_path)
    if not entries:
        raise ValueError("No registry entries found in {0}".format(registry_path))

    matches = [entry for entry in entries if entry.rank == rank]
    if not matches:
        raise ValueError("Rank {0} not found in {1}".format(rank, registry_path))

    node = matches[0]
    client = weaviate.connect_to_custom(
        http_host=node.ip,
        http_port=node.http_port,
        http_secure=False,
        grpc_host=node.ip,
        grpc_port=node.grpc_port,
        grpc_secure=False,
    )
    return client, node


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Create a minimal Weaviate collection with UUID + vector only"
    )
    parser.add_argument(
        "--registry",
        default="ip_registry.txt",
        help="Path to the registry file (default: ip_registry.txt)",
    )
    parser.add_argument(
        "--rank",
        type=int,
        default=0,
        help="Registry rank to connect to (default: 0)",
    )
    parser.add_argument(
        "--collection",
        default="BasicVectorCollection",
        help="Collection name to create (default: BasicVectorCollection)",
    )
    parser.add_argument(
        "--drop-if-exists",
        action="store_true",
        help="Delete the collection first if it already exists",
    )
    args = parser.parse_args()

    registry_path = Path(args.registry)
    client = None

    try:
        client, node = connect_from_registry(registry_path, args.rank)

        if not client.is_ready():
            print(
                "ERROR: Weaviate node at http://{0}:{1} is not ready".format(
                    node.ip, node.http_port
                ),
                file=sys.stderr,
            )
            return 1

        exists = client.collections.exists(args.collection)
        if exists and args.drop_if_exists:
            client.collections.delete(args.collection)
            exists = False
            print("Deleted existing collection {0}".format(args.collection))

        if exists:
            print("Collection {0} already exists".format(args.collection))
            collection = client.collections.use(args.collection)
            collection_config = collection.config.get()
            print("Collection status: exists=True" ,flush=True)
            print("Collection definition:", flush=True)
            print(collection_config, flush=True)
            return 0

        client.collections.create(
            name=args.collection,
            description="Minimal collection storing only UUIDs and self-provided vectors",
            vector_config=wvc.config.Configure.Vectors.self_provided(),
            properties=[],
        )

        print(
            "Created collection {0} on http://{1}:{2} using grpc port {3}".format(
                args.collection, node.ip, node.http_port, node.grpc_port
            ), flush=True
        )
        print("Schema: no user properties; objects store UUID plus supplied vector")
        collection = client.collections.use(args.collection)
        collection_config = collection.config.get()
        print("Collection status: exists={0}".format(client.collections.exists(args.collection)))
        print("Collection definition:")
        print(collection_config, flush=True)
        return 0

    except Exception as exc:
        print("ERROR: {0}".format(exc), file=sys.stderr)
        return 1
    finally:
        if client is not None:
            client.close()


if __name__ == "__main__":
    raise SystemExit(main())
