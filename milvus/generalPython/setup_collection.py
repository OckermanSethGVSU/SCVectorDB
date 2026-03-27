import os
import json
import time
import requests
from pymilvus import MilvusClient
from pymilvus import FieldSchema, CollectionSchema, DataType
from pathlib import Path

def get_streaming_count(filename="STREAMING_registry.txt"):
    path = Path(filename)

    if not path.exists():
        return 1

    with path.open("r") as f:
        line_count = sum(1 for _ in f)

    return line_count

def read_ip_from_file(path):
    with open(path, 'r', encoding='utf-8') as f:
        return f.read().strip()

MILVUS_HOST = read_ip_from_file("worker.ip")
MILVUS_GRPC_PORT = int(os.getenv("MILVUS_GRPC_PORT", "20001"))
MILVUS_TOKEN = os.getenv("MILVUS_TOKEN", "root:Milvus")

client = MilvusClient(f"http://{MILVUS_HOST}:{MILVUS_GRPC_PORT}", token=MILVUS_TOKEN)
collection_name = "standalone"

if client.has_collection(collection_name):
    client.drop_collection(collection_name)
    print(f"Dropped existing collection '{collection_name}' to avoid schema conflict.", flush=True)
else:
    print(f"No existing collection named '{collection_name}' found.", flush=True)


# Define fields
id_field = FieldSchema(
    name="id",
    dtype=DataType.INT64,
    is_primary=True,
    auto_id=False,
    mmap_enabled=False,
)

VECTOR_DIM = int(os.getenv("VECTOR_DIM", "2560"))
vector_field = FieldSchema(
    name="vector",
    dtype=DataType.FLOAT_VECTOR,
    dim=VECTOR_DIM,
    mmap_enabled=False,
)
schema = CollectionSchema(fields=[id_field, vector_field])

distance_metric = os.environ["DISTANCE_METRIC"].strip().lower()
match distance_metric:
    case "dot" | "ip" | "innerproduct":
        metric = "IP"
    case "cosine":
        metric = "COSINE"
    case "euclidan" | "l2":
        metric = "L2"
    case _:
        raise ValueError(f"Unknown distance metric: {distance_metric}")


init_flat_index = os.getenv("INIT_FLAT_INDEX", "TRUE").strip().lower() == "true"


streaming_value = get_streaming_count()

create_collection_kwargs = {
    "collection_name": collection_name,
    "schema": schema,
    "num_shards": streaming_value,
    "properties": {"mmap.enabled": False},
}

if init_flat_index:
    index_params = client.prepare_index_params()
    index_params.add_index(
        field_name="vector",
        index_type="FLAT",
        metric_type=metric,
        params={},
        mmap_enabled=False,
    )
    create_collection_kwargs["index_params"] = index_params
    print(f"Creating collection '{collection_name}' with a FLAT index.", flush=True)
else:
    print(f"Creating collection '{collection_name}' without an index.", flush=True)

client.create_collection(**create_collection_kwargs)

print(f"Collection shards: {streaming_value}. Indexes for {collection_name}: ", client.list_indexes(collection_name), flush=True)
if init_flat_index:
    print(client.describe_index(collection_name, "vector"), flush=True)

