# Validation

Prefer validation that matches the part of the repo you changed.

## Unified Submit Changes

Inspect:

- [pbs_submit_manager.sh](/home/seth/Documents/research/SCVectorDB/pbs_submit_manager.sh)
- [common/submit_lib.sh](/home/seth/Documents/research/SCVectorDB/common/submit_lib.sh)
- [common/schema.sh](/home/seth/Documents/research/SCVectorDB/common/schema.sh)
- [common/engine_schema_lib.sh](/home/seth/Documents/research/SCVectorDB/common/engine_schema_lib.sh)

Useful checks:

```bash
./pbs_submit_manager.sh --help
./pbs_submit_manager.sh --help --engine qdrant
./pbs_submit_manager.sh --help --engine milvus
./pbs_submit_manager.sh --help --engine weaviate
```

## Qdrant Changes

Best local options:

- static inspection of `qdrant/engine.sh`, `qdrant/main.sh`, and `qdrant/local_main.sh`
- client builds via `qdrant/clients/build.sh`

## Milvus Changes

Best local options:

- static inspection of `milvus/engine.sh`, `milvus/main.sh`, and `milvus/local_main.sh`
- client builds via `milvus/clients/build.sh`

## Weaviate Changes

Best local options:

- static inspection of `weaviate/engine.sh`, `weaviate/main.sh`, and `weaviate/local_main.sh`
- client builds via `weaviate/clients/build.sh`

## Cluster-Only Gaps

If behavior depends on PBS, MPI, Apptainer, Aurora, Polaris, DAOS, or
site-local absolute paths, say so explicitly instead of pretending to validate
end to end.
