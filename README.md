# SCVectorDB

SCVectorDB is a workflow repository for vector-database experiments across
Qdrant, Milvus, and Weaviate. The active control plane is the repo-root
`pbs_submit_manager.sh`, which expands config, generates run directories, and
submits PBS jobs when appropriate.

## Start Here

```bash
./pbs_submit_manager.sh --help
./pbs_submit_manager.sh --help --engine qdrant
./pbs_submit_manager.sh --engine milvus --config path/to/run.env
./pbs_submit_manager.sh --generate-only --engine weaviate --config path/to/run.env
```

## Docs

- [Repo overview](docs/overview.md)
- [Unified submit workflow](docs/unified-submit.md)
- [Config reference](docs/config-reference.md)
- [Generated artifacts](docs/generated-artifacts.md)
- [Validation guidance](docs/validation.md)
- [Qdrant engine docs](docs/engines/qdrant.md)
- [Milvus engine docs](docs/engines/milvus.md)
- [Weaviate engine docs](docs/engines/weaviate.md)

## Repository Layout

```text
.
├── pbs_submit_manager.sh
├── common/
├── milvus/
├── qdrant/
└── weaviate/
```

## Notes

- This repo mixes tracked source files with generated run directories and
  experiment outputs.
- Prefer editing tracked source under `common/`, `qdrant/`, `milvus/`, and
  `weaviate/`, not generated run directories.
- The schema files are the source of truth for config variables:
  [common/schema.sh](/home/seth/Documents/research/SCVectorDB/common/schema.sh),
  [qdrant/schema.sh](/home/seth/Documents/research/SCVectorDB/qdrant/schema.sh),
  [milvus/schema.sh](/home/seth/Documents/research/SCVectorDB/milvus/schema.sh),
  and [weaviate/schema.sh](/home/seth/Documents/research/SCVectorDB/weaviate/schema.sh).
