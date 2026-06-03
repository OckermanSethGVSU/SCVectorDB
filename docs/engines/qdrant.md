# Qdrant

Qdrant is an active engine in the unified submit workflow and supports both
`RUN_MODE=PBS` and `RUN_MODE=LOCAL`.

## Main Files

- [qdrant/engine.sh](/home/seth/Documents/research/SCVectorDB/qdrant/engine.sh)
- [qdrant/schema.sh](/home/seth/Documents/research/SCVectorDB/qdrant/schema.sh)
- [qdrant/main.sh](/home/seth/Documents/research/SCVectorDB/qdrant/main.sh)
- [qdrant/local_main.sh](/home/seth/Documents/research/SCVectorDB/qdrant/local_main.sh)

## Active Tasks

- `INSERT`
- `INDEX`
- `QUERY`
- `MIXED`
- `LAUNCH`

## Operational Notes

- PBS runs require `ENV_PATH` unless `ALLOW_SYSTEM_PYTHON=True`.
- PBS runs require `QDRANT_SIF` as a filename under `qdrant/sifs/`.
- Local runs use `qdrant/local_main.sh` and a Docker or Podman container.
- Mixed runs derive `INSERT_START_ID` from existing data or restore metadata
  when possible.

## Build Commands

```bash
cd qdrant/clients
./build.sh batch_client
./build.sh mixed
```
