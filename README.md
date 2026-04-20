# SCVectorDB

SCVectorDB contains HPC workflows for vector database experiments across Qdrant, Milvus, and Weaviate.

## Layout

```text
.
├── pbs_submit_manager.sh
├── common/
├── graphing/
├── milvus/
├── qdrant/
└── weaviate/
```

## Unified submit flow

The repository uses a single top-level submit entrypoint:

```bash
./pbs_submit_manager.sh --help
./pbs_submit_manager.sh --help --engine qdrant
./pbs_submit_manager.sh --engine qdrant --config path/to/run.env
./pbs_submit_manager.sh --generate-only --engine qdrant --config path/to/run.env
```

Behavior:

- normal execution generates the run directory and submits if `RUN_MODE=PBS`
- `--generate-only` generates the run directory but does not submit
- `--help --engine <engine>` prints the engine variables, defaults, and requirement status

Each engine VDB its engine-specific logic in its own directory.


## Config model

Runs can be configured with:

- `--set KEY=value`
- `--config path/to/file.env`

Config files are plain `KEY=value` env-style files. Example:

```bash
ENGINE=qdrant
TASK=QUERY
RUN_MODE=local
INSERT_DATA_FILEPATH=/path/to/base.npy
QUERY_DATA_FILEPATH=/path/to/query.npy
VECTOR_DIM=768
```

For `RUN_MODE=local`, PBS-only settings such as `PLATFORM`, `WALLTIME`, `QUEUE`, and `ACCOUNT` are optional.

## Shared Schema Variables

The common variables below are defined in `common/schema.sh` and apply across all engines. Engine-specific schemas add more variables on top of these.

| Variable | Default | Required When | Allowed Values | Purpose |
|---|---|---|---|---|
| `TASK` | none | always | any | Experiment task |
| `RUN_MODE` | `PBS` | optional | `PBS`, `LOCAL`, `local` | Run under PBS or use a local harness |
| `PLATFORM` | none | `RUN_MODE=PBS` | `POLARIS`, `AURORA` | Target platform |
| `NODES` | none | `RUN_MODE=PBS` | any | Compute-node count for worker ranks |
| `CORES` | empty | optional | any | CPU cores per worker rank; empty disables explicit CPU binding |
| `STORAGE_MEDIUM` | `memory` | optional | `memory`, `DAOS`, `lustre`, `SSD` | Storage medium for engine data |
| `ACCOUNT` | none | `RUN_MODE=PBS` | any | PBS project/account |
| `WALLTIME` | none | `RUN_MODE=PBS` | any | PBS walltime |
| `QUEUE` | none | `RUN_MODE=PBS` | any | PBS queue name |
| `ENV_PATH` | empty | optional | any | Python environment path |
| `ALLOW_SYSTEM_PYTHON` | `False` | optional | `True`, `False` | Allow PBS runs to use the already-loaded Python environment when `ENV_PATH` is empty |
| `COLLECTION_NAME` | `default_collection` | optional | any | Optional collection override |
| `VECTOR_DIM` | none | always | any | Vector dimension |
| `DISTANCE_METRIC` | `COSINE` | optional | `IP`, `COSINE`, `L2` | Distance metric |
| `INSERT_DATA_FILEPATH` | none | `TASK=INSERT\|INDEX\|QUERY\|MIXED` | any | Insert corpus file path |
| `INSERT_CORPUS_SIZE` | empty | optional | any | Total vectors to preload; empty uses all rows in the file |
| `INSERT_BATCH_SIZE` | `512` | optional | any | Insert batch size; can be a sweep value |
| `INSERT_BALANCE_STRATEGY` | `WORKER_BALANCE` | optional | `NO_BALANCE`, `WORKER_BALANCE` | Insert balancing policy |
| `INSERT_STREAMING` | `False` | optional | `True`, `False` | Enable streaming insert behavior |
| `QUERY_DATA_FILEPATH` | none | `TASK=QUERY\|MIXED` | any | Query vector file path |
| `QUERY_CORPUS_SIZE` | empty | optional | any | Total queries to execute; empty uses all rows in the file |
| `QUERY_BATCH_SIZE` | `32` | optional | any | Query batch size; can be a sweep value |
| `QUERY_BALANCE_STRATEGY` | `NO_BALANCE` | `TASK=QUERY\|MIXED` | `NO_BALANCE`, `WORKER_BALANCE` | Query balancing policy |
| `QUERY_STREAMING` | `False` | optional | `True`, `False` | Enable streaming query behavior |
| `TOP_K` | `10` | optional | any | Optional top-k override |
| `BASE_DIR` | empty | optional | any | Base directory for generated run directories; auto-filled when empty |
