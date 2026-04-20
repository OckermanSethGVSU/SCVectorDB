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

### Control

- `TASK`: experiment task to run
- `RUN_MODE`: `PBS` by default; use `LOCAL` or `local` for local harnesses

### Platform and allocation

- `PLATFORM`: PBS-only platform selector; currently `POLARIS` or `AURORA`
- `NODES`: PBS-only compute node count for worker ranks
- `CORES`: CPU cores per worker rank; leave empty to disable explicit CPU binding
- `STORAGE_MEDIUM`: storage target for engine data; default `memory`

### PBS and Python environment

- `ACCOUNT`: PBS project/account
- `WALLTIME`: PBS walltime
- `QUEUE`: PBS queue name
- `ENV_PATH`: Python environment path to activate for PBS runs
- `ALLOW_SYSTEM_PYTHON`: default `False`; set `True` to use the already-loaded Python environment when `ENV_PATH` is empty

### Collection and vector settings

- `COLLECTION_NAME`: optional collection override; default `default_collection`
- `VECTOR_DIM`: vector dimension
- `DISTANCE_METRIC`: default `COSINE`; allowed values are `IP`, `COSINE`, and `L2`

### Insert workload

- `INSERT_DATA_FILEPATH`: insert corpus `.npy` file path
- `INSERT_CORPUS_SIZE`: total vectors available to preload; leave empty to use all rows in the file
- `INSERT_BATCH_SIZE`: insert batch size; default `512`
- `INSERT_BALANCE_STRATEGY`: default `WORKER_BALANCE`; allowed values are `NO_BALANCE` and `WORKER_BALANCE`
- `INSERT_STREAMING`: default `False`; enables streaming insert behavior

### Query workload

- `QUERY_DATA_FILEPATH`: query vector `.npy` file path
- `QUERY_CORPUS_SIZE`: total queries to execute; leave empty to use all rows in the file
- `QUERY_BATCH_SIZE`: query batch size; default `32`
- `QUERY_BALANCE_STRATEGY`: default `NO_BALANCE`; allowed values are `NO_BALANCE` and `WORKER_BALANCE`
- `QUERY_STREAMING`: default `False`; enables streaming query behavior
- `TOP_K`: default `10`; optional top-k override

### Output path

- `BASE_DIR`: base directory for generated run directories; auto-filled by the submit manager when empty
