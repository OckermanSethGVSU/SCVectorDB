# Milvus workflow

This workflow launches Milvus on PBS systems (standalone or distributed), runs multi-client insert/query operations, and can optionally perform index generation.

Main entrypoint:

- `pbs_submit_manager.sh`

## What is currently supported

- Task modes: `INSERT`, `INDEX`, `QUERY` (`TASK`)
- Deployment modes: `STANDALONE`, `DISTRIBUTED` (`MODE`)
- Platforms: `POLARIS`, `AURORA` (`PLATFORM`)
- Storage options (`STORAGE_MEDIUM`): `memory`, `DAOS`, `lustre`, `SSD`
- WAL configuration (`WAL`): `woodpecker`, `default`
- Perf modes (`PERF`): `NONE`, `STAT`, `RECORD`
- Insert/query balancing:
  - `INSERT_BALANCE_STRATEGY`: `NONE`, `WORKER`
  - `QUERY_BALANCE_STRATEGY`: `NONE`, `WORKER`
- Distributed controls:
  - `MINIO_MODE`: `single`, `stripped`
  - `MINIO_MEDIUM`: `DAOS`, `lustre`
  - `ETCD_MODE`: `single`, `replicated`
  - `STREAMING_NODES`, `STREAMING_NODES_PER_CN`
  - `NUM_PROXIES`, `NUM_PROXIES_PER_CN`
  - `DML_CHANNELS`

## Workflow files

- `pbs_submit_manager.sh`: config sweep + PBS generation + staging + `qsub`
- `main.sh`: runtime orchestration for standalone/distributed paths and task modes
- `check_dependencies.sh`: dependency validation helper
- `milvusSetup/*.sh`: component launch scripts
- `generalPython/*.py`: profiling/polling/setup/summary/index helpers
- `goCode/multiClientOP/`: Go multi-client binary used by the workflow
- `cpuMilvus/configs/`: Milvus config templates used at runtime
- `utils/`: tracing helpers, status checks, and utility scripts

## Important submit variables (`pbs_submit_manager.sh`)

### Sweep variables

- `NODES=(...)`
- `CORES=(...)`
- `INSERT_BATCH_SIZE=(...)`
- `QUERY_BATCH_SIZE=(...)`

### Runtime variables

- `TASK`
- `MODE`
- `STORAGE_MEDIUM`
- `PERF`
- `TRACING`
- `GPU_INDEX`
- `ENV_PATH`
- `MILVUS_BUILD_DIR`
- `MILVUS_CONFIG_DIR`
- `WAL`
- Insert/query variables:
  - `INSERT_DATA_FILEPATH`, `INSERT_CORPUS_SIZE`, `INSERT_CLIENTS_PER_PROXY`, `INSERT_BALANCE_STRATEGY`
  - `QUERY_DATA_FILEPATH`, `QUERY_CORPUS_SIZE`, `QUERY_CLIENTS_PER_PROXY`, `QUERY_BALANCE_STRATEGY`
- `VECTOR_DIM`, `DISTANCE_METRIC`
- `RESTORE_DIR`
- Distributed-only variables listed above

### Scheduler variables

- `WALLTIME`
- `queue`

## Runtime flow (`main.sh`)

1. Exports runtime variables and loads platform-specific modules/env.
2. Optionally mounts DAOS (`launch-dfuse.sh`) for Milvus and/or MinIO storage.
3. Launches Milvus:
   - `STANDALONE`: one server process.
   - `DISTRIBUTED`: etcd/minio + Milvus service roles.
4. Waits for readiness and, for insert/index, configures collection (`setup_collection.py`).
5. Runs Go multi-client operation binary (`./multiClientOP`) in insert/query modes.
6. If `TASK=INDEX`, runs `generalPython/index_data.py`.
7. Writes timing/summary outputs and optional trace analysis.

## Dependency check

Run manually:

```bash
./check_dependencies.sh
```

Or missing-only mode:

```bash
./check_dependencies.sh --missing-only
```

## Build notes

Build the Go client from `milvus/goCode`:

```bash
cd goCode
./build.sh multiClientOP
```

This produces `goCode/multiClientOP/multiClientOP`.

## Required local artifacts (not committed)

- `sifs/milvus.sif`
- `sifs/etcd_v3.5.18.sif`
- `sifs/minio.sif`
- Built Go client: `goCode/multiClientOP/multiClientOP`
- Optional tracing image: `sifs/otel-collector.sif` (when `TRACING=True`)

## Typical run outputs

Per run directory:

- `workflow.out`, `output.log`
- Task-specific timing/summary files
- `uploadNPY/*.npy` (for insert/index runs)
- Component logs under node/worker output folders
