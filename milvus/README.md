# Milvus workflow

This directory contains the Milvus workflows used in this repo: standalone and distributed cluster launch paths, Go client binaries for insert/query, and a Go mixed insert/query runner for event-timeline experiments.

Main HPC entrypoint:

- `pbs_submit_manager.sh`

## Current workflow capabilities

- Task modes: `INSERT`, `INDEX`, `QUERY` (`TASK`)
- Deployment modes: `STANDALONE`, `DISTRIBUTED` (`MODE`)
- Platforms: `POLARIS`, `AURORA` (`PLATFORM`)
- Storage media: `memory`, `DAOS`, `lustre`, `SSD` (`STORAGE_MEDIUM`)
- WAL settings: `woodpecker`, `default` (`WAL`)
- Perf modes: `NONE`, `STAT`, `RECORD` (`PERF`)
- Optional tracing via `TRACING=True`
- Insert/query balancing used by the Go clients:
  - `INSERT_BALANCE_STRATEGY`: `NONE`, `WORKER`
  - `QUERY_BALANCE_STRATEGY`: `NONE`, `WORKER`
- Distributed controls:
  - `MINIO_MODE`: `single`, `stripped`
  - `MINIO_MEDIUM`: `DAOS`, `lustre`
  - `ETCD_MODE`: `single`, `replicated`
  - `STREAMING_NODES`, `STREAMING_NODES_PER_CN`
  - `NUM_PROXIES`, `NUM_PROXIES_PER_CN`
  - `DML_CHANNELS`

## Important files

- `pbs_submit_manager.sh`: sweep configuration and PBS script generation
- `main.sh`: runtime orchestration for standalone and distributed runs
- `check_dependencies.sh`: dependency validation helper
- `milvusSetup/`: launch scripts for Milvus, etcd, and MinIO
- `generalPython/`: collection setup, profiling, polling, indexing, summaries, and helper scripts
- `goCode/multiClientOP/`: main Go client used by `main.sh`
- `goCode/mixedrunner/`: mixed insert/query Go runner with JSONL event logs
- `goCode/run_mixed.sh`: example launcher for the mixed runner
- `cpuMilvus/configs/`: Milvus config templates used at runtime
- `utils/`: tracing helpers, status scripts, local standalone helpers, and timeline analysis

## HPC runtime flow (`main.sh`)

1. Export run variables and activate the configured platform-specific Python environment.
2. Optionally mount DAOS and start OpenTelemetry when tracing is enabled.
3. Launch either:
   - standalone Milvus on one worker node, or
   - distributed etcd/MinIO plus Milvus service roles across worker nodes.
4. Wait for readiness and determine the reachable Milvus address.
5. For insert/index paths, configure the collection with `generalPython/setup_collection.py`.
6. Run the Go multi-client binary for insert or query workload generation.
7. If `TASK=INDEX`, run `generalPython/index_data.py`.
8. Write timing outputs, summaries, optional tracing data, and worker logs.

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
- `WAL`
- `GPU_INDEX`
- `DEBUG`
- `BASE_DIR`
- `ENV_PATH`
- `MILVUS_BUILD_DIR`
- `MILVUS_CONFIG_DIR`
- Insert path:
  - `INSERT_DATA_FILEPATH`
  - `INSERT_CORPUS_SIZE`
  - `INSERT_CLIENTS_PER_PROXY`
  - `INSERT_BALANCE_STRATEGY`
  - `INSERT_BATCH_SIZE`
- Query path:
  - `QUERY_DATA_FILEPATH`
  - `QUERY_CORPUS_SIZE`
  - `QUERY_CLIENTS_PER_PROXY`
  - `QUERY_BALANCE_STRATEGY`
  - `QUERY_BATCH_SIZE`
- Collection/index path:
  - `VECTOR_DIM`
  - `DISTANCE_METRIC`
  - `RESTORE_DIR`
  - `EXPECTED_CORPUS_SIZE`
- Distributed-only controls listed above

### Scheduler variables

- `WALLTIME`
- `queue`

## Go clients

Build from `milvus/goCode`:

```bash
cd goCode
./build.sh multiClientOP
./build.sh mixedrunner
```

Important binaries/scripts:

- `goCode/multiClientOP/multiClientOP`: insert/query client used by `main.sh`
- `goCode/mixedrunner/mixedrunner`: mixed insert/query runner
- `goCode/run_mixed.sh`: example mixed-runner invocation

## Mixed runner notes

The Go mixed runner is not the main HPC entrypoint, but it is present in the repo and currently supports:

- dedicated insert and query worker counts
- `max` and `rate` modes per role
- deterministic row partitioning
- per-worker JSONL event logs
- fixed or bounded-random batch sizing
- optional dry-run backend

It is useful for reconstructing a mixed insert/query timeline outside the larger PBS workflow.

## Dependency check

Run manually:

```bash
./check_dependencies.sh
./check_dependencies.sh --missing-only
```

## Expected outputs

Per run directory you will typically see:

- `workflow.out`, `workflow.log`, and component logs
- timing/summary CSV or NPY files
- insert/query outputs under task-specific directories
- tracing outputs when `TRACING=True`
- worker status files under `workerOut/` or related runtime directories

## Required local artifacts (site-specific)

Examples of required non-committed artifacts include:

- Milvus/etcd/MinIO Apptainer images
- built Go clients
- Python environments referenced by `ENV_PATH`
- dataset `.npy` files and optional restore directories
