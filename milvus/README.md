# Milvus workflow

This workflow launches Milvus on PBS systems (standalone or distributed), runs multi-client ingest, and can optionally trigger index conversion.

Main entrypoint:

- `pbs_submit_manager.sh`

## What is currently supported

- Task modes: `insert`, `index` (`TASK`)
- Deployment modes: `STANDALONE`, `DISTRIBUTED` (`MODE`)
- Platforms: `POLARIS`, `AURORA` (`PLATFORM`)
- Storage options (`STORAGE_MEDIUM`): `memory`, `DAOS`, `lustre`, `SSD`
- WAL configuration (`WAL`): `woodpecker`, `default`
- Upload balancing (`UPLOAD_BALANCE_STRATEGY`): `NONE`, `WORKER`
- Distributed controls:
  - `MINIO_MODE`: `single`, `stripped`
  - `MINIO_MEDIUM`: `DAOS`, `lustre`
  - `ETCD_MODE`: `single`, `replicated`
  - `STREAMING_NODES`, `STREAMING_NODES_PER_CN`
  - `NUM_PROXIES`, `NUM_PROXIES_PER_CN`
  - `DML_CHANNELS`

## Workflow files

- `pbs_submit_manager.sh`: configuration sweep + PBS generation + staging + `qsub`
- `main.sh`: runtime orchestration for standalone/distributed paths
- `check_dependencies.sh`: validates required files before submission
- `milvusSetup/*.sh`: component launch scripts (standalone, etcd, minio, role launches)
- `generalPython/*.py`: status/polling/profiling/collection setup/summary/index conversion
- `goCode/multiClientInsert/`: Go ingest client
- `cpuMilvus/configs/`: base Milvus config templates used at runtime

## Important submit variables (`pbs_submit_manager.sh`)

### Sweep variables

- `NODES=(...)`
- `CORES=(...)`
- `UPLOAD_BATCH_SIZE=(...)`
- `QUERY_BATCH_SIZE=(...)`

### Runtime variables

- `TASK`
- `STORAGE_MEDIUM`
- `usePerf` (exported as `USEPERF`)
- `CORPUS_SIZE`
- `UPLOAD_CLIENTS_PER_PROXY`
- `UPLOAD_BALANCE_STRATEGY`
- `BASE_DIR`
- `WAL`
- `DATA_FILEPATH`
- `VECTOR_DIM`
- `ENV_PATH`
- `PLATFORM`
- `MODE`
- Distributed-only variables listed above

### Scheduler variables

- `WALLTIME`
- `queue`

## Runtime flow (`main.sh`)

1. Sets environment, loads platform modules, and activates Python env.
2. Optionally mounts DAOS (`launch-dfuse.sh`) for Milvus and/or MinIO storage.
3. Launches Milvus:
   - `STANDALONE`: one server process + profiling.
   - `DISTRIBUTED`: etcd/minio + coordinator/streaming/query/data/proxy components.
4. Waits for service readiness and configures collection (`setup_collection.py`).
5. Runs Go ingest client (`./multiClientInsert`).
6. Produces summary/timing outputs.
7. If `TASK=index`, runs `convert_to_hnsw.py`.

## Dependency check

Run manually:

```bash
./check_dependencies.sh
```

The submit manager runs the missing-only check automatically:

```bash
./check_dependencies.sh --missing-only
```

`check_dependencies.sh` always checks for required images/scripts/binaries and has additional `perfDir/perf` warning behavior unless `USE_PERF=true`.

## Build notes

Build the Go uploader with:

```bash
cd goCode
./build.sh multiClientInsert
```

This produces `goCode/multiClientInsert/multiClientInsert`.

## Required local artifacts (not committed)

- `milvus.sif`
- `etcd_v3.5.18.sif`
- `minio.sif`
- Built Go client: `goCode/multiClientInsert/multiClientInsert`
- `perfDir/` (and optionally `perfDir/perf` for profiling)

## Typical run outputs

Per run directory:

- `workflow.out`, `output.log`
- `insert_times.txt`
- `insert_summary.txt`
- `uploadNPY/*.npy`
- `workerOut/*` logs and readiness markers
