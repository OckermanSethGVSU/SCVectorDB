# Weaviate

This directory contains the tracked Weaviate engine used by the unified
`pbs_submit_manager.sh` workflow. The active pieces are:

- `engine.sh`: unified-engine contract implementation
- `schema.sh`: Weaviate-specific schema variables and defaults
- `main.sh`: PBS runtime copied into generated run directories
- `local_main.sh`: local runtime copied into generated run directories
- `weaviateSetup/`: node launch helpers
- `clients/`: Go client source and build helper
- `scripts/`: collection setup, status, and result-summary helpers
- `sampleConfigs/`: ready-to-edit `.env` files

## Main entrypoint

From the repo root:

```bash
./pbs_submit_manager.sh --help --engine weaviate
./pbs_submit_manager.sh --engine weaviate --config weaviate/sampleConfigs/local_dev.env
./pbs_submit_manager.sh --generate-only --engine weaviate --config weaviate/sampleConfigs/index_testing.env
```

The unified manager expands sweep values, creates one run directory per combo,
stages the Weaviate payload into that directory, writes `run_config.env`, and
submits automatically only when `RUN_MODE=PBS`.

## Current task model

The tracked Weaviate batch client supports:

- `INSERT`
- `INDEX`
- `QUERY`

The engine also knows how to stage the `mixed` binary for `TASK=MIXED`, but the
tracked `main.sh` and `local_main.sh` are not fully wired for a mixed workflow
yet. Treat `INSERT`, `INDEX`, and `QUERY` as the active supported tasks.

## Directory structure

- `engine.sh`: Weaviate engine wiring for the unified submit manager
- `schema.sh`: Weaviate-specific variable registry and defaults
- `main.sh`: PBS runtime launcher
- `local_main.sh`: local runtime launcher
- `weaviateSetup/launchWeaviateNode.sh`: per-rank cluster launcher
- `weaviateSetup/launchWeaviateNodeLocal.sh`: local container launcher
- `weaviateSetup/mapping.py`: PBS helper for interface discovery
- `clients/build.sh`: Go client build helper
- `clients/batch_client/`: active insert/index/query Go client
- `clients/mixed/`: mixed client source
- `scripts/create_basic_collection.py`: creates the collection used by the run
- `scripts/health_check.py`: waits for the cluster API to respond
- `scripts/status.py`: reads `ip_registry.txt` plus `run_config.env` and prints
  collection existence/config/count
- `scripts/multi_client_summary.py`: summarizes per-client `.npy` timing files
- `scripts/check_query_results.py`: quick inspection helper for query result
  `*.npy` files

## Generated run contents

Generated Weaviate run directories now typically contain:

- `submit.sh`
- `run_config.env`
- `main.sh` or `local_main.sh`
- `launchWeaviateNode.sh` or `launchWeaviateNodeLocal.sh`
- `mapping.py` for PBS runs
- `batch_client` or `mixed` staged directly in the run directory root
- `health_check.py`
- `create_basic_collection.py`
- `multi_client_summary.py`
- `runtime_state/`
- `ip_registry.txt`
- `uploadNPY/` and `queryNPY/` after client execution

The readiness flags used by the runtime scripts now live under
`runtime_state/weaviate_running<rank>.txt`. The older `perf/` readiness
directory is no longer part of the tracked runtime path.

## Active client behavior

`clients/batch_client/main.go` is the current insert/index/query worker.

Behavior worth knowing:

- it reads `N_WORKERS`
- it reads `<INSERT|QUERY>_CLIENTS_PER_WORKER`
- `INDEX` reuses insert client/env settings
- `RESULT_PATH` defaults to `.` inside the Go code when unset
- query search uses the Go client gRPC search path
- query sanity checks also use gRPC
- `INDEX` writes `index_time.txt`
- insert/query timing CSVs are written as `<task>_times.csv`

For `INDEX`, rank 0 waits for:

- nodes API queue/indexing quiescence
- and dynamic-index HNSW readiness from Weaviate debug shard stats

before releasing the shared searchable gate.

## Common Weaviate variables

In addition to the shared variables in `common/schema.sh`, the tracked
Weaviate-specific variables in `schema.sh` are:

- `WORKERS_PER_NODE`
- `USEPERF`
- `DEBUG`
- `GPU_INDEX`
- `ASYNC_INDEXING`
- `DISABLE_LAZY_LOAD_SHARDS`
- `HNSW_STARTUP_WAIT_FOR_VECTOR_CACHE`
- `SHARD_COUNT`
- `HNSW_M`
- `HNSW_EF_CONSTRUCTION`
- `HNSW_DYNAMIC_THRESHOLD`
- `INSERT_CLIENTS_PER_WORKER`
- `QUERY_TOPK`
- `HNSW_EF_SEARCH`
- `QUERY_CLIENTS_PER_WORKER`

Use:

```bash
./pbs_submit_manager.sh --help --engine weaviate
```

to see the resolved defaults, requirement rules, and current choices.

## Local and PBS runtime

Both runtime modes are tracked:

- `RUN_MODE=LOCAL` uses `local_main.sh` and `launchWeaviateNodeLocal.sh`
- `RUN_MODE=PBS` uses `main.sh` and `launchWeaviateNode.sh`

Local mode launches a single Weaviate container, writes `ip_registry.txt`, runs
the health check, creates the collection, and then runs `./batch_client`.

PBS mode launches one Weaviate rank per worker through `mpirun`, waits for
every rank to publish its readiness flag, runs the health check and collection
creation from the client node, and then runs `./batch_client`.

## Building the clients

The tracked build entrypoint is:

```bash
cd weaviate/clients
./build.sh batch_client
./build.sh mixed
```

This builds the binary in place under the corresponding client directory, for
example:

- `weaviate/clients/batch_client/batch_client`

Generated runs stage that binary directly into the run directory root.

## Sample configs

Tracked sample configs:

- `sampleConfigs/local_dev.env`
- `sampleConfigs/index_testing.env`
- `sampleConfigs/insertion_testing_pes2o.env`
- `sampleConfigs/query_core_testing.env`
- `sampleConfigs/query_multi_node_testing.env`
- `sampleConfigs/mixed_insert_query.env`