# Qdrant Rust code

This directory contains the Rust client projects used by the Qdrant workflows in this repo.

## Projects

- `multiClientOP`: current combined insert/query client used by `qdrant/main.sh`
- `multiClientUpload`: older upload-only client kept for experiments
- `multiClientQuery`: older query-only client kept for experiments
- `mixedrunner`: mixed insert/query runner with per-worker JSONL event logs
- `compile.sh`: helper that builds a named Rust project and copies the produced binary into the current directory
- `run_mixed.sh`: convenience wrapper for launching `mixedrunner` with the current environment

## `multiClientOP`

### Behavior

- Selects insert or query behavior via `ACTIVE_TASK=INSERT|QUERY`
- Spawns `N_WORKERS * *_CLIENTS_PER_WORKER` async clients
- Splits `.npy` rows evenly across logical clients
- Defaults to eager `.npy` loading, with optional `INSERT_STREAMING=true` or `QUERY_STREAMING=true` batch-by-batch direct reads
- Uses `ip_registry.txt` routing with:
  - `NO_BALANCE`
  - `WORKER_BALANCE`
- Insert mode writes timing files consumed by the existing Python summary scripts
- Query mode supports optional debug printing of returned results
- Query mode writes `query_result_ids.npy` with shape `(QUERY_CORPUS_SIZE, top_k)` where each row stores the returned point ids for that global query row

### Environment variables consumed

Insert mode:

- `ACTIVE_TASK=INSERT`
- `N_WORKERS`
- `INSERT_CLIENTS_PER_WORKER`
- `INSERT_CORPUS_SIZE`
- `INSERT_FILEPATH`
- `INSERT_BATCH_SIZE`
- `INSERT_BALANCE_STRATEGY`
- `INSERT_STREAMING` or `STREAMING` (optional; `true` enables direct batch reads instead of eager full-file load)

Query mode:

- `ACTIVE_TASK=QUERY`
- `N_WORKERS`
- `QUERY_CLIENTS_PER_WORKER`
- `QUERY_CORPUS_SIZE`
- `QUERY_FILEPATH`
- `QUERY_BATCH_SIZE`
- `QUERY_BALANCE_STRATEGY`
- `QUERY_DEBUG_RESULTS` (optional)
- `QUERY_EF_SEARCH` or `EF_SEARCH` (optional)
- `QUERY_TOP_K` or `TOP_K` (optional; defaults to `10`)
- `QUERY_STREAMING` or `STREAMING` (optional; `true` enables direct batch reads instead of eager full-file load)

## `multiClientUpload`

### Behavior

- Older upload-only client
- Spawns `N_WORKERS * UPLOAD_CLIENTS_PER_WORKER` async upload clients
- Uses `UPLOAD_BALANCE_STRATEGY` with `NO_BALANCE` / `WORKER_BALANCE`
- Writes timing `.csv` and `.npy` outputs

### Environment variables consumed

- `N_WORKERS`
- `CORPUS_SIZE`
- `UPLOAD_CLIENTS_PER_WORKER`
- `DATA_FILEPATH`
- `UPLOAD_BATCH_SIZE`
- `UPLOAD_BALANCE_STRATEGY`

## `multiClientQuery`

### Behavior

- Older query-only client
- Spawns `N_WORKERS * QUERY_CLIENTS_PER_WORKER` async query clients
- Uses `QUERY_BALANCE_STRATEGY` with `NO_BALANCE` / `WORKER_BALANCE`
- Supports optional result debugging via `QUERY_DEBUG_RESULTS`

### Environment variables consumed

- `N_WORKERS`
- `QUERY_SET_SIZE`
- `QUERY_CLIENTS_PER_WORKER`
- `TOTAL_QUERY_CLIENTS` (optional; overrides `QUERY_CLIENTS_PER_WORKER` by deriving `TOTAL_QUERY_CLIENTS / N_WORKERS`, which must divide evenly)
- `QUERY_FILEPATH`
- `QUERY_BATCH_SIZE`
- `QUERY_BALANCE_STRATEGY`
- `QUERY_DEBUG_RESULTS` (optional)

## `mixedrunner`

### Behavior

- Runs insert and query workers in one process
- Supports direct totals via `INSERT_CLIENTS` / `QUERY_CLIENTS`
- Can also derive totals from `N_WORKERS * INSERT_CLIENTS_PER_WORKER` and `N_WORKERS * QUERY_CLIENTS_PER_WORKER`
- Supports `max` mode and per-role `rate` mode via:
  - `MODE`
  - `INSERT_MODE`, `QUERY_MODE`
  - `INSERT_OPS_PER_SEC`, `QUERY_OPS_PER_SEC`
- Supports fixed or bounded-random batch sizes per role
- Reuses the repo's existing endpoint routing conventions:
  - explicit `QDRANT_URL`
  - otherwise `QDRANT_REGISTRY_PATH` / `ip_registry.txt` plus `INSERT_BALANCE_STRATEGY` and `QUERY_BALANCE_STRATEGY`
- Buffers per-worker event logs in memory and writes one JSONL file per worker at completion or failure
- Synchronizes all workers behind a shared startup barrier before issuing operations

### Environment variables consumed

- `RESULT_PATH`
- `COLLECTION_NAME` (optional, defaults to `singleShard`)
- `QDRANT_URL` (optional)
- `QDRANT_REGISTRY_PATH` (optional, defaults to `ip_registry.txt`)
- `N_WORKERS` (required only for `WORKER_BALANCE` or when deriving client totals from `*_CLIENTS_PER_WORKER`)
- `INSERT_CLIENTS` / `QUERY_CLIENTS` (optional direct totals)
- `INSERT_CLIENTS_PER_WORKER` / `QUERY_CLIENTS_PER_WORKER` (optional derived totals)
- `INSERT_FILEPATH`, `INSERT_CORPUS_SIZE`, `INSERT_BATCH_SIZE`
- `QUERY_FILEPATH`, `QUERY_CORPUS_SIZE`, `QUERY_BATCH_SIZE`
- `INSERT_BATCH_MIN`, `INSERT_BATCH_MAX` (optional)
- `QUERY_BATCH_MIN`, `QUERY_BATCH_MAX` (optional)
- `INSERT_BALANCE_STRATEGY`, `QUERY_BALANCE_STRATEGY`
- `MODE`, `INSERT_MODE`, `QUERY_MODE`
- `INSERT_OPS_PER_SEC`, `QUERY_OPS_PER_SEC`
- `TOP_K` (optional)
- `QUERY_EF_SEARCH` or `EF_SEARCH` (optional)
- `RPC_TIMEOUT` (optional, e.g. `30s`, `5m`)
- `INSERT_START_ID` (optional)

## Build

From `qdrant/rustCode`:

```bash
./compile.sh multiClientOP
./compile.sh mixedrunner
```

Older projects can also be built:

```bash
./compile.sh multiClientUpload
./compile.sh multiClientQuery
```

Expected binary paths after build:

- `qdrant/rustCode/multiClientOP/multiClientOP`
- `qdrant/rustCode/mixedrunner/mixedrunner`
- `qdrant/rustCode/multiClientUpload/multiClientUpload`
- `qdrant/rustCode/multiClientQuery/multiClientQuery`

## Local Harness

A small local integration harness lives under `qdrant/local_test_harness`:

- `gen_test_npy.py`: writes a deterministic float32 `.npy`
- `verify_qdrant_points.py`: checks that local Qdrant contains the expected ids and vectors
- `verify_query_results.py`: checks `query_result_ids.npy` against brute-force expected top-k ids
- `run_local_qdrant_test.sh`: launches a disposable Docker Qdrant, creates `singleShard`, runs `multiClientOP`, verifies inserted points, and optionally verifies query mode too

Example:

```bash
cd qdrant
./local_test_harness/run_local_qdrant_test.sh
STREAMING=true ./local_test_harness/run_local_qdrant_test.sh
```

Useful overrides:

- `RUST_BINARY=/path/to/multiClientOP`
- `VECTOR_DIM=8`
- `DISTANCE_METRIC=Dot`
- `TEST_ROWS=16`
- `INSERT_BATCH_SIZE=4`
- `QUERY_BATCH_SIZE=4`
- `QUERY_TOP_K=5`
- `RUN_QUERY_TEST=true`
- `RUN_MULTI_CLIENT_TEST=true`
- `MULTI_N_WORKERS=2`
- `MULTI_INSERT_CLIENTS_PER_WORKER=2`
- `MULTI_QUERY_CLIENTS_PER_WORKER=2`
- `STREAMING=true`
- `QDRANT_REGISTRY_PORT=6335` (the local harness writes `ip_registry.txt` using the client's existing `port - 1` convention)
