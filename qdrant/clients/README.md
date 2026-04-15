# Qdrant Rust code

This directory contains the Rust client projects used by the Qdrant workflows in this repo.

## Projects

- `batch_client`: current combined insert/query client used by `qdrant/main.sh`
- `upload`: older upload-only client kept for experiments
- `query`: older query-only client kept for experiments
- `mixed`: mixed insert/query runner with per-worker JSONL event logs
- `build.sh`: helper that builds a named Rust project and copies the produced binary into the current directory

## `batch_client`

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

## `mixed`

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

From `qdrant/clients`:

```bash
./build.sh batch_client
./build.sh mixed
```

Older projects can also be built:

```bash
./build.sh upload
./build.sh query
```

Expected binary paths after build:

- `qdrant/clients/batch_client/batch_client`
- `qdrant/clients/mixed/mixed`
- `qdrant/clients/upload/upload`
- `qdrant/clients/query/query`

## Local Workflow

The main local workflow is driven by `qdrant/local_main.sh`, which starts a disposable local Qdrant instance and runs either the `batch_client` or the mixed client depending on `TASK`.

Example:

```bash
cd qdrant
./local_main.sh
STREAMING=true ./local_main.sh
```

Useful overrides:

- `RUST_BINARY=/path/to/batch_client`
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
