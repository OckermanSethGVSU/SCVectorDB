# Qdrant Rust code

This directory contains Rust clients used by Qdrant HPC workflows.

## Projects

- `multiClientUpload`: async uploader used by `qdrant/main.sh`.
- `multiClientQuery`: async query client (available for experiments; not currently called by default workflow scripts).

## `multiClientUpload`

### Behavior

- Spawns `N_WORKERS * UPLOAD_CLIENTS_PER_WORKER` async upload clients.
- Splits `.npy` rows across workers, then across clients per worker.
- Supports endpoint targeting via `UPLOAD_BALANCE_STRATEGY`:
  - `NO_BALANCE`: all clients target worker 0
  - `WORKER_BALANCE`: each client targets its owning worker
- Uses `UPLOAD_BATCH_SIZE` for upsert size.
- Writes timing CSV output consumed by summary scripts.

### Environment variables consumed

- `N_WORKERS`
- `CORPUS_SIZE`
- `UPLOAD_CLIENTS_PER_WORKER`
- `DATA_FILEPATH`
- `UPLOAD_BATCH_SIZE`
- `UPLOAD_BALANCE_STRATEGY`

## `multiClientQuery`

### Behavior

- Spawns `N_WORKERS * QUERY_CLIENTS_PER_WORKER` async query clients.
- Splits query vectors from `QUERY_FILEPATH` evenly across clients.
- Routes queries according to `QUERY_BALANCE_STRATEGY` (`NO_BALANCE` / `WORKER_BALANCE`).
- Supports optional result logging with `QUERY_DEBUG_RESULTS`.

### Environment variables consumed

- `N_WORKERS`
- `QUERY_SET_SIZE`
- `QUERY_CLIENTS_PER_WORKER`
- `QUERY_FILEPATH`
- `QUERY_BATCH_SIZE`
- `QUERY_BALANCE_STRATEGY`
- `QUERY_DEBUG_RESULTS` (optional)

## Build

From `qdrant/rustCode`:

```bash
./compile.sh multiClientUpload
./compile.sh multiClientQuery
```

Expected binary paths after build:

- `qdrant/rustCode/multiClientUpload/multiClientUpload`
- `qdrant/rustCode/multiClientQuery/multiClientQuery`
