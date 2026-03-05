# Qdrant Rust code

## `multiClientUpload`

Async Rust uploader used by the Qdrant workflow.

### Behavior

- Spawns `N_WORKERS * UPLOAD_CLIENTS_PER_WORKER` async upload clients.
- Splits `.npy` input rows evenly across workers and then evenly across clients per worker.
- Supports endpoint targeting strategies via `UPLOAD_BALANCE_STRATEGY`:
  - `NO_BALANCE`: all clients target worker 0
  - `WORKER_BALANCE`: each client targets its owning worker
- Uses `UPLOAD_BATCH_SIZE` for upsert batch size.
- Writes timing outputs consumed by workflow summary scripts.

### Environment variables consumed

- `N_WORKERS`
- `CORPUS_SIZE`
- `UPLOAD_CLIENTS_PER_WORKER`
- `DATA_FILEPATH`
- `UPLOAD_BATCH_SIZE`
- `UPLOAD_BALANCE_STRATEGY`

### Build

From `qdrant/rustCode`:

```bash
./compile.sh multiClientUpload
```

Expected runtime binary path for submit workflow:

- `qdrant/rustCode/multiClientUpload/multiClientUpload`
