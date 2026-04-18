# Weaviate Go clients

Single Go module under `go_client/`. Each `*.go` declares its own `package
main`, so one source file produces one binary.

## Binaries

- `insert_streaming` — bulk insert driver. Runs N concurrent clients against
  the worker ranks, retries transient failures, drains to quiescence, and
  writes `insert_summary.json`. Dataset-agnostic: class/vector dim/distance
  all come from env (`CLASS_NAME`, `VECTOR_DIM`, `DISTANCE_METRIC`).
- `index` — standalone indexing workload. Creates a dynamic-index class,
  streams vectors in, then measures time from the dynamic-index threshold
  crossing to queue drain (`vectorQueueLength == 0` for N stable polls).
  `ef` is a flag (default comes from `QUERY_EF`); distance from
  `DISTANCE_METRIC`.
- `query` — query driver. Reads vectors from an `.npy`, runs `QUERY_WORKLOAD`
  GraphQL nearVector batches, and writes `query_summary.json`. Used for
  `query_bs`, `query_core`, and the per-client fan-out inside the
  `query_scaling` task.
- `mixed_insert_query` — mixed insert/query workload for concurrent-write
  benchmarks.

## Building

```bash
./build_all.sh                   # build every binary
./go_client/build.sh index       # build one
./go_client/build.sh query index # build several
```

Binaries are written in place (e.g. `go_client/index`). They're git-ignored
— rebuild once per host. The engine reads them from there when staging a run
directory.
