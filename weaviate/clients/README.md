# Weaviate Go clients

Each subdirectory is a self-contained Go module that produces one binary used
by the unified submit framework (via `weaviate/engine.sh::engine_copy_payload`).

## Layout

- `insert_pes2o_streaming/` — streaming insert client used by the `query_scaling`
  task's insert phase.
- `query_scaling/` — multi-phase query client used by the `query_scaling` task's
  query phase.

## Building

Build everything at once:

```
./build_all.sh
```

Or build a single client:

```
cd insert_pes2o_streaming && ./build.sh
```

Each `build.sh` emits the binary in place (e.g. `./insert_pes2o_streaming`),
which is where `engine_copy_payload` looks for it before staging into a run
directory under `go_client/`.

The compiled binaries are git-ignored (see `weaviate/.gitignore`).
