# Weaviate Go clients

A single Go module under `go_client/` containing every Weaviate client binary
the unified submit framework needs. The module mirrors
`/flare/radix-io/songoh/weaviate/multi_node/go_client/` on Aurora.

## Layout

- `go_client/` — shared Go module (`go.mod`, `go.sum`, `vendor/`) with one
  `*.go` file per binary. Each file declares `package main` with its own
  `main()`, so builds are per-file:
  `go build -o insert_pes2o_streaming insert_pes2o_streaming.go`.

## Building

```
./build_all.sh                              # builds the defaults in go_client/build.sh
./go_client/build.sh                        # same, invoked directly
./go_client/build.sh query_scaling          # build one
./go_client/build.sh insert_pes2o_streaming query_scaling index_pes2o_ef64
```

Each build emits the binary in place (e.g. `go_client/insert_pes2o_streaming`),
which is where `weaviate/engine.sh::engine_copy_payload` picks it up before
staging it into `go_client/` inside the run directory. Compiled binaries are
git-ignored (see `weaviate/.gitignore`).
