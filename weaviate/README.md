# Weaviate

This directory contains the Weaviate engine implementation used by the unified
submit interface. It plugs into the shared `pbs_submit_manager.sh` framework
the same way `qdrant/` does: an `engine.sh` contract, a `schema.sh` variable
registry, runtime launch scripts under `weaviateSetup/`, Go client source
under `clients/`, and ready-to-edit run templates under `sampleConfigs/`.

## Main entrypoint

From the repo root:

```bash
./pbs_submit_manager.sh --help --engine weaviate
./pbs_submit_manager.sh --engine weaviate --config weaviate_run.env
./pbs_submit_manager.sh --generate-only --engine weaviate --config weaviate_run.env
```

Any variable in `schema.sh` accepts a space-separated sweep list, which
expands into a Cartesian product of run directories (same behavior as the
other engines).

## Directory structure

- `engine.sh`: Weaviate engine wiring for the unified submit manager
- `schema.sh`: Weaviate variable registry and defaults
- `main.sh`: PBS launch script copied into each run directory for the
  `insert`, `index`, `query_bs`, and `query_core` tasks
- `main_query_scaling.sh`: PBS launch script for `TASK=query_scaling`; drives
  the multi-phase insert → index quiescence → query pipeline on top of the
  running Weaviate cluster
- `weaviateSetup/launchWeaviateNode.sh`: per-rank Weaviate launcher invoked
  by both main scripts via `mpirun`; pulls the container directly from
  `docker://semitechnologies/weaviate:1.36.2`
- `weaviateSetup/mapping.py`: per-rank helper that discovers the `hsn0`
  interface and writes `interfaces<rank>.json` for the launcher
- `clients/go_client/`: single Go module containing every Weaviate client
  binary (one `*.go` file per binary, each with its own `package main`)
- `clients/go_client/build.sh`: per-file `go build` driver
- `clients/build_all.sh`: convenience wrapper that delegates to the per-file
  builder
- `sampleConfigs/`: ready-to-edit `.env` templates for the unified submit
  manager

## Important run artifacts

Generated Weaviate runs typically contain:

- `submit.sh` — qsub header + dispatch to the selected main script
- `run_config.env` — canonical resolved run config (the values every other
  file reads from)
- `launchWeaviateNode.sh`, `mapping.py` — per-rank launcher and IP mapper
- `main.sh` **or** `main_query_scaling.sh` — whichever the TASK dispatches to
- `go_client/` — staged Weaviate Go client binaries for `query_scaling` runs
  (contains `$INSERT_BIN` and `$QUERY_SCALING_BIN`, both `chmod +x`-ed)
- `perf/` — per-rank readiness flags (`weaviate_running<rank>.txt`) and any
  `perf`-based profiling output when `USEPERF=true`
- `ip_registry.txt` — comma-separated `rank,node,ip,http,gossip,data,raft,raft_internal`
  written by each launched rank; used by client-side scripts to discover the
  cluster
- `worker_nodefile.txt`, `all_nodefile.txt` — node lists derived from
  `$PBS_NODEFILE`
- `nodes_verbose.json`, `nodes_verbose_pre_query.json` — snapshots of
  `/v1/nodes?output=verbose` used during the indexing-quiescence gate
- `insert_summary.json`, `index_time.json`, `query_client_*/query_summary.json`,
  `query_scaling_summary.json` — per-phase and aggregate metrics for
  `query_scaling` runs
- `output.log` — combined stdout/stderr tee of the main script

`run_config.env` is the canonical resolved run config for the generated run
directory; `engine_emit_runtime_env()` in `engine.sh` is what writes it.

## Common Weaviate variables

Required for runs:

- `TASK`
- `PLATFORM`
- `WALLTIME`
- `QUEUE`
- `ACCOUNT`

Common runtime knobs:

- `RUN_MODE`: `PBS` (the only mode currently supported)
- `BASE_DIR`: optional base directory; auto-filled to the current directory
  by `engine_apply_overrides()` when empty
- `NODES`: compute-node count to allocate for Weaviate workers (the client
  runs on an additional, non-worker node, so `TOTAL_NODES = NODES + 1`)
- `WORKERS_PER_NODE`: Weaviate worker ranks launched per compute node
- `CORES`: CPU cores per worker rank; `112` falls through to `--cpu-bind
  none` in `main.sh`, any other value uses `--cpu-bind depth -d $CORES`
- `STORAGE_MEDIUM`: `memory`, `DAOS`, `lustre`, or `SSD` — selects the
  per-rank data directory inside `launchWeaviateNode.sh`
- `USEPERF`: toggles the optional `perf/` profiling path

Engine/dataset knobs:

- `VECTOR_DIM`
- `DISTANCE_METRIC`: `COSINE`, `DOT`, or `L2`; mapped to Weaviate's
  `cosine`/`dot`/`l2-squared` by `create_class_with_consensus`
- `GPU_INDEX`: forwarded to the Go client (defaults to `false`)
- `DATASET_LABEL`: dataset tag used in run-directory names
- `CLASS_NAME`: Weaviate class/collection name

Insert / index workload:

- `DATA_FILEPATH`
- `CORPUS_SIZE`
- `UPLOAD_BATCH_SIZE`
- `UPLOAD_CLIENTS_PER_WORKER`
- `UPLOAD_BALANCE_STRATEGY`: `NO_BALANCE` or `WORKER_BALANCE`

Query workload:

- `QUERY_FILEPATH`
- `QUERY_WORKLOAD`
- `QUERY_BATCH_SIZE`
- `QUERY_TOPK`
- `QUERY_EF`
- `QUERY_CLIENTS_PER_WORKER`
- `QUERY_CLIENT_MODE`: `per_worker` (each worker gets
  `QUERY_CLIENTS_PER_WORKER` clients) or `fixed` (exactly
  `QUERY_CLIENTS_PER_WORKER` clients total, distributed round-robin across
  workers)

Per-task client binaries:

- `WEAVIATE_CLIENT_BINARY`: client binary copied into the run directory for
  the `insert` / `index` / `query_bs` / `query_core` tasks (older,
  single-binary workflows)
- `INSERT_BIN`: binary used for the insert phase of `query_scaling`
  (default: `insert_pes2o_streaming`)
- `QUERY_SCALING_BIN`: binary used for the query phase of `query_scaling`
  (default: `query_scaling`)

Use:

```bash
./pbs_submit_manager.sh --help --engine weaviate
```

to see the full variable list, defaults, choices, and requirement rules.

## Tasks

- `insert`, `index`, `query_bs`, `query_core` — original Seth-scaffolded
  workflows. `engine_copy_payload` stages the binary named by
  `WEAVIATE_CLIENT_BINARY` and dispatches `main.sh`, which brings up the
  cluster and execs the binary against `WEAVIATE_HOST`.
- `query_scaling` — multi-phase pipeline used for the SC paper. Dispatches
  `main_query_scaling.sh`, which:
  1. Brings up the cluster via `launchWeaviateNode.sh` (same as the other
     tasks).
  2. Creates the collection and waits for schema consensus across all
     workers (`create_class_with_consensus`, `wait_for_schema_consensus`).
  3. Runs the insert phase: `go_client/$INSERT_BIN` with
     `NODES * WORKERS_PER_NODE * UPLOAD_CLIENTS_PER_WORKER` total clients,
     terminated when observed object count crosses
     `CORPUS_SIZE * (1 - INSERT_TOLERANCE)`.
  4. Waits for index quiescence — `vectorQueueLength == 0` and every
     shard's `vectorIndexingStatus == READY` for a configurable number of
     consecutive polls (`wait_for_object_count`, `wait_for_index_quiescence`,
     `assert_all_shards_ready`).
  5. Runs the query phase: fans out `go_client/$QUERY_SCALING_BIN` per
     client per worker with partitioned query ranges, collects per-client
     `query_summary.json`, and aggregates into `query_scaling_summary.json`.

Run-directory names are built by `engine_make_run_dir_name()` in
`engine.sh`. For `query_scaling` the layout is:

```
query_scaling_${DATASET_LABEL}_${STORAGE_MEDIUM}_N<nodes>_NP<workers_per_node>\
  _TW<total_workers>_UCPW<upload_clients>_QCPW<query_clients>_CORES<cores>\
  _IBS<upload_batch>_QBS<query_batch>_<timestamp>
```

## PBS setup

Weaviate runs do not ship a local SIF. `launchWeaviateNode.sh` pulls the
container directly from upstream on first use:

```
docker://semitechnologies/weaviate:1.36.2
```

The first time a rank launches, Apptainer caches the image in the user's
Apptainer cache; subsequent runs on the same host reuse it. There is no
`weaviate/utils/download_sif.sh` helper — the pull is inline in the
launcher. If you want to pin a different upstream tag, edit the `apptainer
run` invocation in `weaviateSetup/launchWeaviateNode.sh`.

Platform activation (modules + Python venv) happens inside the main script:

- `main.sh` handles `POLARIS` and `AURORA`.
- `main_query_scaling.sh` currently supports `PLATFORM=AURORA` only and
  fails fast for anything else.

Both scripts source the same Python venv
(`/lus/flare/projects/radix-io/sockerman/qdrant/qEnv/bin/activate` on
Aurora). That environment provides the Python helpers
`main_query_scaling.sh` calls for object-count and quiescence polling.

## Building the Go clients

The Go clients live in a single module at `clients/go_client/`. Every
`*.go` file declares its own `main`, so each binary is built from one
source file.

```bash
cd weaviate/clients
./build_all.sh                              # builds the defaults
./go_client/build.sh insert_pes2o_streaming # build one
./go_client/build.sh insert_pes2o_streaming query_scaling
```

Each build emits the binary in place (e.g.
`clients/go_client/query_scaling`). The compiled artifacts are git-ignored
(see `weaviate/.gitignore`), so you rebuild once per host; the submit
framework then stages the selected binaries into the run directory under
`go_client/`.

For `query_scaling`, `engine_copy_payload()` requires that both
`clients/go_client/$INSERT_BIN` and `clients/go_client/$QUERY_SCALING_BIN`
already exist on disk — if they do not, it prints the exact `build.sh`
invocation needed and refuses to stage the run directory.

## Example configs

Sample configs live under `weaviate/sampleConfigs/`. For example:

```bash
./pbs_submit_manager.sh --generate-only --engine weaviate \
    --config weaviate/sampleConfigs/aurora_pes2o_query_scaling.env
```

Remove `--generate-only` when the config is ready to submit. Shipped
templates:

- `aurora_pes2o_query_scaling.env` — pes2o embeddings, `CLASS_NAME=PES2OEF64`,
  `VECTOR_DIM=2560`, `DISTANCE_METRIC=COSINE`.
- `aurora_yandex_query_scaling.env` — Yandex text2image embeddings,
  `CLASS_NAME=YandexT2I`, `VECTOR_DIM=200`, `DISTANCE_METRIC=DOT`.

Both default to `ACCOUNT=radix-io`, `WALLTIME=01:00:00`,
`QUEUE=debug-scaling`, and `NODES=1`, so they can be used as-is against a
small allocation after only updating dataset paths and allocation settings.

## Notes on current behavior

- `NODES` counts Weaviate worker nodes only; the submit framework adds one
  more node for the client driver and sets `TOTAL_NODES = NODES + 1` in
  `engine_load_combo()`.
- `main.sh` waits only for `./perf/weaviate_running${MAX_RANK}.txt` before
  execing the client binary; `main_query_scaling.sh` waits for every rank's
  readiness flag, plus an extra 60 s Raft stabilization pause, before doing
  anything.
- `main_query_scaling.sh` tolerates a small insert shortfall
  (`INSERT_TOLERANCE=1e-5` by default). If the Go client dies but the
  observed object count is within tolerance of `CORPUS_SIZE`, the run
  continues; otherwise it exits non-zero and emits the `insert_end_1` event.
- All runtime knobs that are truly implementation details of the
  `query_scaling` pipeline — `RPC_TIMEOUT`, `DRAIN_*`, `INSERT_MAX_RETRIES`,
  `SCHEMA_CONSENSUS_TIMEOUT`, `DAOS_POOL`, `DAOS_CONT`, etc. — live as env
  defaults inside `main_query_scaling.sh`, not in the schema. Override them
  with `--set NAME=value` at submit time if you need to.
- Compiled Go binaries and timestamped run directories are git-ignored; see
  `weaviate/.gitignore` for the full pattern list.
