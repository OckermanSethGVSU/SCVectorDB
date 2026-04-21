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
- `main.sh`: PBS launch script copied into every run directory. Boots the
  cluster, waits for all ranks to register, creates the collection with
  schema consensus, then dispatches on `TASK` — `QUERY_SCALING` runs the
  multi-phase insert → index quiescence → query pipeline; `INSERT`,
  `INDEX`, `QUERY_BS`, and `QUERY_CORE` exec `go_client/$WEAVIATE_CLIENT_BINARY`
- `weaviateSetup/launchWeaviateNode.sh`: per-rank launcher invoked by
  `main.sh` via `mpirun`; pulls the container directly from
  `docker://semitechnologies/weaviate:1.36.2`
- `weaviateSetup/mapping.py`: per-rank helper that discovers the `hsn0`
  interface and writes `interfaces<rank>.json` for the launcher
- `clients/go_client/`: single Go module with four client binaries —
  `insert_streaming`, `index`, `query`, `mixed_insert_query` (one `*.go`
  file per binary, each `package main`)
- `clients/go_client/build.sh`: per-file `go build` driver
- `clients/build_all.sh`: convenience wrapper that delegates to the per-file
  builder
- `sampleConfigs/`: ready-to-edit `.env` templates for the unified submit
  manager

## Important run artifacts

Generated Weaviate runs typically contain:

- `submit.sh` — qsub header + dispatch to `main.sh`
- `run_config.env` — canonical resolved run config (the values every other
  file reads from)
- `launchWeaviateNode.sh`, `mapping.py` — per-rank launcher and IP mapper
- `main.sh` — unified launch script (every TASK dispatches through it)
- `go_client/` — staged client binaries for this run (`QUERY_SCALING` stages
  `$INSERT_BIN` and `$QUERY_SCALING_BIN`; every other TASK stages
  `$WEAVIATE_CLIENT_BINARY`), all `chmod +x`-ed
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
  `QUERY_SCALING` runs
- `output.log` — combined stdout/stderr tee of the main script

`run_config.env` is the canonical resolved run config for the generated run
directory; `engine_emit_runtime_env()` in `engine.sh` is what writes it.

## Common Weaviate variables

Required for runs:

- `TASK`: one of `INSERT`, `INDEX`, `QUERY_BS`, `QUERY_CORE`, `QUERY_SCALING`
  (case-sensitive; lowercase values are rejected at validation time)
- `PLATFORM`
- `WALLTIME`
- `QUEUE`
- `ACCOUNT`
- `INSERT_DATA_FILEPATH`: required for `INSERT`, `INDEX`, and `QUERY_SCALING`
  (the last runs its own insert phase)

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
- `DISTANCE_METRIC`: `IP`, `COSINE`, or `L2`; `IP` is translated to
  Weaviate's `dot`, and the others map to `cosine`/`l2-squared` in
  `create_class_with_consensus`
- `GPU_INDEX`: forwarded to the Go client (defaults to `false`)
- `DATASET_LABEL`: dataset tag used in run-directory names
- `CLASS_NAME`: Weaviate class/collection name

Insert / index workload:

- `INSERT_DATA_FILEPATH`
- `INSERT_CORPUS_SIZE` (Weaviate default: `10000000`)
- `INSERT_BATCH_SIZE` (Weaviate default: `2048`)
- `INSERT_CLIENTS_PER_WORKER`
- `INSERT_BALANCE_STRATEGY`: `NO_BALANCE` or `WORKER_BALANCE`

Query workload:

- `QUERY_DATA_FILEPATH`
- `QUERY_CORPUS_SIZE`
- `QUERY_BATCH_SIZE`
- `QUERY_TOPK`
- `QUERY_EF`
- `QUERY_CLIENTS_PER_WORKER`
- `QUERY_CLIENT_MODE`: `per_worker` (each worker gets
  `QUERY_CLIENTS_PER_WORKER` clients) or `fixed` (exactly
  `QUERY_CLIENTS_PER_WORKER` clients total, distributed round-robin across
  workers)

Per-task client binaries:

- `WEAVIATE_CLIENT_BINARY`: client binary staged under `go_client/` for the
  `INSERT` / `INDEX` / `QUERY_BS` / `QUERY_CORE` tasks (default:
  `insert_streaming`)
- `INSERT_BIN`: binary used for the insert phase of `QUERY_SCALING`
  (default: `insert_streaming`)
- `QUERY_SCALING_BIN`: binary used for the query phase of `QUERY_SCALING`
  (default: `query`)

Use:

```bash
./pbs_submit_manager.sh --help --engine weaviate
```

to see the full variable list, defaults, choices, and requirement rules.

## Tasks

All tasks flow through the same `main.sh`, which brings up the cluster,
waits for every rank's readiness flag, creates the collection with schema
consensus, and then dispatches:

- `INSERT`, `INDEX`, `QUERY_BS`, `QUERY_CORE` — exec
  `go_client/$WEAVIATE_CLIENT_BINARY` with the proxy env scrubbed; the
  binary reads `WEAVIATE_HOST`, `CLASS_NAME`, `INSERT_DATA_FILEPATH`, etc. from
  env to decide its behavior.
- `QUERY_SCALING` — multi-phase pipeline:
  1. Creates the collection and waits for schema consensus across all
     workers (`create_class_with_consensus`, `wait_for_schema_consensus`).
  2. Runs the insert phase: `go_client/$INSERT_BIN` with
     `NODES * WORKERS_PER_NODE * INSERT_CLIENTS_PER_WORKER` total clients,
     terminated when observed object count crosses
     `INSERT_CORPUS_SIZE * (1 - INSERT_TOLERANCE)`.
  3. Waits for index quiescence — `vectorQueueLength == 0` and every
     shard's `vectorIndexingStatus == READY` for a configurable number of
     consecutive polls (`wait_for_object_count`, `wait_for_index_quiescence`,
     `assert_all_shards_ready`).
  4. Runs the query phase: fans out `go_client/$QUERY_SCALING_BIN` per
     client per worker with partitioned query ranges, collects per-client
     `query_summary.json`, and aggregates into `query_scaling_summary.json`.

Run-directory names are built by `engine_make_run_dir_name()` in
`engine.sh`. For `QUERY_SCALING` the layout is:

```
QUERY_SCALING_${DATASET_LABEL}_${STORAGE_MEDIUM}_N<nodes>_NP<workers_per_node>\
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

There is no local-mode harness in `weaviate/` today; the unified engine only
stages PBS runs.

Platform activation (modules + Python venv) happens inside `main.sh`.
`PLATFORM=AURORA` is currently supported; the script fails fast for any
other platform. It sources the Python venv at
`/lus/flare/projects/radix-io/sockerman/qdrant/qEnv/bin/activate`, which
provides the Python helpers the script invokes for object-count and
indexing-quiescence polling.

## Building the Go clients

The Go clients live in a single module at `clients/go_client/`. Every
`*.go` file declares its own `main`, so each binary is built from one
source file.

```bash
cd weaviate/clients
./build_all.sh                       # build every binary
./go_client/build.sh index           # build one
./go_client/build.sh insert_streaming query
```

Each build emits the binary in place (e.g. `clients/go_client/query`). The
compiled artifacts are git-ignored
(see `weaviate/.gitignore`), so you rebuild once per host; the submit
framework then stages the selected binaries into the run directory under
`go_client/`.

For `QUERY_SCALING`, `engine_copy_payload()` requires that both
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
  `CLASS_NAME=YandexT2I`, `VECTOR_DIM=200`, `DISTANCE_METRIC=IP`.

Both default to `ACCOUNT=radix-io`, `WALLTIME=01:00:00`,
`QUEUE=debug-scaling`, and `NODES=1`, so they can be used as-is against a
small allocation after only updating dataset paths and allocation settings.

## Notes on current behavior

- `NODES` counts Weaviate worker nodes only; the submit framework adds one
  more node for the client driver and sets `TOTAL_NODES = NODES + 1` in
  `engine_load_combo()`.
- `main.sh` waits for every rank's readiness flag under `./perf/`, then
  adds a 60 s Raft stabilization pause before creating the class.
- `QUERY_SCALING` tolerates a small insert shortfall (`INSERT_TOLERANCE=1e-5`
  by default). If the Go client dies but the observed object count is
  within tolerance of `INSERT_CORPUS_SIZE`, the run continues; otherwise it exits
  non-zero and emits the `insert_end_1` event.
- Runtime knobs that are implementation details of the `QUERY_SCALING`
  pipeline — `RPC_TIMEOUT`, `DRAIN_*`, `INSERT_MAX_RETRIES`,
  `SCHEMA_CONSENSUS_TIMEOUT`, `DAOS_POOL`, `DAOS_CONT`, etc. — live as env
  defaults inside `main.sh`, not in the schema. Override them with
  `--set NAME=value` at submit time if you need to.
- Compiled Go binaries and timestamped run directories are git-ignored; see
  `weaviate/.gitignore` for the full pattern list.
