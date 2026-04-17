# Weaviate engine (unified submit)

Engine plug-in for the shared `pbs_submit_manager.sh` framework. Mirrors the
layout that `qdrant/` uses: an `engine.sh` contract, a `schema.sh` variable
registry, runtime launch scripts under `weaviateSetup/`, Go client source
under `clients/`, and ready-to-edit run templates under `sampleConfigs/`.

## Layout

- `engine.sh` — engine contract (copy_payload, run-dir naming, main-script
  dispatch).
- `schema.sh` — experiment variables available via `--set NAME=value` or
  `--config`.
- `main.sh` — default launch script copied into each run dir.
- `main_query_scaling.sh` — launch script used when `TASK=query_scaling`
  (multi-phase insert → index quiescence → query).
- `weaviateSetup/launchWeaviateNode.sh` — per-rank Weaviate launcher invoked by
  both main scripts. Pulls the container directly from
  `docker://semitechnologies/weaviate:1.36.2`.
- `clients/` — Go client source, one subdirectory per binary.
- `sampleConfigs/` — `.env` templates for common experiments.

## Building the Go clients

```
cd clients
./build_all.sh            # builds every client/*/
# or build a single one:
cd insert_pes2o_streaming && ./build.sh
```

Each `build.sh` emits the binary in place. The compiled artifacts are
git-ignored.

## Running an experiment

Show the available variables:

```
./pbs_submit_manager.sh --help --engine weaviate
```

Generate a run directory without submitting (useful for inspection):

```
./pbs_submit_manager.sh --generate-only --engine weaviate \
    --config weaviate/sampleConfigs/aurora_pes2o_query_scaling.env
```

Once `ACCOUNT`, `WALLTIME`, `QUEUE`, and any dataset paths are correct, drop
`--generate-only` to submit via `qsub`.

Any variable in `schema.sh` accepts a space-separated sweep list, which
expands into a Cartesian product of run directories (same behavior as the
other engines).

## Tasks

- `insert`, `index`, `query_bs`, `query_core` — original Seth-scaffolded
  workflows; use `WEAVIATE_CLIENT_BINARY` to select the client.
- `query_scaling` — multi-phase pipeline that stages both
  `clients/insert_pes2o_streaming/$INSERT_BIN` and
  `clients/query_scaling/$QUERY_SCALING_BIN` under `go_client/` in the run
  directory and dispatches `main_query_scaling.sh` as the launch script.
