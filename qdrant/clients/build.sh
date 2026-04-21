#!/bin/bash


PROJ_DIR=${1:?Usage: $0 <rust_project_dir>}


HOST=$(hostname)

if [[ "$HOST" == *"aurora"* ]]; then
    echo "Detected Aurora"

    module load apptainer
    module load frameworks

    export CC=gcc
    export CXX=g++
    export AR=ar

elif [[ "$HOST" == *"polaris"* ]]; then
    echo "Detected Polaris"
    module use /soft/modulefiles; module load conda; conda activate base
else
    echo "Unknown system: $HOST. Likely Polaris"
    module use /soft/modulefiles; module load conda; conda activate base
fi



# Resolve to an absolute path (nice for safety)
PROJ_DIR="$(cd "$PROJ_DIR" && pwd)"

BIN_NAME="$(basename "$PROJ_DIR")"

echo "Building Rust project in: $PROJ_DIR"

# Build in the project directory
pushd "$PROJ_DIR" >/dev/null
if [[ "$HOST" == *"polaris-login"* ]]; then
    # Polaris has really low login node parallelism limits
    export CARGO_BUILD_JOBS=1
    cargo build --release -j 1
else
  cargo build --release
fi
popd >/dev/null

# Copy the binary back to where you ran this script
SRC_BIN="$PROJ_DIR/target/release/$BIN_NAME"
DST_BIN="./$BIN_NAME"

if [[ ! -x "$SRC_BIN" ]]; then
  echo "Error: expected executable not found (or not executable): $SRC_BIN" >&2
  echo "Hint: ensure Cargo.toml [package] name or [[bin]] name produces '$BIN_NAME'" >&2
  exit 1
fi

cp -f "$SRC_BIN" "$DST_BIN"
chmod +x "$DST_BIN"

echo "Copied: $SRC_BIN -> $DST_BIN"
