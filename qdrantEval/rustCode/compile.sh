#!/bin/bash


PROJ_DIR=${1:?Usage: $0 <rust_project_dir>}

module load apptainer
module load frameworks

export CC=gcc
export CXX=g++
export AR=ar

# Resolve to an absolute path (nice for safety)
PROJ_DIR="$(cd "$PROJ_DIR" && pwd)"

# Executable name == directory name
BIN_NAME="$(basename "$PROJ_DIR")"

echo "Building Rust project in: $PROJ_DIR"

# Build in the project directory
pushd "$PROJ_DIR" >/dev/null
cargo build --release
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
