#!/bin/bash

set -e

if [ -z "$1" ]; then
    echo "Usage: $0 <go_project_directory>"
    exit 1
fi

DIR="$1"

if [ ! -d "$DIR" ]; then
    echo "Error: Directory '$DIR' does not exist"
    exit 1
fi

cd "$DIR"

if [ ! -f "main.go" ]; then
    echo "Error: main.go not found in $DIR"
    exit 1
fi

# Binary name = directory name
BIN_NAME=$(basename "$DIR")

echo "Building Go project in: $DIR"
echo "Output binary: $DIR/$BIN_NAME"

# Optional: ensure dependencies are downloaded once
go mod download

# Build binary in the same directory
go build -o "$BIN_NAME" main.go

echo "Build complete."
