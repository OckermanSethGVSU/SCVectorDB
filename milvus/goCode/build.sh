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

# Ensure there is at least one Go file
if ! ls *.go >/dev/null 2>&1; then
    echo "Error: No .go files found in $DIR"
    exit 1
fi

# Binary name = directory name
BIN_NAME=$(basename "$DIR")

echo "Building Go project in: $DIR"
echo "Output binary: $DIR/$BIN_NAME"

# If go.mod does not exist, initialize module
if [ ! -f "go.mod" ]; then
    echo "No go.mod found. Initializing module..."
    go mod init "$BIN_NAME"
fi

# Download and clean dependencies
echo "Tidying modules..."
go mod tidy

# Build entire module (recommended over building main.go explicitly)
go build -o "$BIN_NAME"

echo "Build complete."