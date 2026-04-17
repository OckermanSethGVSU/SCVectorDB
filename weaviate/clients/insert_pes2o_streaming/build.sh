#!/bin/bash
# Build the insert_pes2o_streaming Weaviate client.
# Produces ./insert_pes2o_streaming in this directory.

set -euo pipefail

cd "$(dirname "$0")"
go build -o insert_pes2o_streaming ./...
