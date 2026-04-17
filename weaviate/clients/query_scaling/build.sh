#!/bin/bash
# Build the query_scaling Weaviate client.
# Produces ./query_scaling in this directory.

set -euo pipefail

cd "$(dirname "$0")"
go build -o query_scaling ./...
