#!/bin/sh
# Run the gmqx benchmark suite from the module root.
#
#   ./scripts/bench.sh
#   BENCHTIME=5s ./scripts/bench.sh
#   BENCH='BenchmarkPublishQoS1$' ./scripts/bench.sh
set -eu

cd "$(dirname "$0")/.."

: "${BENCH:=.}"
: "${BENCHTIME:=2s}"

exec go test -run '^$' -bench "$BENCH" -benchtime "$BENCHTIME" ./...
