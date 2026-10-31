#!/bin/sh
# Run the gmqx soak test.
#
#   ./scripts/soak.sh
#   GMQX_SOAK_DURATION=30s ./scripts/soak.sh
set -eu

cd "$(dirname "$0")/.."

: "${GMQX_SOAK_DURATION:=10s}"
export GMQX_SOAK=1
export GMQX_SOAK_DURATION

exec go test -run TestSoak -v -count=1 .
