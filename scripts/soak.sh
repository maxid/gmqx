#!/bin/sh
# Run the gmqx soak test.
#
#   ./scripts/soak.sh
#   GMQX_SOAK_DURATION=30s ./scripts/soak.sh
#   GMQX_SOAK_DURATION=10m GMQX_SOAK_TIMEOUT=15m ./scripts/soak.sh
#
# -timeout is separate from the soak duration and must be larger than it: go test
# aborts the binary (and dumps every goroutine) after its own timeout, which is
# 10m by default — exactly the length of a "10 minute soak".
set -eu

cd "$(dirname "$0")/.."

: "${GMQX_SOAK_DURATION:=10s}"
: "${GMQX_SOAK_TIMEOUT:=15m}"
export GMQX_SOAK=1
export GMQX_SOAK_DURATION

exec go test -timeout "$GMQX_SOAK_TIMEOUT" -run '^TestSoak$' -v -count=1 ./server
