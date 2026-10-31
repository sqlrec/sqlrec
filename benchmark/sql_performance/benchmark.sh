#!/bin/bash
set +x
set -eo pipefail
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"

source "${dir}/../../deploy/env.sh"

CONCURRENCY=10
DURATION=30
WARMUP_DURATION=10
URL="http://${NODE_IP}:${SQLREC_REST_PORT}/api/v1/main_rec"

auth_args=()
if [ "${AUTH_ENABLED}" = true ]; then
    auth_args=(-H "Authorization: Basic $(printf '%s' "${AUTH_USERNAME}:${AUTH_PASSWORD}" | base64 | tr -d '\r\n')")
fi

echo "Warming up the system..."
# Warm up the system
wrk -t1 -c1 -d${WARMUP_DURATION}s -s "${dir}/request.lua" "${auth_args[@]}" "${URL}"

printf '\nWarm-up completed, starting formal benchmark...\n'
# Formal benchmark
echo "Benchmark configuration: Concurrency=${CONCURRENCY}, Duration=${DURATION}s"
wrk --latency -t${CONCURRENCY} -c${CONCURRENCY} -d${DURATION}s -s "${dir}/request.lua" "${auth_args[@]}" "${URL}"
