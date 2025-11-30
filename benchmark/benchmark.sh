#!/bin/bash
shopt -s expand_aliases
source ~/.bash_profile
set -ex
dir=$(dirname $(realpath $0))

export BASE_DIR=$(dirname ${dir})/deploy
source ${dir}/../deploy/env.sh

CONCURRENCY=10
DURATION=60
WARMUP_DURATION=10
URL="http://127.0.0.1:${SQLREC_REST_PORT}/api/v1/main_rec"

echo "Warming up the system..."
# Warm up the system
wrk -t1 -c1 -d${WARMUP_DURATION}s -s request.lua ${URL}

echo "\nWarm-up completed, starting formal benchmark..."
# Formal benchmark
echo "Benchmark configuration: Concurrency=${CONCURRENCY}, Duration=${DURATION}s"
wrk -t${CONCURRENCY} -c${CONCURRENCY} -d${DURATION}s -s request.lua ${URL}