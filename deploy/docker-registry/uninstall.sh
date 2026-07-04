#!/bin/bash
set -ex
dir=$(dirname $(realpath $0))
source ${dir}/../env.sh

docker rm -f registry 2>/dev/null || true
