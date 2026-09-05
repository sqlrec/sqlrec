#!/bin/bash
set -ex
shopt -s expand_aliases
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source ${dir}/../env.sh

bash ${dir}/deploy.sh mongodb ${MONGODB_PORT} ${MONGODB_USER} ${MONGODB_PASSWORD}
