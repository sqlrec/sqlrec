#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))
source ${dir}/../env.sh

bash ${dir}/deploy.sh mongodb ${MONGODB_PORT} ${MONGODB_USER} ${MONGODB_PASSWORD}
