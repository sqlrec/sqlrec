#!/bin/bash
shopt -s expand_aliases
source ~/.bash_profile
set -ex
dir=$(dirname $(realpath $0))
source ${dir}/env.sh

if ! kubectl get namespace "${NAMESPACE}" >/dev/null 2>&1; then
  kubectl create namespace "${NAMESPACE}"
fi

#bash ${dir}/minio/init.sh
#bash ${dir}/juicefs/init.sh
bash ${dir}/hadoop/init.sh
bash ${dir}/kafka/init.sh
bash ${dir}/hms/init.sh
bash ${dir}/redis/init.sh
bash ${dir}/flink/init.sh
bash ${dir}/milvus/init.sh
bash ${dir}/kyuubi/init.sh
bash ${dir}/postgresql/init.sh
bash ${dir}/jupyter/init.sh

echo 'download resource done'