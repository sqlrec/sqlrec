#!/bin/bash
set -exo pipefail
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/env.sh"

kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

bash "${dir}/postgresql/init.sh"
bash "${dir}/minio/init.sh"
bash "${dir}/juicefs/init.sh"
bash "${dir}/hadoop/init.sh"
bash "${dir}/spark/init.sh"
bash "${dir}/kyuubi/init.sh"
bash "${dir}/kafka/init.sh"
bash "${dir}/hms/init.sh"
bash "${dir}/redis/init.sh"
bash "${dir}/flink/init.sh"
bash "${dir}/milvus/init.sh"
bash "${dir}/jupyter/init.sh"
bash "${dir}/prometheus/init.sh"

echo 'download resource done'
