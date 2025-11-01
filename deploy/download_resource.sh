#!/bin/bash
set -ex
dir=$(dirname $(realpath $0))
source ${dir}/env.sh

bash ${dir}/kafka/init.sh
bash ${dir}/hms/init.sh
bash ${dir}/flink/init.sh
bash ${dir}/juicefs/init.sh
bash ${dir}/milvus/init.sh
bash ${dir}/kyuubi/init.sh