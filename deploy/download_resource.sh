#!/bin/bash
set -ex
dir=$(dirname $(realpath $0))
source ${dir}/env.sh

bash ${dir}/hms/init.sh
bash ${dir}/flink/init.sh
bash ${dir}/juicefs/init.sh