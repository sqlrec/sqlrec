#!/bin/bash
shopt -s expand_aliases
source ~/.bash_profile

namespace="${1:-sqlrec}"
kubectl create namespace "${namespace}"

bash ./minio/deploy.sh "${namespace}"
bash ./juicefs/deploy.sh "${namespace}"
bash ./kafka/deploy.sh "${namespace}"
bash ./redis/deploy.sh "${namespace}"
bash ./hms/deploy.sh "${namespace}"
bash ./kyuubi/deploy.sh "${namespace}"
bash ./flink/deploy.sh "${namespace}"

