#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))
source ${dir}/../env.sh

envsubst < ${dir}/copy_jar_job.yaml > ${dir}/copy_jar_job.yaml.tmp
kubectl apply -f ${dir}/copy_jar_job.yaml.tmp -n "${NAMESPACE}"

kubectl wait --for=condition=complete job/sqlrec-copy-jar -n "${NAMESPACE}" --timeout=3600s

kubectl delete job sqlrec-copy-jar -n "${NAMESPACE}" 2>/dev/null || true
