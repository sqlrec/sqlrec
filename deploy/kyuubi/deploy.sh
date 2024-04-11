#!/bin/bash
shopt -s expand_aliases
source ~/.bash_profile

namespace=$1

dir=$(dirname $(realpath $0))
juicefs_dir=$(dirname ${dir})"/juicefs/juicefs-hadoop.jar"

kubectl create serviceaccount spark -n "${namespace}"
kubectl create clusterrolebinding spark-role --clusterrole=edit --serviceaccount="${namespace}":spark --namespace="${namespace}"

node_ip=`kubectl get node -o wide | awk 'NR==2{print $6}'`
sed "s|K8S-APISERVER-ADDR|${node_ip}:8443|" "${dir}/spark-defaults.conf.template" | \
  sed "s|NAMESPACE|${namespace}|" | \
  sed "s|JUICEFS_JAR_LOCATION|${juicefs_dir}|" > "${dir}/spark-defaults.conf"
kubectl delete configmap spark-defaults -n "${namespace}"
kubectl create configmap spark-defaults --from-file="${dir}/spark-defaults.conf" -n "${namespace}"

sed "s|JUICEFS_JAR_LOCATION|${juicefs_dir}|" "${dir}/kyuubi.yaml" > "${dir}/kyuubi.yaml.tmp"
kubectl apply -f "${dir}/kyuubi.yaml.tmp" -n "${namespace}"