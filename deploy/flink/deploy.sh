#!/bin/bash
shopt -s expand_aliases
source ~/.bash_profile

namespace="${1:-sqlrec}"

helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-1.8.0/
helm install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator --set webhook.create=false --namespace "$namespace"

kubectl create serviceaccount flink -n "${namespace}"
kubectl create clusterrolebinding flink-role --clusterrole=edit --serviceaccount="${namespace}":flink --namespace="${namespace}"

dir=$(dirname $(realpath $0))
if [ ! -f "${dir}/flink-shaded-hadoop-2-uber.jar" ];then
  wget -P "${dir}" "https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar"
  ln -s "${dir}/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar" "${dir}/flink-shaded-hadoop-2-uber.jar"
fi

# refer to https://issues.apache.org/jira/browse/FLINK-27450
if [ ! -f "${dir}/flink-sql-connector-hive.jar" ];then
  wget -P "${dir}" "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-hive-2.3.9_2.12/1.19.0/flink-sql-connector-hive-2.3.9_2.12-1.19.0.jar"
  ln -s "${dir}/flink-sql-connector-hive-2.3.9_2.12-1.19.0.jar" "${dir}/flink-sql-connector-hive.jar"
fi

hive_jar_dir="${dir}/flink-sql-connector-hive.jar"
hadoop_jar_dir="${dir}/flink-shaded-hadoop-2-uber.jar"
juicefs_dir=$(dirname ${dir})"/juicefs/juicefs-hadoop.jar"
sed "s|JUICEFS_JAR_LOCATION|${juicefs_dir}|" "${dir}/sql_gateway.yaml" | \
    sed "s|HIVE_JAR_LOCATION|${hive_jar_dir}|" | \
    sed "s|HADOOP_JAR_LOCATION|${hadoop_jar_dir}|" > "${dir}/sql_gateway.yaml.tmp"

kubectl apply -f "${dir}/sql_gateway.yaml.tmp" -n "${namespace}"