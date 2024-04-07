#!/bin/bash
shopt -s expand_aliases
source ~/.bash_profile

namespace=$1

helm install mysql-hms \
  --namespace "$namespace" \
  --set primary.service.type=NodePort \
  --set auth.database=metastore,auth.username=metastore,auth.password=abc123456 \
  --wait \
  --timeout 3600s \
  oci://registry-1.docker.io/bitnamicharts/mysql

dir=$(dirname $(realpath $0))
if [ ! -f "${dir}/mysql-connector-java-8.0.27.jar" ];then
  wget -P "${dir}" "https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar"
fi

JFS_LATEST_TAG=$(curl -s https://api.github.com/repos/juicedata/juicefs/releases/latest | grep 'tag_name' | cut -d '"' -f 4 | tr -d 'v')
juicefs_jar="juicefs-hadoop-${JFS_LATEST_TAG}.jar"
mysql_jar="mysql-connector-java-8.0.27.jar"
mysql_dir="${dir}/${mysql_jar}"
juicefs_dir=$(dirname ${dir})"/juicefs/${juicefs_jar}"
sed "s|MYSQL_JAR_LOCATION|${dir}|" "${dir}/hms.yaml" | \
  sed "s|JUICEFS_JAR_LOCATION|${juicefs_dir}|" | \
  sed "s|MYSQL_JAR_NAME|${mysql_jar}|" | \
  sed "s|JUICEFS_JAR_NAME|${juicefs_jar}|" > "${dir}/hms.yaml.tmp"

mysql_dns="mysql-hms.${namespace}.svc.cluster.local"
hms_dns="hms.${namespace}.svc.cluster.local"
sed "s/MYSQL_DNS_PLACEHOLDER/${mysql_dns}/" "${dir}/hive-site.template" | sed "s/HMS_DNS_PLACEHOLDER/${hms_dns}/" > "${dir}/hive-site.xml"
kubectl create configmap hive-site --from-file="${dir}/hive-site.xml" -n "${namespace}"

kubectl apply -f "${dir}/hms.yaml.tmp" -n "${namespace}"