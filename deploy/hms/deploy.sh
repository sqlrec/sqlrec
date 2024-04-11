#!/bin/bash
shopt -s expand_aliases
source ~/.bash_profile

namespace=$1

helm uninstall mysql-hms --namespace "$namespace"
kubectl delete pvc data-mysql-hms-0 --namespace "$namespace"
helm install mysql-hms \
  --namespace "$namespace" \
  --set primary.service.type=NodePort \
  --set auth.database=metastore,auth.username=metastore,auth.password=abc123456 \
  --wait \
  --timeout 3600s \
  oci://registry-1.docker.io/bitnamicharts/mysql

dir=$(dirname $(realpath $0))
if [ ! -f "${dir}/mysql-connector-java.jar" ];then
  wget -P "${dir}" "https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar"
  ln -s "${dir}/mysql-connector-java-8.0.27.jar" "${dir}/mysql-connector-java.jar"
fi

mysql_dir="${dir}/mysql-connector-java.jar"
juicefs_dir=$(dirname ${dir})"/juicefs/juicefs-hadoop.jar"
sed "s|MYSQL_JAR_LOCATION|${mysql_dir}|" "${dir}/hms.yaml" | \
  sed "s|JUICEFS_JAR_LOCATION|${juicefs_dir}|" > "${dir}/hms.yaml.tmp"

sed "s|MYSQL_JAR_LOCATION|${mysql_dir}|" "${dir}/hms-init.yaml" | \
  sed "s|JUICEFS_JAR_LOCATION|${juicefs_dir}|" > "${dir}/hms-init.yaml.tmp"

mysql_dns="mysql-hms.${namespace}.svc.cluster.local"
hms_dns="hms.${namespace}.svc.cluster.local"
sed "s/MYSQL_DNS_PLACEHOLDER/${mysql_dns}/" "${dir}/hive-site-hms.template" | \
 sed "s/HMS_DNS_PLACEHOLDER/${hms_dns}/" > "${dir}/hive-site-hms.xml"
kubectl delete configmap hive-site-hms -n "${namespace}"
kubectl create configmap hive-site-hms --from-file="${dir}/hive-site-hms.xml" -n "${namespace}"

sed "s/HMS_DNS_PLACEHOLDER/${hms_dns}/" "${dir}/hive-site.template" > "${dir}/hive-site.xml"
kubectl delete configmap hive-site -n "${namespace}"
kubectl create configmap hive-site --from-file="${dir}/hive-site.xml" -n "${namespace}"

kubectl apply -f "${dir}/hms-init.yaml.tmp" -n "${namespace}"
sleep 15

kubectl apply -f "${dir}/hms.yaml.tmp" -n "${namespace}"