#!/bin/bash
shopt -s expand_aliases
source ~/.bash_profile

namespace=$1

helm install mysql-juicefs \
  --namespace "$namespace" \
  --set primary.service.type=NodePort \
  --set primary.service.nodePorts.mysql=30306 \
  --set auth.database=juicefs,auth.username=juicefs,auth.password=abc123456 \
  --wait \
  --timeout 3600s \
  oci://registry-1.docker.io/bitnamicharts/mysql

if command -v juicefs >/dev/null 2>&1; then
  echo 'exists juicefs'
else
  # refer to https://juicefs.com/docs/zh/community/introduction/
  curl -sSL https://d.juicefs.com/install | sh -
fi

node_ip=`kubectl get node -o wide | awk 'NR==2{print $6}'`
juicefs format \
    --storage minio \
    --bucket http://${node_ip}:32000/bucket1 \
    --access-key rootuser \
    --secret-key rootpass123 \
    "mysql://juicefs:abc123456@(${node_ip}:30306)/juicefs" \
    myjfs

dir=$(dirname $(realpath $0))
JFS_LATEST_TAG=$(curl -s https://api.github.com/repos/juicedata/juicefs/releases/latest | grep 'tag_name' | cut -d '"' -f 4 | tr -d 'v')
if [ ! -f "${dir}/juicefs-hadoop-${JFS_LATEST_TAG}.jar" ];then
  wget -P "${dir}" "https://github.com/juicedata/juicefs/releases/download/v${JFS_LATEST_TAG}/juicefs-hadoop-${JFS_LATEST_TAG}.jar"
fi

if [ ! -e "${dir}/hadoop" ];then
  wget -P "${dir}" https://dlcdn.apache.org/hadoop/common/hadoop-3.4.0/hadoop-3.4.0.tar.gz
  tar -zxvf "${dir}/hadoop-3.4.0.tar.gz" -C "${dir}"
  ln -s "${dir}/hadoop-3.4.0" "${dir}/hadoop"
  cp "${dir}/juicefs-hadoop-${JFS_LATEST_TAG}.jar" "${dir}/hadoop/share/hadoop/common/lib/"
fi

sed "s/NODE_IP/${node_ip}/" "${dir}/core-site.template"  > "${dir}/core-site.xml"
cp "${dir}/core-site.xml" "${dir}/hadoop/etc/hadoop/"

kubectl create configmap core-site --from-file="${dir}/core-site.xml" -n "${namespace}"