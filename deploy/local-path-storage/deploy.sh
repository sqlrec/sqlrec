#!/bin/bash
shopt -s expand_aliases
source ~/.bash_profile

# change pv data path in local-path-storage.yaml first
# refer to https://github.com/rancher/local-path-provisioner
dir=$(dirname $(realpath $0))
kubectl apply -f "${dir}/local-path-storage.yaml"

kubectl patch storageclass standard -p '{"metadata": {"annotations":{"storageclass.kubernetes.io/is-default-class":"false"}}}'
kubectl patch storageclass local-path -p '{"metadata": {"annotations":{"storageclass.kubernetes.io/is-default-class":"true"}}}'
