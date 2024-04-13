#!/bin/bash
shopt -s expand_aliases
source ~/.bash_profile

namespace="${1:-sqlrec}"

# refer to https://github.com/minio/minio/blob/master/helm/minio/README.md
helm repo add minio https://charts.min.io/
helm install minio \
 --namespace "$namespace" \
 --set resources.requests.memory=2Gi \
 --set replicas=1 \
 --set mode=standalone \
 --set rootUser=rootuser,rootPassword=rootpass123 \
 --set service.type=NodePort \
 --set service.nodePort=32000 \
 --set consoleService.type=NodePort \
 --set consoleService.nodePort=32001 \
 --set buckets[0].name=bucket1,buckets[0].policy=none,buckets[0].purge=false \
 --wait \
 --timeout 3600s \
 minio/minio
