#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))
source ${dir}/../env.sh

# refer to https://docs.opensearch.org.cn/docs/latest/install-and-configure/install-opensearch/helm/
# refer to https://docs.opensearch.org.cn/docs/latest/install-and-configure/install-dashboards/helm/

# Deploy OpenSearch
envsubst < ${dir}/opensearch.yaml > ${dir}/opensearch.yaml.rendered
helm upgrade --install opensearch opensearch/opensearch \
  --namespace ${NAMESPACE} \
  --set image.tag=${OPENSEARCH_VERSION} \
  --set service.nodePort=${OPENSEARCH_HTTP_PORT} \
  -f ${dir}/opensearch.yaml.rendered

# Wait for OpenSearch to be ready
kubectl wait --for=condition=Ready pod -l app.kubernetes.io/instance=opensearch --timeout=600s -n ${NAMESPACE}

# Deploy OpenSearch Dashboards
envsubst < ${dir}/opensearch-dashboards.yaml > ${dir}/opensearch-dashboards.yaml.rendered
helm upgrade --install opensearch-dashboards opensearch/opensearch-dashboards \
  --namespace ${NAMESPACE} \
  -f ${dir}/opensearch-dashboards.yaml.rendered

# Wait for OpenSearch Dashboards to be ready
kubectl wait --for=condition=Ready pod -l app.kubernetes.io/instance=opensearch-dashboards --timeout=600s -n ${NAMESPACE}
