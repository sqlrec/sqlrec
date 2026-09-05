#!/bin/bash
set -ex
shopt -s expand_aliases
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source ${dir}/../env.sh

# refer to https://docs.opensearch.org.cn/docs/latest/install-and-configure/install-opensearch/helm/
# refer to https://docs.opensearch.org.cn/docs/latest/install-and-configure/install-dashboards/helm/

# Deploy OpenSearch
envsubst < ${dir}/opensearch.yaml > ${dir}/opensearch.yaml.rendered
helm upgrade --install opensearch opensearch/opensearch \
  --namespace ${NAMESPACE} \
  --set image.tag=${OPENSEARCH_VERSION} \
  --set service.nodePort=${OPENSEARCH_HTTP_PORT} \
  -f ${dir}/opensearch.yaml.rendered \
  --wait \
  --timeout ${DEPLOY_TIMEOUT}s

# Deploy OpenSearch Dashboards
envsubst < ${dir}/opensearch-dashboards.yaml > ${dir}/opensearch-dashboards.yaml.rendered
helm upgrade --install opensearch-dashboards opensearch/opensearch-dashboards \
  --namespace ${NAMESPACE} \
  -f ${dir}/opensearch-dashboards.yaml.rendered \
  --wait \
  --timeout ${DEPLOY_TIMEOUT}s
