#!/bin/bash
set -ex

# refer to https://docs.opensearch.org.cn/docs/latest/install-and-configure/install-opensearch/helm/
helm repo add opensearch https://opensearch-project.github.io/helm-charts/
helm repo update
