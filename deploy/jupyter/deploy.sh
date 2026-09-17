#!/bin/bash
set -exo pipefail
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/../env.sh"

render_config "${dir}/config.yaml"

helm upgrade --install jupyterhub jupyterhub/jupyterhub \
  --namespace ${NAMESPACE} \
  --version=${JUPYTERHUB_VERSION} \
  --values "${dir}/config.yaml.tmp"
