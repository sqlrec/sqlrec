#!/bin/bash
set -exo pipefail

dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/../env.sh"

helm repo add zilliztech https://zilliztech.github.io/milvus-helm/ --force-update
