#!/bin/bash
set -eo pipefail

PROJECT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd -P)"
for arg in "$@"; do
    case "${arg}" in
        -h|--help)
            echo 'Usage: bash benchmark/growthbook/test.sh [--output DIRECTORY]'
            echo 'Deploy GrowthBook, provision experiments through its APIs, and verify the SQLRec UDF.'
            exit 0
            ;;
    esac
done
source "${PROJECT_DIR}/deploy/env.sh"
cd "${PROJECT_DIR}"

bash "${PROJECT_DIR}/deploy/growthbook/deploy.sh"
if [ ! -x "${PROJECT_DIR}/.venv/bin/python" ]; then
    bash "${PROJECT_DIR}/bin/setup_python_env.sh"
fi
"${PROJECT_DIR}/.venv/bin/python" -m pip install -r "${PROJECT_DIR}/benchmark/growthbook/requirements.txt"
exec "${PROJECT_DIR}/.venv/bin/python" -m benchmark.growthbook "$@"
