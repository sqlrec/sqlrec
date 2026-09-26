#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
venv_dir="${repo_root}/.venv"

if [[ ! -x "${venv_dir}/bin/python" ]]; then
    bootstrap_python=""
    if [[ -n "${PYTHON_BOOTSTRAP:-}" ]]; then
        candidates=("${PYTHON_BOOTSTRAP}")
    else
        candidates=(python3 python)
    fi
    for candidate in "${candidates[@]}"; do
        if command -v "${candidate}" >/dev/null 2>&1 \
            && "${candidate}" -c 'import sys; sys.exit(sys.version_info < (3, 10))' 2>/dev/null; then
            bootstrap_python="${candidate}"
            break
        fi
    done
    if [[ -z "${bootstrap_python}" ]]; then
        echo "Python 3.10 or newer is required to create ${venv_dir}" >&2
        exit 1
    fi
    "${bootstrap_python}" -m venv "${venv_dir}"
fi

if ! "${venv_dir}/bin/python" -c 'import sys; sys.exit(sys.version_info < (3, 10))'; then
    echo "${venv_dir} needs Python 3.10 or newer; remove it and rerun this script" >&2
    exit 1
fi

"${venv_dir}/bin/python" -m pip install -r "${repo_root}/requirements.txt"
