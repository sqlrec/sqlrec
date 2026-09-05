#!/bin/bash
# shared shell functions for deploy scripts; sourced by env.sh

prepend_path() {
    case ":${PATH}:" in
        *":$1:"*) ;;
        *) PATH="$1:${PATH}" ;;
    esac
    export PATH
}

# Download a file without depending on wget, which is not installed by default
# on macOS. Existing files are left untouched by the callers.
download_file() {
    local url=$1
    local destination=$2
    curl --fail --location --retry 3 --output "${destination}" "${url}"
}

require_commands() {
    local missing=""
    local command_name
    for command_name in "$@"; do
        if ! command -v "${command_name}" >/dev/null 2>&1; then
            missing="${missing} ${command_name}"
        fi
    done
    if [ -n "${missing}" ]; then
        echo "ERROR: missing required commands:${missing}" >&2
        return 1
    fi
}

version_at_least() {
    local actual=${1#v}
    local required=${2#v}
    local actual_major actual_minor actual_patch
    local required_major required_minor required_patch
    IFS=. read -r actual_major actual_minor actual_patch <<EOF
${actual}
EOF
    IFS=. read -r required_major required_minor required_patch <<EOF
${required}
EOF
    actual_patch=${actual_patch%%[^0-9]*}
    required_patch=${required_patch%%[^0-9]*}
    actual_patch=${actual_patch:-0}
    required_patch=${required_patch:-0}
    [ "${actual_major:-0}" -gt "${required_major:-0}" ] ||
        { [ "${actual_major:-0}" -eq "${required_major:-0}" ] &&
          { [ "${actual_minor:-0}" -gt "${required_minor:-0}" ] ||
            { [ "${actual_minor:-0}" -eq "${required_minor:-0}" ] &&
              [ "${actual_patch}" -ge "${required_patch}" ]; }; }; }
}

# wait for a k8s job to complete; fail fast (with logs) if the job fails
# usage: wait_for_job <job-name> [namespace] [timeout-seconds]
wait_for_job() {
    local job=$1
    local ns=${2:-${NAMESPACE}}
    local timeout=${3:-${DEPLOY_TIMEOUT:-3600}}
    local elapsed=0
    while ! kubectl get job "${job}" -n "${ns}" -o jsonpath='{.status.conditions[?(@.type=="Complete")].status}' 2>/dev/null | grep -q True; do
        if kubectl get job "${job}" -n "${ns}" -o jsonpath='{.status.conditions[?(@.type=="Failed")].status}' 2>/dev/null | grep -q True; then
            echo "ERROR: job ${job} failed, recent logs:" >&2
            kubectl logs "job/${job}" -n "${ns}" --tail=100 >&2 || true
            return 1
        fi
        if [ "${elapsed}" -ge "${timeout}" ]; then
            echo "ERROR: timed out waiting for job ${job} to complete" >&2
            return 1
        fi
        sleep 5
        elapsed=$((elapsed + 5))
    done
}

# Bash can export functions to child Bash processes. In zsh, `export -f` prints
# the function definitions and does not provide the Bash-compatible export that
# the deployment scripts need.
if [ -n "${BASH_VERSION:-}" ]; then
    export -f prepend_path download_file require_commands version_at_least wait_for_job
fi
