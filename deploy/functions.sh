#!/bin/bash
# shared shell functions for deploy scripts; sourced by env.sh

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
