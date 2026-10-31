#!/bin/bash
set -eo pipefail

PROJECT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd -P)"
RUN_ALL=false
MAVEN_ARGS=()

usage() {
    cat <<'EOF'
Usage: bash bin/test.sh [-all|--all] [Maven arguments...]

Without -all, run Maven tests against an existing deployment.
With -all, cache images from the existing Minikube environment, delete it,
rebuild Minikube, download resources, and deploy all test dependencies.
Run Maven tests, then SQL performance, GrowthBook, Hugging Face and model quality benchmarks.
Remaining arguments are passed to Maven unchanged.
With -all, a complete report and stage logs are saved under benchmark/runs/.
Set TEST_REPORT_ROOT to change the parent directory. Failed runs also produce a report.
EOF
}

for arg in "$@"; do
    case "${arg}" in
        -all|--all) RUN_ALL=true ;;
        -h|--help) usage; exit 0 ;;
        *) MAVEN_ARGS+=("${arg}") ;;
    esac
done

run_stage() {
    local stage="$1" status
    printf '\n[test] %s\n' "$1"
    shift
    "${REPORT_PYTHON}" "${REPORT_SCRIPT}" start "${REPORT_DIR}" "${stage}"
    # Do not put the command/function in an `if`: that disables errexit inside
    # shell functions. Capture its exit status after the independent subshell.
    set +e
    (set -e; "$@") 2>&1 | tee "${REPORT_DIR}/logs/${stage}.log"
    local codes=("${PIPESTATUS[@]}")
    set -e
    status="${codes[0]}"
    if [ "${status}" -eq 0 ]; then status="${codes[1]}"; fi
    "${REPORT_PYTHON}" "${REPORT_SCRIPT}" finish "${REPORT_DIR}" "${stage}" "${status}"
    return "${status}"
}

finish_report() {
    local status="$1" report_status
    trap - EXIT INT TERM
    set +e
    "${REPORT_PYTHON}" "${REPORT_SCRIPT}" finalize "${REPORT_DIR}" "${status}"
    report_status=$?
    if [ "${status}" -eq 0 ]; then status="${report_status}"; fi
    printf '\n[test] Full report: %s/summary.md\n' "${REPORT_DIR}"
    exit "${status}"
}

discover_cluster_address() {
    local address
    address="$(minikube -p minikube ip)"
    if [ -z "${address}" ]; then
        echo "ERROR: Minikube did not report a node IP." >&2
        return 1
    fi
    printf '%s\n' "${address}" > "${REPORT_DIR}/node-ip.txt"
}

wait_for_workloads() {
    local namespace workloads workload
    # The Flink operator creates its JobManager Deployment asynchronously.
    kubectl wait --for=jsonpath='{.status.jobManagerDeploymentStatus}'=READY \
        flinkdeployment/flink-session-cluster -n "${NAMESPACE}" --timeout="${DEPLOY_TIMEOUT}s"
    for namespace in "${NAMESPACE}" "${NAMESPACE}-milvus"; do
        workloads="$(kubectl get deployments,statefulsets -n "${namespace}" -o name)"
        while IFS= read -r workload; do
            [ -n "${workload}" ] || continue
            kubectl rollout status "${workload}" -n "${namespace}" --timeout="${DEPLOY_TIMEOUT}s"
        done <<< "${workloads}"
    done
}

wait_for_sqlrec() {
    local deadline=$((SECONDS + DEPLOY_TIMEOUT))
    until bash "${PROJECT_DIR}/bin/beeline.sh" -e 'SELECT 1;'; do
        if [ "${SECONDS}" -ge "${deadline}" ]; then
            echo "ERROR: SQLRec did not become ready within ${DEPLOY_TIMEOUT}s." >&2
            return 1
        fi
        echo "SQLRec is not ready; retrying in 5s..." >&2
        sleep 5
    done
}

deploy_test_environment() (
    # env.sh selects Java 8 for Hive/Hadoop clients. Keep Maven's host JDK intact.
    source "${PROJECT_DIR}/deploy/env.sh"
    run_stage download-resources bash "${PROJECT_DIR}/deploy/download_resource.sh"
    run_stage deploy-components bash "${PROJECT_DIR}/deploy/deploy_components.sh"
    # These connectors are covered by integration tests but are optional in the
    # standard component deployment.
    run_stage deploy-mongodb bash "${PROJECT_DIR}/deploy/mongodb/deploy_default.sh"
    run_stage deploy-redis bash "${PROJECT_DIR}/deploy/redis/deploy-cluster.sh"
    run_stage deploy-growthbook bash "${PROJECT_DIR}/deploy/growthbook/deploy.sh"
    run_stage wait-workloads wait_for_workloads
    run_stage sqlrec-ready wait_for_sqlrec
)

run_benchmarks() (
    source "${PROJECT_DIR}/deploy/env.sh"
    run_stage sql-prepare bash "${PROJECT_DIR}/benchmark/sql_performance/init.sh"
    run_stage sql-performance bash "${PROJECT_DIR}/benchmark/sql_performance/benchmark.sh"
    run_stage python-dependencies "${PROJECT_DIR}/.venv/bin/python" -m pip install \
        -r "${PROJECT_DIR}/benchmark/model_quality/requirements.txt"
    run_stage growthbook "${PROJECT_DIR}/.venv/bin/python" \
        -m benchmark.growthbook --run-dir "${REPORT_DIR}/growthbook"
    run_stage huggingface "${PROJECT_DIR}/.venv/bin/python" \
        -m benchmark.huggingface --task all --run-dir "${REPORT_DIR}/huggingface"
    run_stage model-prepare "${PROJECT_DIR}/.venv/bin/python" \
        -m benchmark.model_quality.prepare --dataset all --upload
    run_stage model-quality "${PROJECT_DIR}/.venv/bin/python" \
        -m benchmark.model_quality --model all --dataset all --full-eval --run-dir "${REPORT_DIR}/model_quality"
)

cd "${PROJECT_DIR}"
if [ "${RUN_ALL}" = true ]; then
    # The report works before the project venv/deployment has been prepared.
    REPORT_PYTHON="$(command -v python3)"
    REPORT_SCRIPT="${PROJECT_DIR}/benchmark/test_report.py"
    REPORT_DIR="$("${REPORT_PYTHON}" "${REPORT_SCRIPT}" init "${PROJECT_DIR}" "${MAVEN_ARGS[@]}")"
    trap 'finish_report "$?"' EXIT
    trap 'exit 130' INT
    trap 'exit 143' TERM
    printf '[test] Report directory: %s\n' "${REPORT_DIR}"
    run_stage cache-images bash "${PROJECT_DIR}/deploy/cache_images.sh" save
    # Delete only after image caching succeeds. The rebuilt cluster reloads the
    # host-side cache through deploy_minikube.sh.
    run_stage delete-minikube minikube -p minikube delete
    run_stage prepare-minikube bash "${PROJECT_DIR}/deploy/deploy_minikube.sh"
    # Discover addresses after cluster startup, including on a first deployment.
    run_stage cluster-address discover_cluster_address
    NODE_IP="$(cat "${REPORT_DIR}/node-ip.txt")"
    export NODE_IP
    export STORAGE_MODE=hostpath
    deploy_test_environment
fi

DEFAULT_TEST_IP="${DEFAULT_TEST_IP:-${NODE_IP:-$(minikube -p minikube ip)}}"
if [ -z "${DEFAULT_TEST_IP}" ]; then
    echo "ERROR: set DEFAULT_TEST_IP or start Minikube before running integration tests." >&2
    exit 1
fi
# Export for SqlRecConfigs.DEFAULT_TEST_IP and demo SQL environment substitution.
export DEFAULT_TEST_IP
export META_DB_URL="${META_DB_URL-jdbc:postgresql://${DEFAULT_TEST_IP}:${SQLREC_POSTGRESQL_PORT:-30005}/sqlrec?sslmode=disable}"
export META_DB_USER="${META_DB_USER-${SQLREC_POSTGRESQL_USER:-sqlrec}}"
export META_DB_PASSWORD="${META_DB_PASSWORD-${SQLREC_POSTGRESQL_PASSWORD:-abc123456}}"
export HIVE_METASTORE_URI="${HIVE_METASTORE_URI-thrift://${DEFAULT_TEST_IP}:${HMS_PORT:-30008}}"
export FLINK_SQL_GATEWAY_ADDRESS="${FLINK_SQL_GATEWAY_ADDRESS-${DEFAULT_TEST_IP}}"
export TRACE_ENDPOINT="${TRACE_ENDPOINT-http://${DEFAULT_TEST_IP}:${JAEGER_OTLP_GRPC_PORT:-30038}}"
export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-${BASE_DIR:-${PROJECT_DIR}/deploy}/data/client/hadoop-${HADOOP_VERSION:-3.4.0}/etc/hadoop}"

run_maven_tests() {
    exec mvn test -Prun-integration-tests "-Dmaven.test.additionalClasspath=${HADOOP_CONF_DIR}" "${MAVEN_ARGS[@]}"
}

if [ "${RUN_ALL}" = true ]; then
    # Replace only the subshell so benchmarks can run after Maven succeeds.
    run_stage maven run_maven_tests
    run_benchmarks
    printf '\n[test] Deployment, integration tests, and benchmarks completed.\n'
else
    run_maven_tests
fi
