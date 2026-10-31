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
Run Maven tests, then run SQL performance and model quality benchmarks.
Remaining arguments are passed to Maven unchanged.
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
    printf '\n[test] %s\n' "$1"
    shift
    "$@"
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
    run_stage "Download deployment resources" bash "${PROJECT_DIR}/deploy/download_resource.sh"
    run_stage "Deploy SQLRec and dependencies" bash "${PROJECT_DIR}/deploy/deploy_components.sh"
    # These connectors are covered by integration tests but are optional in the
    # standard component deployment.
    run_stage "Deploy MongoDB" bash "${PROJECT_DIR}/deploy/mongodb/deploy_default.sh"
    run_stage "Deploy Redis Cluster" bash "${PROJECT_DIR}/deploy/redis/deploy-cluster.sh"
    run_stage "Wait for deployed workloads" wait_for_workloads
    run_stage "Verify SQLRec connection" wait_for_sqlrec
)

run_benchmarks() (
    source "${PROJECT_DIR}/deploy/env.sh"
    run_stage "Prepare SQL performance benchmark" bash "${PROJECT_DIR}/benchmark/sql_performance/init.sh"
    run_stage "Run SQL performance benchmark" bash "${PROJECT_DIR}/benchmark/sql_performance/benchmark.sh"
    run_stage "Install model quality dependencies" "${PROJECT_DIR}/.venv/bin/python" -m pip install \
        -r "${PROJECT_DIR}/benchmark/model_quality/requirements.txt"
    run_stage "Prepare all model quality datasets" "${PROJECT_DIR}/.venv/bin/python" \
        -m benchmark.model_quality.prepare --dataset all --upload
    run_stage "Run all model quality benchmarks" "${PROJECT_DIR}/.venv/bin/python" \
        -m benchmark.model_quality --model all --dataset all --full-eval
)

cd "${PROJECT_DIR}"
if [ "${RUN_ALL}" = true ]; then
    run_stage "Cache existing Minikube images" bash "${PROJECT_DIR}/deploy/cache_images.sh" save
    # Delete only after image caching succeeds. The rebuilt cluster reloads the
    # host-side cache through deploy_minikube.sh.
    run_stage "Delete existing Minikube environment" minikube -p minikube delete
    run_stage "Prepare Minikube" bash "${PROJECT_DIR}/deploy/deploy_minikube.sh"
    # Discover addresses after cluster startup, including on a first deployment.
    NODE_IP="$(minikube -p minikube ip)"
    if [ -z "${NODE_IP}" ]; then
        echo "ERROR: Minikube did not report a node IP." >&2
        exit 1
    fi
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
    (run_stage "Run Maven integration tests" run_maven_tests)
    run_benchmarks
    printf '\n[test] Deployment, integration tests, and benchmarks completed.\n'
else
    run_maven_tests
fi
