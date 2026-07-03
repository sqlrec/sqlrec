#!/bin/bash
set -ex
shopt -s expand_aliases
source ~/.bash_profile
dir=$(dirname $(realpath $0))
source ${dir}/../env.sh

DEFAULT_JAVA_TOOL_OPTIONS="-XX:+UseCompactObjectHeaders -XX:+UseStringDeduplication"
export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS:-${DEFAULT_JAVA_TOOL_OPTIONS}}"
if [ "${DEBUG_MODE}" = "true" ]; then
    export JAVA_TOOL_OPTIONS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:${SQLREC_DEBUG_PORT}"
fi

export DEBUG_TRACE="${DEBUG_TRACE:-false}"
export TRACE_ENDPOINT="${TRACE_ENDPOINT:-http://${NODE_IP}:${JAEGER_OTLP_GRPC_PORT}}"
export TRACE_SERVICE_NAME="${TRACE_SERVICE_NAME:-sqlrec}"

envsubst < ${dir}/demo.yaml > ${dir}/demo.yaml.tmp
kubectl apply -f ${dir}/demo.yaml.tmp -n "${NAMESPACE}"
