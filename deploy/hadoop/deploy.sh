#!/bin/bash
set -exo pipefail
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/../env.sh"

require_commands kubectl envsubst
mkdir -p "${CONF_DIR}"

# Prepare the local client before accessing JuiceFS, including standalone runs.
cp "${LIB_DIR}/${JUICEFS_HADOOP_JAR_NAME}" "${HADOOP_HOME}/share/hadoop/common/lib/"

for config in core-site hdfs-site; do
  render_config "${dir}/${config}.xml"
  cp "${dir}/${config}.xml.tmp" "${CONF_DIR}/${config}.xml"
  cp "${CONF_DIR}/${config}.xml" "${HADOOP_HOME}/etc/hadoop/"
  kubectl create configmap "${config}" --from-file="${config}.xml=${CONF_DIR}/${config}.xml" \
    -n "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -
done

HADOOP_CONF_DIR="${HADOOP_HOME}/etc/hadoop" "${HADOOP_HOME}/bin/hadoop" fs -mkdir -p /spark/upload
