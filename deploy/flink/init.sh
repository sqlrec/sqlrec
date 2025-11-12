set -ex

if [ ! -f "${LIB_DIR}/${FLINK_HADOOP_JAR_NAME}" ];then
  wget -P "${LIB_DIR}" "${FLINK_HADOOP_JAR_URL}"
fi

if [ ! -f "${LIB_DIR}/${FLINK_SQL_CONNECTOR_HIVE_JAR_NAME}" ];then
  wget -P "${LIB_DIR}" "${FLINK_SQL_CONNECTOR_HIVE_JAR_URL}"
fi

helm repo add flink-operator-repo https://dlcdn.apache.org/flink/flink-kubernetes-operator-1.12.1/
helm upgrade --install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator --set webhook.create=false --namespace "${NAMESPACE}"
