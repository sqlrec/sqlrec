if [ ! -f "${LIB_DIR}/${FLINK_HADOOP_JAR_NAME}" ];then
  wget -P "${LIB_DIR}" "${FLINK_HADOOP_JAR_URL}"
fi

if [ ! -f "${LIB_DIR}/${FLINK_SQL_CONNECTOR_HIVE_JAR_NAME}" ];then
  wget -P "${LIB_DIR}" "${FLINK_SQL_CONNECTOR_HIVE_JAR_URL}"
fi
