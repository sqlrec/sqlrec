set -ex

if [ ! -f ${LIB_DIR}/${POSTGRESQL_CONNECTOR_JAR_NAME} ]; then
  wget -P ${LIB_DIR} ${POSTGRESQL_CONNECTOR_JAR_URL}
fi

if [ ! -f ${CLIENT_DIR}/${HIVE_CLIENT_ARCH_NAME} ]; then
  wget -P ${CLIENT_DIR} ${HIVE_CLIENT_URL}
fi

if [ ! -e ${CLIENT_DIR}/${HIVE_CLIENT_DIR_NAME} ]; then
  tar -xzf ${CLIENT_DIR}/${HIVE_CLIENT_ARCH_NAME} -C ${CLIENT_DIR}
fi
