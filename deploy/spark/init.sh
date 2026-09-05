set -ex

if [ ! -f ${CLIENT_DIR}/${SPARK_CLIENT_ARCH_NAME} ]; then
  download_file "${SPARK_CLIENT_URL}" "${CLIENT_DIR}/${SPARK_CLIENT_ARCH_NAME}"
fi

if [ ! -e ${CLIENT_DIR}/${SPARK_CLIENT_DIR_NAME} ]; then
  tar -xzf ${CLIENT_DIR}/${SPARK_CLIENT_ARCH_NAME} -C ${CLIENT_DIR}
fi
