set -ex

if [ ! -f ${CLIENT_DIR}/${HADOOP_CLIENT_ARCH_NAME} ]; then
  download_file "${HADOOP_CLIENT_URL}" "${CLIENT_DIR}/${HADOOP_CLIENT_ARCH_NAME}"
fi

if [ ! -e ${CLIENT_DIR}/${HADOOP_CLIENT_DIR_NAME} ]; then
  tar -xzf ${CLIENT_DIR}/${HADOOP_CLIENT_ARCH_NAME} -C ${CLIENT_DIR}
fi

if [ ! -f ${CLIENT_DIR}/${JAVA_CLIENT_ARCH_NAME} ]; then
  download_file "${JAVA_CLIENT_URL}" "${CLIENT_DIR}/${JAVA_CLIENT_ARCH_NAME}"
fi

if [ ! -e ${CLIENT_DIR}/${JAVA_CLIENT_DIR_NAME} ]; then
  tar -xzf ${CLIENT_DIR}/${JAVA_CLIENT_ARCH_NAME} -C ${CLIENT_DIR}
fi

if [ "${CONTAINER_JAVA_ARCH_NAME}" != "${JAVA_CLIENT_ARCH_NAME}" ]; then
  if [ ! -f "${CLIENT_DIR}/${CONTAINER_JAVA_ARCH_NAME}" ]; then
    download_file "${CONTAINER_JAVA_URL}" "${CLIENT_DIR}/${CONTAINER_JAVA_ARCH_NAME}"
  fi
  if [ ! -e "${CLIENT_DIR}/${CONTAINER_JAVA_DIR_NAME}" ]; then
    tar -xzf "${CLIENT_DIR}/${CONTAINER_JAVA_ARCH_NAME}" -C "${CLIENT_DIR}"
  fi
fi
