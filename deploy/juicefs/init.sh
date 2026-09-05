set -ex

if [ ! -f ${CLIENT_DIR}/${JUICEFS_ARCH_NAME} ]; then
  download_file "${JUICEFS_URL}" "${CLIENT_DIR}/${JUICEFS_ARCH_NAME}"
fi

if [ ! -f ${CLIENT_DIR}/juicefs ]; then
  tar -xzf ${CLIENT_DIR}/${JUICEFS_ARCH_NAME} -C ${CLIENT_DIR}
fi

if command -v juicefs >/dev/null 2>&1; then
  echo 'juicefs has installed'
elif [ "${DEPLOY_OS}" = darwin ]; then
  echo "using downloaded JuiceFS CLI from ${CLIENT_DIR}/juicefs"
else
  sudo install ${CLIENT_DIR}/juicefs /usr/local/bin
fi

if [ ! -f ${LIB_DIR}/${JUICEFS_HADOOP_JAR_NAME} ]; then
  download_file "${JUICEFS_HADOOP_JAR_URL}" "${LIB_DIR}/${JUICEFS_HADOOP_JAR_NAME}"
fi