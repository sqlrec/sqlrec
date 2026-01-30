set -ex

if [ ! -f ${CLIENT_DIR}/${JUICEFS_ARCH_NAME} ]; then
  wget -P ${CLIENT_DIR} ${JUICEFS_URL}
fi

if [ ! -f ${CLIENT_DIR}/juicefs ]; then
  tar -xzf ${CLIENT_DIR}/${JUICEFS_ARCH_NAME} -C ${CLIENT_DIR}
fi

if command -v juicefs >/dev/null 2>&1; then
  echo 'juicefs has installed'
else
  sudo install ${CLIENT_DIR}/juicefs /usr/local/bin
fi

if [ ! -f ${LIB_DIR}/${JUICEFS_HADOOP_JAR_NAME} ]; then
  wget -P ${LIB_DIR} ${JUICEFS_HADOOP_JAR_URL}
fi