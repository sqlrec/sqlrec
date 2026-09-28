#!/bin/bash
set -eo pipefail
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/../env.sh"

case "${STORAGE_MODE}" in
  hostpath|shared) ;;
  *) echo "ERROR: unsupported STORAGE_MODE '${STORAGE_MODE}'." >&2; exit 1 ;;
esac
if [ "${STORAGE_MODE}" = shared ]; then
  validate_runtime_resources || exit 1
fi

# Both storage backends use the same prepared configuration. Copy before
# deciding whether the client directories need a remote upload.
for config in "${CONF_DIR}"/*; do
  [ -f "${config}" ] || continue
  for destination in "${HADOOP_HOME}/etc/hadoop" "${HIVE_HOME}/conf" "${SPARK_HOME}/conf"; do
    [ -d "${destination}" ] || continue
    cp "${config}" "${destination}/"
  done
done

case "${STORAGE_MODE}" in
  hostpath)
    echo 'Skip resource upload: hostPath uses the directories mapped into Minikube.'
    exit 0
    ;;
  shared) ;;
esac

require_commands kubectl envsubst tar find readlink || exit 1
if command -v sha256sum >/dev/null 2>&1; then
  hash_command=(sha256sum)
else
  require_commands shasum || exit 1
  hash_command=(shasum -a 256)
fi

temporary="$(mktemp -d "${TMPDIR:-/tmp}/sqlrec-sync.XXXXXX")"
export RESOURCE_SYNC_CONFIGMAP_NAME="sqlrec-sync-worker-$(printf '%s' "${temporary##*.}" | tr '[:upper:]' '[:lower:]')-$$"
pod_created=false
configmap_created=false
cleanup() {
  if [ "${configmap_created}" = true ]; then
    kubectl delete configmap "${RESOURCE_SYNC_CONFIGMAP_NAME}" -n "${NAMESPACE}" --ignore-not-found >/dev/null || true
  fi
  if [ "${pod_created}" = true ]; then
    kubectl delete pod "${RESOURCE_SYNC_POD_NAME}" -n "${NAMESPACE}" --ignore-not-found >/dev/null || true
  fi
  rm -rf "${temporary}"
}
trap cleanup EXIT
trap 'exit 1' HUP INT TERM

# Select extracted runtime clients, not archives, host Java, Minikube binaries,
# or host installers. Kyuubi is included when it has been downloaded.
clients=("./${HADOOP_CLIENT_DIR_NAME}" "./${HIVE_CLIENT_DIR_NAME}" \
  "./${SPARK_CLIENT_DIR_NAME}" "./${CONTAINER_JAVA_DIR_NAME}")
if [ -d "${CLIENT_DIR}/${KYUUBI_CLIENT_DIR_NAME}" ]; then
  clients+=("./${KYUUBI_CLIENT_DIR_NAME}")
fi
for client in "${clients[@]}"; do
  if [ ! -d "${CLIENT_DIR}/${client}" ]; then
    echo "ERROR: missing runtime client ${CLIENT_DIR}/${client}; run download_resource.sh first." >&2
    exit 1
  fi
done

build_manifest() {
  local root=$1 manifest=$2
  shift 2
  local path kind digest target
  (cd "${root}" && find "$@" \( -type d -o -type f -o -type l \) -print0) > "${manifest}.paths"
  : > "${manifest}"
  while IFS= read -r -d '' path; do
    [ "${path}" != . ] || continue
    case "${path}" in
      *$'\n'*|*$'\t'*) echo "ERROR: resource names cannot contain tabs or newlines: ${path}" >&2; return 1 ;;
    esac
    if [ -L "${root}/${path}" ]; then
      kind=L
      target="$(readlink "${root}/${path}")"
      case "${target}" in
        *$'\n'*) echo "ERROR: resource symlink targets cannot contain newlines: ${path}" >&2; return 1 ;;
      esac
      digest="$(printf '%s' "${target}" | "${hash_command[@]}")"
    elif [ -d "${root}/${path}" ]; then
      kind=D
      digest=-
    else
      kind=F
      [ ! -x "${root}/${path}" ] || kind=X
      digest="$("${hash_command[@]}" < "${root}/${path}")"
    fi
    printf '%s\t%s\t%s\n' "${kind}" "${digest%% *}" "${path}" >> "${manifest}"
  done < "${manifest}.paths"
}

build_manifest "${CLIENT_DIR}" "${temporary}/client.manifest" "${clients[@]}"
build_manifest "${LIB_DIR}" "${temporary}/lib.manifest" .

# Atomically reserve the helper name before creating its private ConfigMap.
# Never delete an existing helper: it may belong to an active deployment.
# Readiness also waits for WaitForFirstConsumer storage to bind.
envsubst < "${dir}/sync_pod.yaml" > "${temporary}/sync_pod.yaml"
if ! kubectl create -f "${temporary}/sync_pod.yaml" -n "${NAMESPACE}"; then
  echo "ERROR: could not create sync Pod ${RESOURCE_SYNC_POD_NAME}. If it already exists, wait for the active sync to finish; only remove it after confirming the previous sync has stopped." >&2
  exit 1
fi
pod_created=true
kubectl create configmap "${RESOURCE_SYNC_CONFIGMAP_NAME}" \
  --from-file="worker.sh=${dir}/sync_worker.sh" -n "${NAMESPACE}"
configmap_created=true
if ! kubectl wait --for=condition=Ready "pod/${RESOURCE_SYNC_POD_NAME}" \
  -n "${NAMESPACE}" --timeout="${DEPLOY_TIMEOUT}s"; then
  kubectl describe pod "${RESOURCE_SYNC_POD_NAME}" -n "${NAMESPACE}" >&2 || true
  exit 1
fi

sync_directory() {
  local name=$1 root=$2
  local manifest="${temporary}/${name}.manifest"
  local missing="${temporary}/${name}.missing"
  local remote_manifest="/tmp/sqlrec-${name}.manifest"
  local remote_changed="/tmp/sqlrec-${name}.changed"
  kubectl exec -i "${RESOURCE_SYNC_POD_NAME}" -n "${NAMESPACE}" -- \
    sh -c 'cat > "$1"' sh "${remote_manifest}" < "${manifest}"
  kubectl exec "${RESOURCE_SYNC_POD_NAME}" -n "${NAMESPACE}" -- \
    sh /scripts/worker.sh check "/sync/${name}" "${remote_manifest}" "${remote_changed}" > "${missing}"
  if [ -s "${missing}" ]; then
    echo "Uploading $(wc -l < "${missing}" | tr -d ' ') changed ${name} files."
    while IFS= read -r path; do
      printf '%s\0' "${path}"
    done < "${missing}" > "${missing}.paths"
    # BSD and GNU tar both support --no-recursion. Only the paths reported by
    # the remote checker are sent, retaining executable modes and symlinks.
    COPYFILE_DISABLE=1 tar -C "${root}" --null --no-recursion -cf - -T "${missing}.paths" | \
      kubectl exec -i "${RESOURCE_SYNC_POD_NAME}" -n "${NAMESPACE}" -- \
      sh /scripts/worker.sh install "/sync/${name}" "${remote_changed}"
    kubectl exec "${RESOURCE_SYNC_POD_NAME}" -n "${NAMESPACE}" -- \
      sh /scripts/worker.sh verify "/sync/${name}" "${remote_manifest}"
  else
    echo "Skip ${name} upload: all files already match."
  fi
}

sync_directory client "${CLIENT_DIR}"
sync_directory lib "${LIB_DIR}"
echo 'Resource synchronization complete.'
