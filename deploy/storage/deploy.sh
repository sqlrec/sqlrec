#!/bin/bash
set -eo pipefail
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/../env.sh"

require_commands kubectl envsubst || exit 1

case "${STORAGE_MODE}" in
  hostpath)
    template="${dir}/hostpath_pvc.yaml"
    access_mode=ReadWriteOnce
    storage_class=""
    ;;
  shared)
    if [ -z "${STORAGE_CLASS}" ]; then
      echo 'ERROR: shared storage requires STORAGE_CLASS (must support ReadWriteMany).' >&2
      exit 1
    fi
    kubectl get storageclass "${STORAGE_CLASS}" >/dev/null
    template="${dir}/shared_pvc.yaml"
    access_mode=ReadWriteMany
    storage_class="${STORAGE_CLASS}"
    ;;
  *)
    echo "ERROR: unsupported STORAGE_MODE '${STORAGE_MODE}'; use hostpath or shared." >&2
    exit 1
    ;;
esac

if [ "${LIB_PVC_NAME}" = "${CLIENT_PVC_NAME}" ]; then
  echo 'ERROR: LIB_PVC_NAME and CLIENT_PVC_NAME must be different.' >&2
  exit 1
fi

# PVC storage classes and access modes cannot be changed in place. Fail before
# applying resources when a previous deployment used another storage backend.
for claim in "${LIB_PVC_NAME}" "${CLIENT_PVC_NAME}"; do
  existing="$(kubectl get pvc "${claim}" -n "${NAMESPACE}" --ignore-not-found \
    -o jsonpath='{.spec.accessModes[*]}|{.spec.storageClassName}')"
  if [ -n "${existing}" ] && [ "${existing}" != "${access_mode}|${storage_class}" ]; then
    echo "ERROR: PVC ${claim} uses '${existing}', expected '${access_mode}|${storage_class}'. Use different PVC names to change storage backends." >&2
    exit 1
  fi
done

render_config "${template}"
kubectl apply -f "${template}.tmp" -n "${NAMESPACE}"
# Shared classes may use WaitForFirstConsumer. The sync Pod mounts both claims
# before waiting for readiness, allowing those volumes to be provisioned.
