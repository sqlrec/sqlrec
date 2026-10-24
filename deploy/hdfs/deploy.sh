#!/bin/bash
set -eo pipefail
dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/../env.sh"

require_commands kubectl envsubst
export HDFS_NODE_HOSTNAME="$(kubectl get nodes -o jsonpath='{range .items[*]}{.metadata.labels.kubernetes\.io/hostname}{"|"}{.status.addresses[?(@.type=="InternalIP")].address}{"\n"}{end}' |
  awk -F '|' -v ip="${NODE_IP}" '$2 == ip {print $1; exit}')"
if [ -z "${HDFS_NODE_HOSTNAME}" ]; then
  echo "ERROR: no Kubernetes node has InternalIP ${NODE_IP}; HDFS requires a directly reachable node address." >&2
  exit 1
fi

# HDFS also needs these files before HMS is deployed, especially on shared PVCs.
bash "${dir}/../storage/sync.sh"
render_config "${dir}/hdfs-pvc.yaml"
if ! kubectl get pvc "${HDFS_NAMENODE_PVC_NAME}" -n "${NAMESPACE}" >/dev/null 2>&1; then
  kubectl apply -f "${dir}/hdfs-pvc.yaml.tmp" -l component=namenode -n "${NAMESPACE}"
fi
if ! kubectl get pvc "${HDFS_DATANODE_PVC_NAME}" -n "${NAMESPACE}" >/dev/null 2>&1; then
  kubectl apply -f "${dir}/hdfs-pvc.yaml.tmp" -l component=datanode -n "${NAMESPACE}"
fi

# Always check the actual volume: a completed Job may refer to an old PVC, and
# an absent Job does not mean the current volume needs formatting.
kubectl delete job hdfs-namenode-init -n "${NAMESPACE}" --ignore-not-found --wait=true
render_config "${dir}/hdfs-init-job.yaml"
kubectl apply -f "${dir}/hdfs-init-job.yaml.tmp" -n "${NAMESPACE}"
wait_for_job hdfs-namenode-init "${NAMESPACE}" "${DEPLOY_TIMEOUT}"

diagnostics() {
  kubectl get pods -l app=hadoop -n "${NAMESPACE}" -o wide >&2 || true
  kubectl describe pods -l app=hadoop -n "${NAMESPACE}" >&2 || true
  kubectl logs deployment/hadoop-namenode -n "${NAMESPACE}" --tail=50 >&2 || true
  kubectl logs deployment/hadoop-datanode -n "${NAMESPACE}" --tail=50 >&2 || true
}
trap diagnostics ERR

render_config "${dir}/hdfs.yaml"
kubectl apply -f "${dir}/hdfs.yaml.tmp" -n "${NAMESPACE}"
for deployment in hadoop-namenode hadoop-datanode; do
  kubectl rollout status "deployment/${deployment}" -n "${NAMESPACE}" --timeout="${DEPLOY_TIMEOUT}s"
done

# Empty namespaces may leave safe mode before a DataNode registers. Check both
# registration and safe mode before the host-to-DataNode read/write round trip.
kubectl exec deployment/hadoop-namenode -n "${NAMESPACE}" -- \
  timeout "${DEPLOY_TIMEOUT}s" bash -ec '
    until "$1/bin/hdfs" dfsadmin -fs hdfs://myhdfs -report -live | grep -Eq "^Live datanodes \([1-9][0-9]*\):"; do sleep 2; done
    "$1/bin/hdfs" dfsadmin -fs hdfs://myhdfs -safemode wait
  ' bash "${HADOOP_HOME}"

probe="hdfs://myhdfs/tmp/sqlrec-deploy-$(date +%s)-$$"
hdfs=("${HADOOP_HOME}/bin/hdfs" dfs)
cleanup() { "${hdfs[@]}" -rm -f "${probe}" >/dev/null 2>&1 || true; }
trap cleanup EXIT
"${hdfs[@]}" -mkdir -p hdfs://myhdfs/tmp
printf 'sqlrec-hdfs-ready' | "${hdfs[@]}" -put - "${probe}"
test "$("${hdfs[@]}" -cat "${probe}")" = sqlrec-hdfs-ready
echo 'HDFS is ready: DataNode registered and remote read/write verified.'
