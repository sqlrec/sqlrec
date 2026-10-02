# Service Deployment

Deployment has two stages: prepare a Kubernetes cluster, then deploy SQLRec and its dependencies. Minikube and existing clusters use the same `download_resource.sh` and `deploy_components.sh`; `STORAGE_MODE` selects how client files are stored.

## System Requirements

The deployment machine can run AMD64/ARM64 Linux or Apple Silicon macOS. The first deployment needs access to image registries, Helm repositories, and resource download URLs.

| Deployment | Preparation |
|------------|-------------|
| Minikube development environment | Linux uses the Docker driver; macOS uses vfkit, vmnet-shared networking, and VirtioFS mounts. `deploy_minikube.sh` installs missing tools and starts the cluster |
| Existing Kubernetes cluster | Configure kubeconfig and install `kubectl`, `helm`, `curl`, `tar`, `gzip`, `envsubst`, `psql`, and either `sha256sum` or `shasum`; Minikube is not required |

Minikube deployment on macOS requires macOS 14 or later and Homebrew. The main packages installed by the script are equivalent to:

```bash
brew install minikube vfkit docker docker-buildx helm gettext libpq
```

The script also installs `kubernetes-cli` if `kubectl` is missing and configures Buildx and `vmnet-helper`. Docker Desktop is not required on macOS. Use the checks under `deploy/` for exact tool version requirements.

## Quick Deployment (Minikube)

```bash
git clone https://github.com/sqlrec/sqlrec.git
cd sqlrec/deploy

# Install tools, start the cluster, and configure its default StorageClass
bash ./deploy_minikube.sh
kubectl get nodes -o wide

# Download clients/JARs, add Helm repositories, and install required operators
bash ./download_resource.sh

# Deploy SQLRec and its dependencies
bash ./deploy_components.sh
kubectl get pods --all-namespaces

# Verify the connection after required components are ready
cd ..
bash ./bin/beeline.sh
```

A successful `SHOW TABLES;` in Beeline confirms that the core SQLRec service is ready.

Minikube configuration:

- The script mounts the host's `data` directory into Minikube at the same path. Clients and JARs use the host files directly, without uploading a second copy to PVCs.
- On macOS, defaults are the host's physical core count, 80% of total memory, and a 256GB disk. Override them with `MINIKUBE_CPUS`, `MINIKUBE_MEMORY_PERCENT`, `MINIKUBE_MEMORY`, or `MINIKUBE_DISK_SIZE`; an explicit `MINIKUBE_MEMORY` takes precedence. Actual resource needs depend on enabled components and data volume.
- Dynamic volumes use the project-installed `local-path` StorageClass by default, with data at `/data/local-path-provisioner` on the node. Set `LOCAL_PATH_PROVISIONER_DATA_DIR` before startup to use another absolute path. These dynamic volumes are separate from the hostPath PVs mounting host client directories.
- NodePorts use the Minikube node address. Access through the host's physical IP from other LAN machines is not guaranteed.

::: details Save Image Caches Manually

After deploying components, optionally run:

```bash
# Run from deploy; save workload images that have already been pulled
bash ./cache_images.sh save
```

Caches are stored in `data/image-cache/<arch>`. Later runs of `deploy_minikube.sh` load existing caches automatically. `deploy_components.sh` prints the save command but does not save images automatically. Image caches do not contain database data; deleting the Minikube cluster loses data in node dynamic volumes.
:::

## Deploy to an Existing Kubernetes Cluster

This flow installs components in the cluster selected by the current kubeconfig context. It does not create a cluster.

### Prerequisites

1. Ensure the deployment account can create namespaces, install operators/CRDs, and create the RBAC resources used by the scripts. Besides downloading files, `download_resource.sh` installs operators including CloudNativePG and Flink and configures Helm repositories.
2. Prepare a StorageClass supporting **ReadWriteMany (RWX)** and set `STORAGE_CLASS` so clients and JARs can be shared across nodes. The script checks that the class exists; the cluster administrator must confirm RWX support.
3. Ensure a usable default StorageClass is available for data volumes such as PostgreSQL and RustFS. It can differ from the RWX class used for clients.
4. Select a worker node address reachable from both the deployment machine and pods as `NODE_IP`, and ensure the required NodePorts are accessible. Discovery selects a Ready worker that is not cordoned, excludes control-plane nodes, and uses its InternalIP. Set a reachable address explicitly if the deployment machine cannot reach that InternalIP.
5. If the deployment machine and cluster nodes have different architectures, set `CONTAINER_ARCH=amd64` or `arm64` before downloading resources to select container Java. This does not change host Java or automatically reconcile different architectures within a cluster.

### Deployment Example

Run these commands in the same terminal, replacing the example context, address, and StorageClass:

```bash
cd /path/to/sqlrec
kubectl config use-context your-cluster-context
kubectl get nodes -o wide
kubectl get storageclass

export NAMESPACE=sqlrec
export NODE_IP=10.0.0.10
export STORAGE_MODE=shared
export STORAGE_CLASS=nfs-rwx
export CONTAINER_ARCH=amd64

bash ./deploy/download_resource.sh
bash ./deploy/deploy_components.sh

kubectl get pods -n "${NAMESPACE}"
kubectl get pvc -n "${NAMESPACE}"
bash ./bin/beeline.sh
```

`K8S_APISERVER_ADDR` is read from the current kubeconfig by default, in the form `k8s://https://...`. It accesses the Kubernetes API and is separate from `NODE_IP`, which is used for NodePorts.

`deploy_components.sh` deploys PostgreSQL, RustFS/JuiceFS, Hadoop configuration, Spark configuration, HMS, Flink SQL Gateway, SQLRec, and Kafka, Redis, and Milvus by default. Milvus runs in `${NAMESPACE}-milvus`. HDFS, MongoDB, Kyuubi, Jupyter, and observability components use their own deployment scripts.

## Storage and Resource Synchronization

### Storage Modes

| Item | `hostpath` (Minikube) | `shared` (existing cluster) |
|------|----------------------|----------------------------|
| Template | `deploy/storage/hostpath_pvc.yaml` | `deploy/storage/shared_pvc.yaml` |
| Resources | Static hostPath PVs + PVCs, ReadWriteOnce | PVCs provisioned by the specified StorageClass, ReadWriteMany |
| File source | Host directories mounted into Minikube | Deployment machine files uploaded to shared PVCs |
| Upload behavior | Copy local configuration only; skip upload | A temporary pod mounts the PVCs and uploads changed files |

If `STORAGE_MODE` is unset, a context named `minikube` selects `hostpath`; all other contexts select `shared`. `deploy_minikube.sh` itself always uses `hostpath`. Remote clusters should use `shared`: local deployment directories do not automatically exist on remote nodes.

| Default PVC name | Contents | Capacity setting |
|------------------|----------|------------------|
| `sqlrec-lib-pvc` | Dependency JARs | `LIB_STORAGE_SIZE`, default `128Gi` |
| `sqlrec-client-pvc` | Hadoop, Hive, Spark, container Java clients and configuration | `CLIENT_STORAGE_SIZE`, default `128Gi` |

Override PVC names with `LIB_PVC_NAME` and `CLIENT_PVC_NAME`. Existing PVC storage classes and access modes cannot be switched in place; the script stops on a mismatch. Use new PVC names and plan file migration when changing storage backends. hostPath PVs use the `Retain` reclaim policy; override their names with `LIB_PV_NAME` and `CLIENT_PV_NAME`.

RustFS provides S3 storage in standalone mode, and the deployment creates the buckets required by JuiceFS and Milvus. Data volumes always use the cluster's default StorageClass; `STORAGE_CLASS` for clients/JARs does not affect RustFS. Set capacities with `RUSTFS_DATA_STORAGE_SIZE` and `RUSTFS_LOG_STORAGE_SIZE`, defaulting to `128Gi` and `1Gi` respectively.

### Resource Synchronization

Deployment scripts prepare clients, JARs, and configuration automatically. No manual PV/PVC template application or separate HMS configuration step is needed. After updating prepared resources, run `bash ./deploy/storage/sync.sh` from the repository root. Synchronization does not remove extra remote files; do not run it concurrently in one namespace.

::: details Directories and Synchronization Details

Files are stored in `deploy/data` by default, or `${BASE_DIR}/data` when `BASE_DIR` is set:

| Directory | Contents |
|-----------|----------|
| `conf` | Generated Hadoop, Hive, and Spark configuration |
| `lib` | Dependency JARs |
| `client` | Clients, Java, and downloaded archives |
| `image-cache` | Manually saved Minikube image caches |

`download_resource.sh` creates `conf/lib/client` automatically. Individual component downloads also create their destination directories and reuse existing downloaded files.

`deploy/storage/deploy.sh` creates storage; no manual PV/PVC template application is needed. Deployment creates storage and generates configuration first. The HMS deployment script then calls `deploy/storage/sync.sh` to prepare volume contents before starting the HMS initialization Job. No separate HMS configuration preparation script is needed.

Shared mode uploads runtime clients, container Java, and JARs, excluding download archives, host-only Java, and Minikube tools. Synchronization checks the actual remote files' SHA-256, type, and executable status, skips matching files, and updates new or changed files. Regenerated configuration participates in the same checks. Synchronization does not remove extra remote files and does not make every component deployment step fully idempotent.

Once volumes and local clients are prepared, synchronize manually from the repository root:

```bash
bash ./deploy/storage/sync.sh
```

The temporary sync pod and ConfigMap are cleaned up on exit. Do not run synchronization concurrently in the same namespace. The default pod name is `sqlrec-resource-sync`; the script stops if a pod with that name already exists.

Key configuration files are `core-site.xml` (filesystem/JuiceFS), `hdfs-site.xml` (HDFS), and `hive-site.xml` (Hive Metastore). Shared volumes mount at the `CLIENT_DIR` and `LIB_DIR` paths computed by the scripts.
:::

## Production Configuration

The existing-cluster flow can be reused, but SRE teams should review component capacity, persistent storage, networking, credentials, and permissions for production. Default deployments include single-instance services and NodePorts; adapt them to your operational requirements.

### Dependencies

| Service | Purpose |
|---------|---------|
| Kubernetes | Manage model training, export, and serving |
| PostgreSQL | Model, service, and function metadata |
| Hive Metastore | Hive table metadata |
| Flink SQL Gateway | Optional; executes forwarded Flink SQL/RPCs |
| Distributed storage | Model files and training data; RustFS + JuiceFS by default |

Use Kafka, Redis, Milvus, Spark, Kyuubi, and Jupyter according to workload needs. Kafka, Redis, and Milvus are already included in the default component deployment flow.

### Script Parameters and Container Configuration

Scripts read `deploy/env.sh`. Override supported parameters using matching environment variables before execution, for example:

```bash
export NAMESPACE=dev
export SQLREC_VERSION=your-version
bash ./deploy/download_resource.sh
bash ./deploy/deploy_components.sh
```

Keep namespace, version, path, and architecture settings consistent between resource download and deployment. Main script parameters and container settings include:

| Parameter | Purpose |
|-----------|---------|
| `NODE_IP` | NodePort address reachable from both the deployment machine and pods |
| `SQLREC_POSTGRESQL_USER` / `SQLREC_POSTGRESQL_PASSWORD` | SQLRec metadata database credentials |
| `HMS_POSTGRESQL_USER` / `HMS_POSTGRESQL_PASSWORD` | HMS metadata database credentials |
| `SQLREC_POSTGRESQL_PORT` / `HMS_POSTGRESQL_PORT` | Database NodePorts |
| `HMS_PORT` | HMS NodePort |
| `FLINK_SQL_GATEWAY_ADDRESS` | Gateway host; defaults to `NODE_IP` during deployment, and an empty address disables forwarding |
| `FLINK_SQL_GATEWAY_PORT` | Gateway NodePort and SQLRec connection port, default `30018` |
| `FLINK_SQL_GATEWAY_CONNECT_TIMEOUT` | SQLRec container runtime setting; connection and RPC read timeout, default `600000` ms (10 minutes) |
| `SQLREC_THRIFT_PORT` / `SQLREC_REST_PORT` | JDBC/Beeline / REST NodePorts, default `30000` / `30001` |
| `RUSTFS_ACCESS_KEY` / `RUSTFS_SECRET_KEY` | Shared S3 credentials |
| `DEPLOY_TIMEOUT` | Deployment wait timeout, default `3600` seconds |

The SQLRec container's `META_DB_URL` and `HIVE_METASTORE_URI` are generated in `deploy/sqlrec/sqlrec.yaml` from `NODE_IP` and the corresponding ports. `MODEL_BASE_PATH` is fixed to `/user/sqlrec/models` in that template.

To use dependencies managed by SRE teams, adjust the corresponding deployment steps and YAML connection settings. The scripts do not skip component deployment when an external address is set. Exporting `META_DB_URL` or `HIVE_METASTORE_URI` alone does not replace the template addresses. Even running `deploy/sqlrec/deploy.sh` alone deploys PostgreSQL first.

SQLRec and Spark scripts create their ServiceAccounts and grant cluster-wide `edit` access through ClusterRoleBindings. For production, scope permissions to the required resources and namespaces using restricted Roles/RoleBindings.

### Gateway Forwarding Configuration

Deployment scripts install Flink SQL Gateway and configure SQLRec's connection address by default. Gateway serves requests that require Flink execution; local SQL, JDBC metadata, and persistent table/database/UDF metadata DDL do not depend on it.

Default settings require no changes. For an external Gateway, set the address and port listed above to its HiveServer2/Thrift endpoint. An empty address followed by redeployment disables forwarding; this does not skip default Flink component deployment or remove existing components.

```bash
export FLINK_SQL_GATEWAY_ADDRESS=""
bash ./deploy/sqlrec/deploy.sh
```

See [Architecture](../reference/architecture.md#gateway-forwarding-boundaries) for execution boundaries and the [SQL Reference](../reference/sql.md#set) for SET/RESET behavior.

### SQL Initialization

`deploy/sqlrec/deploy.sh` waits until PostgreSQL is reachable, executes `deploy/sql/master.sql`, and applies the SQLRec Deployment only after success. Normal script deployment does not require a separate manual SQL import.

::: details Initialization Retries and Transactions

| Parameter | Default | Requirement |
|-----------|---------|-------------|
| `PSQL_MAX_ATTEMPTS` | `30` | Positive integer; maximum connection attempts |
| `PSQL_RETRY_INTERVAL` | `5` seconds | Non-negative integer; connection retry interval |
| `PGCONNECT_TIMEOUT` | `10` seconds | Positive integer; timeout per connection attempt |

Invalid parameters fail before resources are created. Connection failures are retried according to these settings. Schema SQL uses `ON_ERROR_STOP=1` and a single transaction: SQL errors stop deployment and roll back the initialization attempt, without retrying schema SQL. Existing tables are reused with `CREATE TABLE IF NOT EXISTS`; the file does not migrate existing table definitions.
:::

## Verification and Troubleshooting

Run from the repository root. If `NAMESPACE` is unset, these commands use the default `sqlrec`:

```bash
kubectl get pods -n "${NAMESPACE:-sqlrec}"
kubectl get pvc -n "${NAMESPACE:-sqlrec}"
bash ./bin/beeline.sh
```

| Symptom | Check |
|---------|-------|
| Missing or invalid client/JAR | Run `deploy/download_resource.sh` first; if the error persists, remove the affected corrupt file or extracted directory and rerun |
| Shared PVC or sync pod remains Pending | Use `kubectl describe pvc <name> -n <namespace>`; check RWX support, capacity, and permissions. WaitForFirstConsumer classes begin binding after the sync pod is created |
| PostgreSQL connection retries exhausted | Check database pod readiness, `NODE_IP`, NodePorts, routes, and credentials. Fix SQL errors using the `psql` output |
| Sync pod already exists | Wait for the active sync to finish; remove a leftover pod only after confirming that the previous sync has stopped |

See [Observability](./observability.md) for metrics, traces, and logs after deployment.
