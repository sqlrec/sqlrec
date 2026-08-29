# Service Deployment

This document introduces how to deploy the SQLRec system.

## System Requirements

The deployment scripts target AMD64 Linux and require Bash, Docker, kubectl, and Helm. The Minikube flow can install missing Docker, Minikube, and Helm components; production environments should manage them centrally.

The Minikube example allocates a 256GB disk and starts several dependencies. Actual memory and disk needs depend on enabled components and data volume; 32GB/256GB should not be treated as a fixed production size. The first deployment needs access to image/Helm repositories and download URLs.

## Quick Deployment (Minikube)

You can quickly deploy a test environment using Minikube:

```bash
# clone sqlrec repository
git clone https://github.com/sqlrec/sqlrec.git
cd ./sqlrec/deploy

# deploy minikube
./deploy_minikube.sh

# verify pod status, wait all pod ready
alias kubectl="minikube kubectl --"
kubectl get pods --all-namespaces

# download resource
./download_resource.sh

# deploy sqlrec and dependencies services
./deploy_components.sh

# verify pod status, wait all pod ready
kubectl get pods --all-namespaces

# verify sqlrec service
cd ..
bash ./bin/beeline.sh
```

**Notes**:
- The Minikube-based deployment solution above is for testing only
- If you need to redeploy, you can first delete the cluster via `minikube delete`
- Some components are not deployed by default, such as Kyuubi, Jupyter, etc. If needed, you can execute the corresponding deployment scripts in the deploy directory
- Deployment scripts read `deploy/env.sh`; override values before execution, for example `NAMESPACE=dev SQLREC_VERSION=0.1.10 bash ./deploy_components.sh`
- `deploy_components.sh` deploys PostgreSQL, MinIO/JuiceFS, Hadoop, HMS, Flink, Spark, SQLRec, and by default Kafka, Redis, and Milvus. HDFS, MongoDB, Kyuubi, Jupyter, and observability components require their own scripts

## Production Environment Deployment

Do not copy the Minikube flow directly into production. Prepare Kubernetes, storage, PostgreSQL, Hive Metastore, and Flink SQL Gateway first, then adapt the YAML for your network, storage classes, and security policy. The repository manifests use `hostPath` and NodePort and are primarily intended for single-node/test environments.

### Core Dependency Services

SQLRec requires the following core dependency services to run:

| Service | Purpose | Required |
|---------|---------|----------|
| **Kubernetes** | Container orchestration platform for deploying and managing model training, export, and serving | Yes |
| **PostgreSQL** | Metadata storage, storing model, service, function definitions, etc. | Yes |
| **Hive Metastore** | Table metadata management, managing Hive table structure information | Yes |
| **Flink SQL Gateway** | SQL execution engine, executing Flink SQL statements | Yes |
| **Distributed Storage** | Storing model files, training data, etc. (MinIO/JuiceFS/HDFS) | Yes |

### Optional Dependency Services

| Service | Purpose |
|---------|---------|
| Kafka | Message queue for streaming data processing |
| Redis | Cache service |
| Milvus | Vector database for vector search |
| Spark | Distributed computing engine |
| Kyuubi | SQL gateway, providing multi-tenant SQL services |
| Jupyter | Notebook environment for interactive development |

### PersistentVolume Configuration

SQLRec relies on Kubernetes PersistentVolume (PV) to store client components and configuration files. Production environments need to prepare the following PVs in advance:

**Required PVs**:

| PV Name | Purpose | Size Recommendation |
|---------|---------|---------------------|
| `sqlrec-lib-pv` / `sqlrec-lib-pvc` | Dependency JARs such as the JuiceFS Hadoop JAR | 128Gi (example default) |
| `sqlrec-client-pv` / `sqlrec-client-pvc` | Hadoop, Hive, Spark, Java clients and configuration | 128Gi (example default) |

`deploy/pv.yaml` defines `hostPath`, `ReadWriteOnce` PVs with a `Retain` reclaim policy. Replace them with a cluster-backed StorageClass/PV in production and verify how SQLRec, Flink, Spark, and HMS access the client files.

**Client files and Hadoop configuration**:

The SQLRec container uses `HADOOP_HOME`, `HADOOP_CONF_DIR`, and `CLASSPATH` to access the clients. Deployment scripts copy files from `deploy/data/conf` into the Hadoop, Hive, and Spark client directories; manual deployments must make these clients and configurations readable from the mounted volume.

**Key Configuration Files**:

| File | Description | Required Configuration Items |
|------|-------------|------------------------------|
| `core-site.xml` | Hadoop core configuration | `fs.defaultFS`, JuiceFS related configurations |
| `hdfs-site.xml` | HDFS configuration | Replication factor, block size, etc. |
| `hive-site.xml` | Hive configuration | `hive.metastore.uris` (when Hive tables are used) |

### SQLRec Service Configuration

SQLRec service is deployed through Kubernetes Deployment with the following main configuration items:

**Required Environment Variables**:

| Environment Variable | Description |
|---------------------|-------------|
| `NAMESPACE` | Kubernetes namespace |
| `MODEL_BASE_PATH` | Model storage base path; the example YAML currently fixes it to `/user/sqlrec/models`, so change the YAML for production |
| `META_DB_URL` | PostgreSQL connection URL |
| `META_DB_USER` | PostgreSQL username |
| `META_DB_PASSWORD` | PostgreSQL password |
| `HIVE_METASTORE_URI` | Hive Metastore Thrift URI |
| `FLINK_SQL_GATEWAY_ADDRESS` | Flink SQL Gateway address |
| `FLINK_SQL_GATEWAY_PORT` | Flink SQL Gateway port |

**Service Ports**:

| Port | Service | Description |
|------|---------|-------------|
| 30000 | Thrift Server | JDBC/Beeline connection port |
| 30001 | REST Server | REST API port |
| 30002 | Debug | Remote debugging port |

**Kubernetes Permissions**:

SQLRec requires the following Kubernetes permissions to manage model training and service deployment:

```bash
# Create ServiceAccount
kubectl create serviceaccount sqlrec -n ${NAMESPACE}

# Grant edit permissions
kubectl create clusterrolebinding sqlrec-role \
  --clusterrole=edit \
  --serviceaccount=${NAMESPACE}:sqlrec \
  --namespace=${NAMESPACE}
```

### Deployment Steps

1. **Prepare Kubernetes Cluster**

   Ensure the Kubernetes cluster is properly configured and can access the container image registry.

2. **Prepare Client PV**

   Create PV and PVC, and prepare Hadoop, Hive, Spark clients and configuration files in the client directory.

3. **Deploy PostgreSQL**

   ```bash
   # Initialize table structure
   psql -d sqlrec -f deploy/sql/master.sql
   ```

4. **Deploy Hive Metastore**

   Ensure Hive Metastore service is started and accessible.

5. **Deploy Flink SQL Gateway**

   Ensure Flink SQL Gateway service is started and accessible.

6. **Deploy Distributed Storage**

   Choose MinIO, JuiceFS, or HDFS as the storage backend according to actual needs.

7. **Deploy SQLRec**

   ```bash
   # Initialize metadata, permissions, and the SQLRec Deployment
   bash deploy/sqlrec/deploy.sh
   ```

   Do not run only `envsubst`: `deploy/sqlrec/deploy.sh` also initializes PostgreSQL, imports `deploy/sql/master.sql`, creates the ServiceAccount, and renders the temporary YAML. Production users may reuse the steps after reviewing database addresses, permissions, NodePorts, and storage.

8. **Verify Deployment**

   ```bash
   # Check Pod status
   kubectl get pod -n ${NAMESPACE}
   
   # Connection test
   bash ./bin/beeline.sh
   ```

## Image Building

SQLRec provides two image build scripts:

| Script | Built Images |
|--------|--------------|
| `bin/build_sqlrec_docker.sh` | SQLRec service related images |
| `bin/build_model_docker.sh` | Model training/inference images |

**Built Images**:

| Image | Dockerfile | Description |
|-------|------------|-------------|
| `sqlrec/sqlrec:${SQLREC_VERSION}` | `docker/Dockerfile` | SQLRec service image |
| `sqlrec/sqlrec-demo:${SQLREC_VERSION}` | `docker/demo.Dockerfile` | SQLRec Demo image |
| `sqlrec/tzrec:${SQLREC_VERSION}-cpu` | `docker/sqlrec-model-tzrec.Dockerfile` | tzrec model training/inference image (CPU version) |
| `sqlrec/gbdt:${SQLREC_VERSION}-cpu` | `docker/sqlrec-model-gbdt.Dockerfile` | GBDT (LightGBM/XGBoost/CatBoost) training/inference image (CPU version) |

The image version `SQLREC_VERSION` comes from `deploy/env.sh` (default `0.1.10`) and can be overridden via environment variables before execution.

**Build Steps**:

```bash
# Build SQLRec service images
bash ./bin/build_sqlrec_docker.sh

# Build model images
bash ./bin/build_model_docker.sh
```

::: tip Tip
The scripts automatically switch to the project root directory to execute the build, no manual cd is needed; the scripts internally `source deploy/env.sh` to read the version number and other configurations.
:::

**Minikube Environment**:

If a Minikube environment is detected, the build scripts will automatically configure Minikube's Docker environment so that built images can be directly used by Minikube:

```bash
if command -v minikube >/dev/null 2>&1; then
  eval $(minikube -p minikube docker-env)
fi
```

**Manual Build**:

If you need to build images manually:

```bash
# Enter project root directory
cd /path/to/sqlrec

# Build SQLRec service image
docker build -t sqlrec/sqlrec:0.1.10 -f ./docker/Dockerfile .

# Build model image
docker build -t sqlrec/tzrec:0.1.10-cpu -f ./docker/sqlrec-model-tzrec.Dockerfile .
```
