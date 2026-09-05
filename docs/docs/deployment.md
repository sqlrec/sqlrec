# 服务部署

本文档介绍如何部署 SQLRec 系统。

## 系统要求

部署脚本支持 AMD64 Linux 和 Apple Silicon macOS 14 及以上版本。Linux 使用 Minikube Docker driver；macOS 使用 Minikube 1.37+、vfkit、vmnet-shared 网络和 VirtioFS 挂载。生产环境应由运维统一管理 Kubernetes 及相关依赖。

macOS 不需要安装 Docker Desktop。部署脚本会通过 Homebrew 安装缺少的命令行依赖，等效命令如下：

```bash
brew install minikube vfkit docker docker-buildx helm gettext libpq
```

部署脚本还会配置 Homebrew Buildx 插件，并按照 [Minikube vfkit 官方文档](https://minikube.sigs.k8s.io/docs/drivers/vfkit/)中对应 macOS 版本的方式安装 `vmnet-helper`。

Minikube 示例把磁盘配额设为 256GB，并且会同时启动多个依赖服务；实际内存和磁盘需求取决于启用的组件及数据量，不能把 32GB/256GB 视为生产环境的固定规格。首次部署需要能访问镜像仓库、Helm 仓库和资源下载地址。

## 快速部署（Minikube）

使用 Minikube 可以快速部署一个测试环境：

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

**注意事项**：
- 上述基于 Minikube 的部署方案仅用于测试
- 如果需要重新部署，可以先通过 `minikube delete` 删除集群
- 部署成功后，工作负载镜像会保存到 `deploy/data/image-cache/<arch>`；重新创建集群时会自动加载
- macOS 默认给 Minikube 分配 8 CPU、24GB 内存和 256GB 磁盘，可通过 `MINIKUBE_CPUS`、`MINIKUBE_MEMORY`、`MINIKUBE_DISK_SIZE` 覆盖
- 主机、Pod 和共享配置统一通过 `minikube ip` 返回的 `NODE_IP` 访问 NodePort；不保证局域网其他机器通过宿主机物理 IP 访问
- 有一些组件没有默认部署，比如 Kyuubi、Jupyter 等，如果需要，可以在 deploy 目录执行对应的部署脚本
- 部署脚本会读取 `deploy/env.sh`；可在执行前通过同名环境变量覆盖版本、命名空间、密码和端口，例如 `NAMESPACE=dev SQLREC_VERSION=0.1.10 bash ./deploy_components.sh`
- `deploy_components.sh` 默认部署 PostgreSQL、MinIO/JuiceFS、Hadoop、HMS、Flink、Spark、SQLRec，以及 Kafka、Redis、Milvus；HDFS、MongoDB、Kyuubi、Jupyter、监控等组件需单独启用对应脚本

## 生产环境部署

生产环境不要直接照搬 Minikube 脚本。应先准备 Kubernetes、对象/分布式存储、PostgreSQL、Hive Metastore 和 Flink SQL Gateway，再按实际网络、存储类和安全策略改写相应 YAML。仓库中的 `deploy/*.yaml` 使用 `hostPath` 和 NodePort，主要用于单节点/测试环境。

### 核心依赖服务

SQLRec 运行需要以下核心依赖服务：

| 服务 | 用途 | 必需 |
|------|------|------|
| **Kubernetes** | 容器编排平台，用于部署和管理模型训练、导出、服务 | 是 |
| **PostgreSQL** | 元数据存储，存储模型、服务、函数等定义 | 是 |
| **Hive Metastore** | 表元数据管理，管理 Hive 表结构信息 | 是 |
| **Flink SQL Gateway** | SQL 执行引擎，执行 Flink SQL 语句 | 是 |
| **分布式存储** | 存储模型文件、训练数据等（MinIO/JuiceFS/HDFS） | 是 |

### 可选依赖服务

| 服务 | 用途 |
|------|------|
| Kafka | 消息队列，用于流式数据处理 |
| Redis | 缓存服务 |
| Milvus | 向量数据库，用于向量搜索 |
| Spark | 分布式计算引擎 |
| Kyuubi | SQL 网关，提供多租户 SQL 服务 |
| Jupyter | Notebook 环境，用于交互式开发 |

### PersistentVolume 配置

SQLRec 依赖 Kubernetes PersistentVolume (PV) 来存储客户端组件和配置文件。生产环境需要预先准备以下 PV：

**必需的 PV**：

| PV 名称 | 用途 | 大小建议 |
|---------|------|----------|
| `sqlrec-lib-pv` / `sqlrec-lib-pvc` | 依赖 JAR（例如 JuiceFS Hadoop JAR） | 128Gi（示例默认值） |
| `sqlrec-client-pv` / `sqlrec-client-pvc` | Hadoop、Hive、Spark、Java 客户端及配置 | 128Gi（示例默认值） |

`deploy/pv.yaml` 中的 PV 是 `hostPath`、`ReadWriteOnce`，并使用 `Retain` 回收策略；生产环境应替换为集群可用的 StorageClass/PV，并确认 SQLRec、Flink、Spark、HMS 对客户端文件和配置的访问方式。

**客户端文件和 Hadoop 配置**：

SQLRec 容器通过 `HADOOP_HOME`、`HADOOP_CONF_DIR` 和 `CLASSPATH` 访问客户端。部署脚本会把 `deploy/data/conf` 中的配置复制到 Hadoop、Hive 和 Spark 客户端目录；手工部署时至少要保证这些客户端和配置在挂载卷中可读。

**关键配置文件**：

| 文件 | 说明 | 必需配置项 |
|------|------|------------|
| `core-site.xml` | Hadoop 核心配置 | `fs.defaultFS`、JuiceFS 相关配置 |
| `hdfs-site.xml` | HDFS 配置 | 副本数、块大小等 |
| `hive-site.xml` | Hive 配置 | `hive.metastore.uris`（使用 Hive 表时） |

### SQLRec 服务配置

SQLRec 服务通过 Kubernetes Deployment 部署，主要配置项如下：

**必需环境变量**：

| 环境变量 | 说明 |
|----------|------|
| `NAMESPACE` | Kubernetes 命名空间 |
| `MODEL_BASE_PATH` | 模型存储基础路径；仓库示例 YAML 当前固定为 `/user/sqlrec/models`，生产环境应按存储后端修改 YAML |
| `META_DB_URL` | PostgreSQL 连接 URL |
| `META_DB_USER` | PostgreSQL 用户名 |
| `META_DB_PASSWORD` | PostgreSQL 密码 |
| `HIVE_METASTORE_URI` | Hive Metastore Thrift URI |
| `FLINK_SQL_GATEWAY_ADDRESS` | Flink SQL Gateway 地址 |
| `FLINK_SQL_GATEWAY_PORT` | Flink SQL Gateway 端口 |

**服务端口**：

| 端口 | 服务 | 说明 |
|------|------|------|
| 30000 | Thrift Server | JDBC/Beeline 连接端口 |
| 30001 | REST Server | REST API 端口 |
| 30002 | Debug | 远程调试端口 |

**Kubernetes 权限**：

SQLRec 需要在目标命名空间创建/管理模型训练 Job 和服务 Deployment。`deploy/sqlrec/deploy.sh` 会创建名为 `sqlrec` 的 ServiceAccount，并绑定集群级 `edit` 角色；生产环境应按最小权限原则改为命名空间级、资源范围受限的 Role/RoleBinding。

```bash
# 创建 ServiceAccount
kubectl create serviceaccount sqlrec -n ${NAMESPACE}

# 授予编辑权限
kubectl create clusterrolebinding sqlrec-role \
  --clusterrole=edit \
  --serviceaccount=${NAMESPACE}:sqlrec \
  --namespace=${NAMESPACE}
```

### 部署步骤

1. **准备 Kubernetes 集群**

确保 Kubernetes 集群已正确配置，可以访问容器镜像仓库。

2. **准备客户端 PV**

创建 PV 和 PVC，并在客户端目录中准备好 Hadoop、Hive、Spark 客户端和配置文件。

3. **部署 PostgreSQL**

```bash
# 初始化表结构
psql -d sqlrec -f deploy/sql/master.sql
```

4. **部署 Hive Metastore**

确保 Hive Metastore 服务已启动并可访问。

5. **部署 Flink SQL Gateway**

确保 Flink SQL Gateway 服务已启动并可访问。

6. **部署分布式存储**

根据实际需求选择 MinIO、JuiceFS 或 HDFS 作为存储后端。

7. **部署 SQLRec**

```bash
# 应用 Kubernetes 配置
bash deploy/sqlrec/deploy.sh
```

不要只执行 `envsubst`：`deploy/sqlrec/deploy.sh` 还负责初始化 PostgreSQL、导入 `deploy/sql/master.sql`、创建 ServiceAccount 和渲染临时 YAML。生产环境可复用这些步骤，但应先审查脚本中的数据库地址、权限、NodePort 和存储配置。

8. **验证部署**

```bash
# 检查 Pod 状态
kubectl get pod -n ${NAMESPACE}

# 连接测试
bash ./bin/beeline.sh
```

## 镜像构建

SQLRec 提供了两个镜像构建脚本：

| 脚本 | 构建的镜像 |
|------|-----------|
| `bin/build_sqlrec_docker.sh` | SQLRec 服务相关镜像 |
| `bin/build_model_docker.sh` | 模型训练/推理镜像 |

**构建的镜像**：

| 镜像 | Dockerfile | 说明 |
|------|------------|------|
| `sqlrec/sqlrec:${SQLREC_VERSION}` | `docker/Dockerfile` | SQLRec 服务镜像 |
| `sqlrec/sqlrec-demo:${SQLREC_VERSION}` | `docker/demo.Dockerfile` | SQLRec Demo 镜像 |
| `sqlrec/tzrec:${SQLREC_VERSION}-cpu` | `docker/sqlrec-model-tzrec.Dockerfile` | tzrec 模型训练/推理镜像（CPU 版本） |
| `sqlrec/gbdt:${SQLREC_VERSION}-cpu` | `docker/sqlrec-model-gbdt.Dockerfile` | GBDT (LightGBM/XGBoost/CatBoost) 训练/推理镜像（CPU 版本） |

镜像版本号 `SQLREC_VERSION` 来自 `deploy/env.sh`（默认 `0.1.11`），可在执行前通过环境变量覆盖。

**构建步骤**：

```bash
# 构建 SQLRec 服务镜像
bash ./bin/build_sqlrec_docker.sh

# 构建模型镜像
bash ./bin/build_model_docker.sh
```

::: tip 提示
脚本会自动切换到项目根目录执行构建，无需手动 cd；脚本内部会 `source deploy/env.sh` 读取版本号等配置。
:::

**Minikube 环境**：

如果检测到 Minikube 环境，构建脚本会自动配置 Minikube 的 Docker 环境，使构建的镜像可以直接被 Minikube 使用：

```bash
if command -v minikube >/dev/null 2>&1; then
  eval $(minikube -p minikube docker-env)
fi
```

macOS 只安装 Docker CLI，不运行 Docker Desktop，因此构建镜像前必须先启动 Minikube。当前 GBDT Dockerfile 仍包含 x86_64 原生依赖，tzrec 基础镜像的 ARM64 支持也未确认；这两个模型镜像暂不属于 macOS ARM64核心部署的保证范围。

**手动构建**：

如果需要手动构建镜像：

```bash
# 进入项目根目录
cd /path/to/sqlrec

# 构建 SQLRec 服务镜像
docker build -t sqlrec/sqlrec:0.1.10 -f ./docker/Dockerfile .

# 构建模型镜像
docker build -t sqlrec/tzrec:0.1.10-cpu -f ./docker/sqlrec-model-tzrec.Dockerfile .
```
