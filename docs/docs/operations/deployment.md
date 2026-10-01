# 服务部署

部署分为两步：准备 Kubernetes 集群，再部署 SQLRec 和依赖组件。Minikube 和已有集群使用相同的 `download_resource.sh`、`deploy_components.sh`，客户端文件的存储方式由 `STORAGE_MODE` 决定。

## Flink SQL Gateway 默认部署与转发配置

部署脚本默认下载和安装 Flink Operator、Session Cluster 和 Gateway，并为 SQLRec 配置节点上的 Gateway 地址和端口。共享元数据模式仍需要 HMS 和 PostgreSQL；本地 SQL、`USE`、`SET`、JDBC 元数据以及持久表、库和 UDF 的纯元数据 DDL 不依赖 Gateway。DDL 使用 Flink 1.19 default 方言，通过进程内官方 HiveCatalog 的 Hive 客户端直接写 HMS，不创建 TableEnvironment 或 Planner。表 DDL 支持普通列、metadata 列、主键和分区，不支持计算列和 WATERMARK；需要解析表结构的查询和修改也会拒绝这些已有定义。

沿用 `FLINK_SQL_GATEWAY_ADDRESS` 主机地址和 `FLINK_SQL_GATEWAY_PORT` 端口配置，使用 HiveServer2/Thrift 接口。部署时未指定地址就使用 `NODE_IP`；显式空地址或只包含空白时禁用转发。应用自身也保留原有默认 Gateway 地址。

默认部署无需额外配置端口。`FLINK_SQL_GATEWAY_PORT` 同时用于 Gateway 的 NodePort 和 SQLRec 连接，默认 `30018`；部署 Gateway 服务时，自定义值须位于集群允许的 NodePort 范围。

如已准备好依赖组件，需要单独部署 SQLRec 并连接外部 Gateway，可配置外部端口：

```bash
# 使用外部 HiveServer2/Thrift Gateway
export FLINK_SQL_GATEWAY_ADDRESS=your-gateway-host
export FLINK_SQL_GATEWAY_PORT=10000
bash ./deploy/sqlrec/deploy.sh

# 或者关闭 SQLRec 的远程转发
export FLINK_SQL_GATEWAY_ADDRESS=""
```

外部端口示例仅用于 SQLRec 部署，不能把 `10000` 同时用作默认集群中的 Gateway NodePort。

连接和 RPC 读取超时保持原有配置方式，均使用 `FLINK_SQL_GATEWAY_CONNECT_TIMEOUT`，默认 600000 毫秒（10 分钟）。本地可处理的 SQL、JDBC 元数据及客户端会话信息不建立 Gateway 连接；需要远程能力的 SQL/RPC 才连接 Gateway，转发禁用时返回 `FLINK_GATEWAY_DISABLED`。REST/CLI 保持本地执行边界。

不支持 `USE CATALOG` 和创建临时表（包括临时 CTAS/RTAS），这些语句直接报错。`USE database` 不校验会话 schema 中是否存在该数据库，后续操作按实际元数据执行。临时函数、视图及持久表的 CTAS/RTAS 等未纳入本地执行的语句保留 Thrift 远程路由。保存表/UDF 定义不代表 SQLRec 本地支持其全部 Connector、列语义或函数接口。SQL 文件元数据模式继续限制运行时 DDL 和远程转发，但不新增 Gateway 配置的启动冲突检查。

显式空地址仅关闭 SQLRec 转发，不改变默认组件部署，也不删除已有 Flink 组件或作业。清理仍使用显式卸载脚本。

## 系统要求

部署机器支持 AMD64/ARM64 Linux 和 Apple Silicon macOS。首次部署需要能访问镜像仓库、Helm 仓库和资源下载地址。

| 部署方式 | 准备工作 |
|----------|----------|
| Minikube 开发环境 | Linux 使用 Docker driver；macOS 使用 vfkit、vmnet-shared 网络和 VirtioFS 挂载。`deploy_minikube.sh` 安装缺少的工具并启动集群 |
| 已有 Kubernetes 集群 | 配置 kubeconfig，安装 `kubectl`、`helm`、`curl`、`tar`、`gzip`、`envsubst`、`psql`，以及 `sha256sum` 或 `shasum`；无需安装 Minikube |

macOS 的 Minikube 部署需要 macOS 14 或更高版本，以及预先安装 Homebrew。脚本安装的主要依赖等效于：

```bash
brew install minikube vfkit docker docker-buildx helm gettext libpq
```

缺少 `kubectl` 时脚本还会安装 `kubernetes-cli`，并配置 Buildx 和 `vmnet-helper`。macOS 不需要 Docker Desktop。具体工具版本要求以 `deploy/` 中的检查逻辑为准。

## 快速部署（Minikube）

```bash
git clone https://github.com/sqlrec/sqlrec.git
cd sqlrec/deploy

# 安装工具、启动集群并配置默认 StorageClass
bash ./deploy_minikube.sh
kubectl get nodes -o wide

# 下载客户端/JAR，添加 Helm 仓库并安装所需 Operator
bash ./download_resource.sh

# 部署 SQLRec 和依赖组件
bash ./deploy_components.sh
kubectl get pods --all-namespaces

# 待必需组件就绪后验证连接
cd ..
bash ./bin/beeline.sh
```

在 Beeline 中执行 `SHOW TABLES;` 成功，表示 SQLRec 基本服务已就绪。

Minikube 的配置说明：

- 脚本将本机 `data` 目录挂载到 Minikube 中的相同路径；客户端和 JAR 直接使用本机文件，不再上传一份到 PVC。
- macOS 默认使用宿主机物理核心数、总内存的 80% 和 256GB 磁盘。可通过 `MINIKUBE_CPUS`、`MINIKUBE_MEMORY_PERCENT`、`MINIKUBE_MEMORY`、`MINIKUBE_DISK_SIZE` 覆盖；显式设置 `MINIKUBE_MEMORY` 时优先使用该值。实际资源需求取决于启用的组件和数据量。
- 动态卷默认使用项目安装的 `local-path` StorageClass，数据位于节点的 `/data/local-path-provisioner`；启动前可用 `LOCAL_PATH_PROVISIONER_DATA_DIR` 指定其他绝对路径。这些动态卷与挂载本机客户端目录的 hostPath PV 不同。
- NodePort 使用 Minikube 节点地址，不保证局域网其他机器可通过宿主机物理 IP 访问。

::: details 手动保存镜像缓存

组件部署完成后，按需执行：

```bash
# 在 deploy 目录执行；保存已拉取的工作负载镜像
bash ./cache_images.sh save
```

缓存保存到 `data/image-cache/<arch>`。之后运行 `deploy_minikube.sh` 会自动加载已有缓存；`deploy_components.sh` 只提示保存命令，不会自动保存。镜像缓存不包含数据库数据，删除 Minikube 集群会丢失节点动态卷中的数据。
:::

## 在已有 Kubernetes 集群上部署

此流程在当前 kubeconfig context 指向的集群中安装组件，不会创建集群。

### 部署前准备

1. 确认部署账号可以创建命名空间、安装 Operator/CRD 和创建脚本所需的 RBAC 资源。`download_resource.sh` 除了下载文件，还会安装 CloudNativePG、Flink 等 Operator，并配置 Helm 仓库。
2. 准备支持 **ReadWriteMany（RWX）** 的 StorageClass，通过 `STORAGE_CLASS` 指定，供客户端和 JAR 跨节点共享。脚本会检查 StorageClass 是否存在，实际 RWX 能力需由集群管理员确认。
3. 确保集群有可用的默认 StorageClass，供 PostgreSQL、RustFS 等组件创建数据卷。它可以与客户端使用的 RWX StorageClass 不同。
4. 选择部署机器和 Pod 都能访问的工作节点地址作为 `NODE_IP`，并确认所用 NodePort 可达。自动发现只选择 Ready、未被 cordon 的工作节点，排除管控面节点；默认取工作节点的 InternalIP。部署机器无法访问 InternalIP 时应显式设置可达地址。
5. 如果部署机器和集群节点架构不同，在下载资源前设置 `CONTAINER_ARCH=amd64` 或 `arm64`，用于选择容器内 Java。它不会改变部署机器使用的 Java 架构，也不会自动统一集群中不同节点的架构。

### 部署示例

以下命令在同一个终端执行，将示例 context、地址和 StorageClass 替换为实际值：

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

`K8S_APISERVER_ADDR` 默认从当前 kubeconfig 中读取，格式为 `k8s://https://...`；它用于访问 Kubernetes API，与 NodePort 使用的 `NODE_IP` 不同。

`deploy_components.sh` 默认部署 PostgreSQL、RustFS/JuiceFS、Hadoop 配置、Spark 配置、HMS、Flink SQL Gateway、SQLRec，以及 Kafka、Redis、Milvus。Milvus 位于 `${NAMESPACE}-milvus` 命名空间。HDFS、MongoDB、Kyuubi、Jupyter、监控等组件通过各自脚本单独启用。

## 存储与资源同步

### 两种存储模式

| 项目 | `hostpath`（Minikube） | `shared`（已有集群） |
|------|-----------------------|---------------------|
| 模板 | `deploy/storage/hostpath_pvc.yaml` | `deploy/storage/shared_pvc.yaml` |
| 资源 | 静态 hostPath PV + PVC，ReadWriteOnce | 由指定 StorageClass 提供 PVC，ReadWriteMany |
| 文件来源 | Minikube 挂载的本机目录 | 从部署机器上传到共享 PVC |
| 上传行为 | 只复制本机配置，跳过上传 | 临时 Pod 挂载 PVC，按文件内容增量上传 |

未指定 `STORAGE_MODE` 时，context 名为 `minikube` 使用 `hostpath`，其他 context 使用 `shared`。`deploy_minikube.sh` 自身始终使用 `hostpath`。远程集群应使用 `shared`，部署机器的本机目录不会自动出现在远程节点上。

| PVC 默认名称 | 内容 | 容量配置 |
|--------------|------|----------|
| `sqlrec-lib-pvc` | 依赖 JAR | `LIB_STORAGE_SIZE`，默认 `128Gi` |
| `sqlrec-client-pvc` | Hadoop、Hive、Spark、容器 Java 客户端及配置 | `CLIENT_STORAGE_SIZE`，默认 `128Gi` |

PVC 名称可通过 `LIB_PVC_NAME`、`CLIENT_PVC_NAME` 设置。已有 PVC 的 StorageClass 和访问模式不能原地切换，脚本遇到不匹配会停止；切换存储方案时使用新的 PVC 名称并安排文件迁移。hostPath PV 使用 `Retain` 回收策略，名称可通过 `LIB_PV_NAME`、`CLIENT_PV_NAME` 设置。

RustFS 以单节点模式提供 S3 存储，部署脚本创建 JuiceFS 和 Milvus 所需的 bucket。数据卷统一使用集群默认 StorageClass，客户端/JAR 的 `STORAGE_CLASS` 不影响 RustFS。容量通过 `RUSTFS_DATA_STORAGE_SIZE`、`RUSTFS_LOG_STORAGE_SIZE` 设置，默认分别为 `128Gi`、`1Gi`。

### 资源同步

部署脚本会自动准备客户端、JAR 和配置；无需手动应用 PV/PVC 模板或单独准备 HMS 配置。已有资源更新后，可在项目根目录执行 `bash ./deploy/storage/sync.sh`。同步不会删除远端多余文件，同一命名空间内不要并发执行。

::: details 目录和同步细节

默认文件存放在 `deploy/data`；设置 `BASE_DIR` 后则存放在 `${BASE_DIR}/data`：

| 目录 | 内容 |
|------|------|
| `conf` | 生成的 Hadoop、Hive、Spark 配置 |
| `lib` | 依赖 JAR |
| `client` | 客户端、Java 和下载的归档文件 |
| `image-cache` | 手动保存的 Minikube 镜像缓存 |

`download_resource.sh` 自动创建 `conf/lib/client`；组件单独下载资源时也会创建目标目录，已有下载文件会被复用。

存储由 `deploy/storage/deploy.sh` 创建，无需手动应用 PV/PVC 模板。部署时先创建存储、生成配置，再由 HMS 部署脚本调用 `deploy/storage/sync.sh`，在启动 HMS 初始化 Job 前准备好卷内文件。无需单独执行 HMS 配置准备脚本。

共享模式仅上传运行所需的客户端、容器 Java 和 JAR，不上传下载归档、仅供宿主机使用的 Java 或 Minikube 工具。同步时检查远端实际文件的 SHA-256、文件类型和可执行属性，跳过一致的文件，更新新增或变化的文件；配置重新生成后参与相同检查。同步不会清理远端多余文件，也不代表所有组件部署步骤都完全幂等。

已有卷和客户端准备好后，可在项目根目录手动同步：

```bash
bash ./deploy/storage/sync.sh
```

同步使用临时 Pod 和 ConfigMap，结束时自动清理。同一命名空间内不要并发执行同步；默认同步 Pod 名为 `sqlrec-resource-sync`，已有同名 Pod 时脚本会停止。

关键配置文件包括 `core-site.xml`（文件系统/JuiceFS）、`hdfs-site.xml`（HDFS）和 `hive-site.xml`（Hive Metastore）。共享卷在容器内挂载到脚本计算的 `CLIENT_DIR`、`LIB_DIR` 路径。
:::

## 生产环境配置

已有集群部署流程可以复用，但生产环境仍需由 SRE 审查组件容量、持久化存储、网络、凭据和权限。当前默认部署包含单实例服务和 NodePort，应按实际运维方案调整。

### 依赖服务

| 服务 | 用途 |
|------|------|
| Kubernetes | 部署和管理模型训练、导出及服务 |
| PostgreSQL | 模型、服务、函数等元数据 |
| Hive Metastore | Hive 表元数据 |
| Flink SQL Gateway | Flink SQL 执行 |
| 分布式存储 | 模型文件和训练数据，默认使用 RustFS + JuiceFS |

Kafka、Redis、Milvus、Spark、Kyuubi、Jupyter 等按业务需求使用，其中 Kafka、Redis、Milvus 已包含在默认组件部署流程中。

### 脚本参数与容器配置

脚本读取 `deploy/env.sh`，可在执行前用同名环境变量覆盖它支持的参数，例如：

```bash
export NAMESPACE=dev
export SQLREC_VERSION=your-version
bash ./deploy/download_resource.sh
bash ./deploy/deploy_components.sh
```

需在下载资源和部署时保持命名空间、版本、路径和架构配置一致。主要参数如下：

| 参数 | 作用 |
|------|------|
| `NODE_IP` | NodePort 访问地址，应同时对部署机器和 Pod 可达 |
| `SQLREC_POSTGRESQL_USER` / `SQLREC_POSTGRESQL_PASSWORD` | SQLRec 元数据库凭据 |
| `HMS_POSTGRESQL_USER` / `HMS_POSTGRESQL_PASSWORD` | HMS 元数据库凭据 |
| `SQLREC_POSTGRESQL_PORT` / `HMS_POSTGRESQL_PORT` | 数据库 NodePort |
| `HMS_PORT` / `FLINK_SQL_GATEWAY_PORT` | HMS / Flink SQL Gateway NodePort；Gateway 端口也供 SQLRec 连接使用 |
| `SQLREC_THRIFT_PORT` / `SQLREC_REST_PORT` | JDBC/Beeline / REST NodePort，默认 `30000` / `30001` |
| `RUSTFS_ACCESS_KEY` / `RUSTFS_SECRET_KEY` | 共享 S3 凭据 |
| `DEPLOY_TIMEOUT` | 部署等待超时，默认 `3600` 秒 |

SQLRec 容器的 `META_DB_URL`、`HIVE_METASTORE_URI`、`FLINK_SQL_GATEWAY_ADDRESS` 等由 `deploy/sqlrec/sqlrec.yaml` 根据 `NODE_IP` 和对应端口生成；`MODEL_BASE_PATH` 在模板中固定为 `/user/sqlrec/models`。

使用 SRE 运维的已有依赖时，需要调整对应部署步骤和 YAML 中的连接配置。当前脚本没有“设置外部组件地址就自动跳过部署”的机制；仅导出 `META_DB_URL` 或 `HIVE_METASTORE_URI` 不会替换模板中的地址。单独运行 `deploy/sqlrec/deploy.sh` 也会先部署 PostgreSQL。

SQLRec 和 Spark 部署脚本会创建各自的 ServiceAccount，并通过 ClusterRoleBinding 授予集群级 `edit` 权限。生产环境应按所需资源和命名空间调整为受限 Role/RoleBinding。

### SQL 初始化

`deploy/sqlrec/deploy.sh` 会先等待 PostgreSQL 可连接，再执行 `deploy/sql/master.sql`，成功后才应用 SQLRec Deployment。无需在正常脚本部署前手动导入 SQL。

::: details 初始化重试参数和事务行为

| 参数 | 默认值 | 要求 |
|------|--------|------|
| `PSQL_MAX_ATTEMPTS` | `30` | 正整数，数据库连接最大尝试次数 |
| `PSQL_RETRY_INTERVAL` | `5` 秒 | 非负整数，连接重试间隔 |
| `PGCONNECT_TIMEOUT` | `10` 秒 | 正整数，单次连接超时 |

参数非法时在创建资源前报错。连接失败会按上表重试；建表 SQL 使用 `ON_ERROR_STOP=1` 和单个事务，SQL 错误会停止部署并回滚本次初始化，不会重复执行建表 SQL。已有表通过 `CREATE TABLE IF NOT EXISTS` 复用，该文件不负责已有表的结构迁移。
:::

## 验证与排障

以下命令在项目根目录执行；未设置 `NAMESPACE` 时使用默认值 `sqlrec`：

```bash
kubectl get pods -n "${NAMESPACE:-sqlrec}"
kubectl get pvc -n "${NAMESPACE:-sqlrec}"
bash ./bin/beeline.sh
```

| 现象 | 检查方法 |
|------|----------|
| 提示缺少或损坏的客户端/JAR | 先运行 `deploy/download_resource.sh`；如仍报错，移除损坏的对应文件或解压目录后重跑 |
| 共享 PVC 或同步 Pod 一直 Pending | `kubectl describe pvc <名称> -n <命名空间>`，确认 StorageClass 的 RWX 能力、容量和权限；使用 WaitForFirstConsumer 的类会在同步 Pod 创建后开始绑定 |
| PostgreSQL 连接重试耗尽 | 确认数据库 Pod 就绪，并检查 `NODE_IP`、NodePort、路由和凭据；SQL 错误应按 `psql` 输出修正 |
| 提示已有同步 Pod | 等待正在进行的同步完成；只有确认前一次同步已停止后，才清理残留 Pod |

部署后查看指标、Trace 和日志的方法见[可观测性](./observability.md)。
