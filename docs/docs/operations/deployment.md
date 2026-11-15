# 服务部署

部署分为两步：准备 Kubernetes 集群，再部署 SQLRec 和依赖组件。Minikube 和已有集群使用相同的 `download_resource.sh`、`deploy_components.sh`，客户端文件的存储方式由 `STORAGE_MODE` 决定。

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
5. 如果部署机器和集群节点架构不同，在下载资源前设置 `CONTAINER_ARCH=amd64` 或 `arm64`，用于选择容器内 Java 和 Hadoop 原生库。它不会改变部署机器使用的 Java 架构，也不会自动统一集群中不同节点的架构。

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

`deploy_components.sh` 默认部署 PostgreSQL、RustFS/JuiceFS、Hadoop 配置、HDFS、Spark 配置、HMS、Flink SQL Gateway、SQLRec，以及 Kafka、Redis、Milvus。Milvus 位于 `${NAMESPACE}-milvus` 命名空间。MongoDB、Kyuubi、Jupyter、监控等组件通过各自脚本单独启用。默认文件系统仍为 `jfs://myjfs`，HDFS 使用 `hdfs://myhdfs` 显式访问。

HDFS 使用单 NameNode、单 DataNode，部署到 `NODE_IP` 对应节点；重复部署保留已有元数据。需要更高容量或可用性时，应单独规划 HDFS 集群。

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

本地资源默认位于 `deploy/data`，配置 `BASE_DIR` 后位于 `${BASE_DIR}/data`：`conf/` 保存生成的配置，`lib/` 保存 JAR，`client/` 保存客户端及 Java。共享模式同步运行所需文件，跳过未变化的文件。

```bash
bash ./deploy/storage/sync.sh
```

## 生产环境配置

已有集群部署流程可以复用，但生产环境仍需由 SRE 审查组件容量、持久化存储、网络、凭据和权限。当前默认部署包含单实例服务和 NodePort，应按实际运维方案调整。

### 依赖服务

| 服务 | 用途 |
|------|------|
| Kubernetes | 部署和管理模型训练、导出及服务 |
| PostgreSQL | 模型、服务、函数等元数据 |
| Hive Metastore | Hive 表元数据 |
| Flink SQL Gateway | 可选；执行需要转发的 Flink SQL/RPC |
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

需在下载资源和部署时保持命名空间、版本、路径和架构配置一致。主要脚本参数和容器配置如下：

| 参数 | 作用 |
|------|------|
| `NODE_IP` | NodePort 访问地址，应同时对部署机器和 Pod 可达 |
| `SQLREC_POSTGRESQL_USER` / `SQLREC_POSTGRESQL_PASSWORD` | SQLRec 元数据库凭据 |
| `HMS_POSTGRESQL_USER` / `HMS_POSTGRESQL_PASSWORD` | HMS 元数据库凭据 |
| `SQLREC_POSTGRESQL_PORT` / `HMS_POSTGRESQL_PORT` | 数据库 NodePort |
| `HMS_PORT` | HMS NodePort |
| `FLINK_SQL_GATEWAY_ADDRESS` | Gateway 主机地址；部署时默认 `NODE_IP`，空地址禁用转发 |
| `FLINK_SQL_GATEWAY_PORT` | Gateway NodePort 和 SQLRec 连接端口，默认 `30018` |
| `FLINK_SQL_GATEWAY_CONNECT_TIMEOUT` | SQLRec 容器运行时配置；连接和 RPC 读取超时，默认 `600000` 毫秒（10 分钟） |
| `SQLREC_THRIFT_PORT` / `SQLREC_REST_PORT` | JDBC/Beeline / REST NodePort，默认 `30000` / `30001` |
| `RUSTFS_ACCESS_KEY` / `RUSTFS_SECRET_KEY` | 共享 S3 凭据 |
| `DEPLOY_TIMEOUT` | 部署等待超时，默认 `3600` 秒 |

SQLRec 容器的 `META_DB_URL` 和 `HIVE_METASTORE_URI` 由 `deploy/sqlrec/sqlrec.yaml` 根据 `NODE_IP` 和对应端口生成；`MODEL_BASE_PATH` 在模板中固定为 `/user/sqlrec/models`。

直接启动 SQLRec 时，远程元数据模式需设置 `META_DB_URL`、`HIVE_METASTORE_URI`；`FLINK_SQL_GATEWAY_ADDRESS`、`TRACE_ENDPOINT` 默认空值，分别关闭转发和 Trace。文件模式改用 `SQL_SCHEMA_DIR`。

客户端连接见 [CLI、Beeline 与 Python](../guides/clients.md)，源码环境和测试见[开发与测试](../development/testing.md)。

使用 SRE 运维的已有依赖时，需要调整对应部署步骤和 YAML 中的连接配置。当前脚本没有“设置外部组件地址就自动跳过部署”的机制；仅导出 `META_DB_URL` 或 `HIVE_METASTORE_URI` 不会替换模板中的地址。单独运行 `deploy/sqlrec/deploy.sh` 也会先部署 PostgreSQL。

SQLRec 和 Spark 部署脚本会创建各自的 ServiceAccount，并通过 ClusterRoleBinding 授予集群级 `edit` 权限。生产环境应按所需资源和命名空间调整为受限 Role/RoleBinding。

### 执行变量与结果参数

以下配置仅从服务进程环境读取，均默认开启；SQL `SET` 和请求 `params` 不能覆盖它们。

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `ENABLE_VARIABLE_ENV_FALLBACK` | `true` | 执行变量不存在（`null`）时，SQL 读取同名环境变量；显式空字符串不回退 |
| `EXECUTE_RESULT_INCLUDE_PARAMS` | `true` | SQL 和函数 REST API 的执行结果包含执行变量 `params`；关闭后 JSON 省略该字段，不影响执行变量的使用 |

设置对应环境变量为 `false` 并重启服务即可关闭。环境变量回退不会将读取的值写入执行变量，`get_variables()` 和响应 `params` 仅包含显式执行变量。删除显式变量后，后续读取会再次回退到环境变量。开启回退时，SQL 可通过变量名读取服务进程中的环境配置。

### 入口认证

直接启动服务默认关闭认证；仓库部署脚本默认开启，开发凭证为 `sqlrec` / `abc123456`。部署前用环境变量覆盖：

| 环境变量 | 服务默认值 | 说明 |
| --- | --- | --- |
| `AUTH_ENABLED` | `false` | 开启 HTTP Basic 和 JDBC/Thrift SASL PLAIN |
| `AUTH_USERNAME` | 空字符串 | 逗号分隔的用户名 |
| `AUTH_PASSWORD` | 空字符串 | 与用户名按位置配对的密码 |

```bash
AUTH_ENABLED=true AUTH_USERNAME=alice AUTH_PASSWORD='replace-with-your-password' \
  bash deploy/sqlrec/deploy.sh
```

多个账号可设置 `AUTH_USERNAME=alice,bob`、`AUTH_PASSWORD=alice-secret,bob-secret`。两份列表长度须一致，每项非空白，用户名不能重复或包含冒号。逗号不能转义，空格属于凭证，控制字符不允许；不合法的配置会导致启动失败。

配置可直接写入容器环境，也可放入由 `bin/server.sh` 通过 `SQLREC_ENV_CONF` 加载的环境文件。修改后重启或重新部署，SQL `SET` 和请求 `params` 不能覆盖。部署模板将凭证写入容器环境变量，渲染后的 YAML 也包含凭证。

- HTTP 和 UI 使用 Basic 认证，缺少或错误凭证返回 `401`；`GET /metrics` 仍匿名开放。
- 所有认证客户端拥有相同能力，尚无角色或资源级权限控制。
- Basic 和 SASL PLAIN 不加密传输，需要时使用 HTTPS 或 TLS 隧道保护连接。

Beeline、PyHive 等客户端示例见[客户端连接](../guides/clients.md)。

### Gateway 转发配置

部署脚本默认安装 Flink SQL Gateway，并配置 SQLRec 的连接地址。Gateway 仅供需要 Flink 执行的请求使用；本地 SQL、[元数据查询](../reference/sql.md#元数据查询)（如 `SHOW CREATE TABLE`、`DESCRIBE`）、JDBC 元数据和持久表、库、UDF 的纯元数据 DDL 不依赖 Gateway。

默认配置无需调整。连接外部 Gateway 时，设置上表中的地址和端口，使用该服务的 HiveServer2/Thrift 接口。将地址设为空并重新部署 SQLRec，可关闭远程转发；这不会跳过默认 Flink 组件部署或删除已有组件。

```bash
export FLINK_SQL_GATEWAY_ADDRESS=""
bash ./deploy/sqlrec/deploy.sh
```

SQL 执行范围见[架构说明](../reference/architecture.md#gateway-转发边界)，SET/RESET 行为见[SQL 参考](../reference/sql.md#set)。

### 本地 SQL 文件元数据

| 环境变量 | 默认值 | 说明 |
|----------|--------|------|
| `SQL_SCHEMA_DIR` | 空字符串 | 非空时从该目录递归加载 SQL 定义；为空时使用远程元数据 |
| `SQL_SCHEMA_DDL_ENABLED` | `false` | 仅在文件模式下开放库、表、Java UDF、SQL 函数和 API 的内存 DDL |

在容器环境或 `SQLREC_ENV_CONF` 中配置，修改后重启服务。该开关不支持会话 `SET`。开启后所有会话共享当前进程的变更，SQL 文件保持不变，重启恢复文件定义；不同实例之间不共享变更。`FLUSH` 不撤销内存修改。模型和 Service 等生命周期命令保持原有限制。语句范围见[元数据 DDL](../reference/sql.md#元数据-ddl)。

### SQL 初始化

`deploy/sqlrec/deploy.sh` 等待 PostgreSQL 可连接后，执行 `deploy/sql/master.sql`，再部署 SQLRec。无需手动导入 SQL。已有表会被复用，该文件不负责已有表的结构迁移。

连接等待不足时可调整 `PSQL_MAX_ATTEMPTS`（默认 30）、`PSQL_RETRY_INTERVAL`（默认 5 秒）、`PGCONNECT_TIMEOUT`（默认 10 秒）。SQL 初始化错误会停止部署并回滚本次初始化事务，修正报错后再重试。

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
