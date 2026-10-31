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

HDFS 使用单 NameNode、单 DataNode，并绑定到 `NODE_IP` 对应节点；部署脚本等待节点就绪后执行真实读写检查。重复部署通过卷内 `current/VERSION` 判断初始化状态，保留已有元数据。ARM 环境的 Hadoop 下载包按 `CONTAINER_ARCH` 选择；旧部署需重新执行 `deploy/hadoop/init.sh`，更新 Python/PyArrow 所需的 ARM 原生库，再运行 HDFS 部署脚本。

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

直接启动 SQLRec 时，`META_DB_URL`、`HIVE_METASTORE_URI`、`FLINK_SQL_GATEWAY_ADDRESS` 和 `TRACE_ENDPOINT` 默认均为空；使用远程元数据时需显式设置前两项，Gateway 转发和 Trace 在地址为空时关闭。开发集成测试通过 `bin/test.sh` 根据 `DEFAULT_TEST_IP`、`NODE_IP` 或 Minikube 地址设置组件连接默认值，也可显式覆盖各组件地址。`DEFAULT_TEST_IP` 配置默认也为空，供 Java 测试和示例 SQL 使用；直接启动服务时，组件地址不会根据它自动生成，仍需分别配置。

在仓库根目录运行集成测试：

```bash
# 使用已有环境；其余参数原样传递给 Maven
bash bin/test.sh -pl sqlrec-core -am

# 完整部署验证和全部 benchmark（也支持 --all）
bash bin/test.sh -all
```

`-all` 首先通过 `deploy/cache_images.sh save` 缓存已有 `minikube` 环境的镜像，缓存命令成功后执行 `minikube -p minikube delete` 删除已有环境，再通过 `deploy_minikube.sh` 重建集群并加载镜像缓存。随后下载部署资源、部署默认组件，以及集成测试需要的 MongoDB、Redis Cluster 和 GrowthBook；等待 Flink 和工作负载就绪，并通过 Beeline 执行 `SELECT 1` 验证 SQLRec 连接后，运行 Maven 集成测试。最后执行 SQL 性能 benchmark 的初始化和压测，安装共享 Python 依赖，运行 GrowthBook UDF 集成测试（通过 API 创建实验、feature 和 SDK 连接，见 `benchmark/growthbook/README.md`），再运行 Hugging Face 小模型集成测试（见 `benchmark/huggingface/README.md`），覆盖文本分类、文本生成、文本 embedding 和图片 embedding；随后准备并上传模型质量数据集，运行全部适用模型组合的完整验证集评估（`--model all --dataset all --full-eval`）。任一步骤失败立即停止；缓存命令失败时不会删除已有环境。

此模式需要已有可访问的 Minikube 环境以保存镜像，并会删除该集群及其节点动态卷数据；宿主机的镜像缓存和客户端目录保留。它使用现有部署脚本和配置的发布镜像，不构建本地源码镜像。运行需要部署权限和外网访问；Maven 使用当前终端的 JDK，Hive/Hadoop 客户端的 Java 环境在子进程中配置。GrowthBook、Hugging Face 和模型质量报告分别写入 `benchmark/growthbook/runs/`、`benchmark/huggingface/runs/`、`benchmark/model_quality/runs/`，SQL 压测结果输出到终端。不带 `-all` 时不部署环境，也不运行 benchmark。测试地址优先使用 `DEFAULT_TEST_IP`，其次使用 `NODE_IP`，均未设置时从 Minikube 获取；`HADOOP_CONF_DIR` 可显式覆盖，默认路径跟随 `BASE_DIR` 和 `HADOOP_VERSION`。

使用 SRE 运维的已有依赖时，需要调整对应部署步骤和 YAML 中的连接配置。当前脚本没有“设置外部组件地址就自动跳过部署”的机制；仅导出 `META_DB_URL` 或 `HIVE_METASTORE_URI` 不会替换模板中的地址。单独运行 `deploy/sqlrec/deploy.sh` 也会先部署 PostgreSQL。

SQLRec 和 Spark 部署脚本会创建各自的 ServiceAccount，并通过 ClusterRoleBinding 授予集群级 `edit` 权限。生产环境应按所需资源和命名空间调整为受限 Role/RoleBinding。

### 执行变量与结果参数

以下配置仅从服务进程环境读取，均默认开启；SQL `SET` 和请求 `params` 不能覆盖它们。

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `ENABLE_VARIABLE_ENV_FALLBACK` | `true` | `getVariable()` 在执行变量不存在（`null`）时读取同名环境变量；显式空字符串不回退 |
| `EXECUTE_RESULT_INCLUDE_PARAMS` | `true` | SQL 和函数 REST API 的执行结果包含执行变量 `params`；关闭后 JSON 省略该字段，不影响执行变量的使用 |

设置对应环境变量为 `false` 并重启服务即可关闭。环境变量回退不会将读取的值写入执行变量，`get_variables()` 和响应 `params` 仅包含显式执行变量。删除显式变量后，后续读取会再次回退到环境变量。开启回退时，SQL 可通过变量名读取服务进程中的环境配置。

### 入口认证

以下配置统一定义在 `SqlRecConfigs.java`，从服务进程的环境变量读取。直接启动服务时使用下表默认值；仓库部署脚本通过 `deploy/env.sh` 将默认值覆盖为 `AUTH_ENABLED=true`、`AUTH_USERNAME=sqlrec`、`AUTH_PASSWORD=abc123456`。默认凭证用于开发环境，可修改 `deploy/env.sh` 或用同名环境变量覆盖：

| 环境变量 | 服务默认值 | 说明 |
|----------|--------|------|
| `AUTH_ENABLED` | `false` | 开启 HTTP Basic 和 Thrift/JDBC SASL PLAIN 认证 |
| `AUTH_USERNAME` | 空字符串 | 共享用户名，开启认证时必填，不能包含冒号或控制字符 |
| `AUTH_PASSWORD` | 空字符串 | 共享密码，开启认证时必填，不能包含控制字符 |

普通部署和 demo 部署都将这三个配置直接写入容器环境变量；渲染后的 YAML 包含账号密码。修改配置后重新运行部署脚本，Deployment 会根据环境变量变更自动更新 Pod。凭证合法性由服务端在启动时校验。`AUTH_ENABLED` 使用 `true` 或 `false`。

```bash
AUTH_USERNAME=alice AUTH_PASSWORD='replace-with-your-password' bash deploy/sqlrec/deploy.sh
# 兼容旧的匿名环境；客户端也需设置 AUTH_ENABLED=false
AUTH_ENABLED=false bash deploy/sqlrec/deploy.sh
```

可以把这些配置和其他密钥统一放到 `/etc/sqlrec/sqlrec.env` 等自己的环境配置文件中：

```bash
export AUTH_ENABLED=true
export AUTH_USERNAME=sqlrec
export AUTH_PASSWORD='replace-with-your-password'
```

启动前设置 `export SQLREC_ENV_CONF=/etc/sqlrec/sqlrec.env`，`bin/server.sh` 会加载该文件。Kubernetes 部署时，将文件挂载到 SQLRec 容器内并在容器中设置此变量，也可以直接配置上述三个容器环境变量。修改后重启服务；开启认证但凭证为空或格式不合法时，启动失败。SQL `SET` 和请求 `params` 不能覆盖认证配置。

- **HTTP 和 UI**：除 `GET /metrics` 外，所有请求均要求 HTTP Basic；缺少或错误凭证返回 `401` 和 `WWW-Authenticate`。UI 使用浏览器原生账号密码提示框；浏览器缓存凭证，暂不提供应用内退出登录。
- **指标**：`GET /metrics` 始终匿名放行，包括开启认证时。Prometheus 无需增加账号密码配置。
- **Thrift/JDBC**：开启认证时使用 `jdbc:hive2://<host>:<port>/default` 和 SASL PLAIN；关闭时使用 `;auth=noSasl`。`bin/beeline.sh` 自动读取 `deploy/env.sh` 的地址、认证开关和凭证，通过同名环境变量覆盖配置。脚本将 `-e`、`-f` 等执行参数原样传给 Beeline，不解析命令行连接或凭证参数；连接设置使用 `NODE_IP`、`SQLREC_THRIFT_PORT`、`AUTH_USERNAME` 和 `AUTH_PASSWORD`。脚本不读取 `SQLREC_ENV_CONF`。错误凭证在 SASL 握手阶段被拒绝，本地凭证不会转发给 Flink Gateway。
- **本地 CLI**：直接运行引擎，继续依赖操作系统的访问控制。

Beeline 示例：

```bash
bash bin/beeline.sh -e 'SELECT 1'
AUTH_USERNAME=alice AUTH_PASSWORD='my-password' bash bin/beeline.sh -f query.sql
NODE_IP=other-host SQLREC_THRIFT_PORT=10000 bash bin/beeline.sh
AUTH_ENABLED=false bash bin/beeline.sh            # 连接关闭认证的服务
```

凭证通过 Beeline 原生 `-n/-p` 参数传入。Hive Beeline 3.1.3 会再次按空格拆分凭证，应使用不含空格的用户名和密码。

HTTP 调用示例（`--user sqlrec` 会提示输入密码，用户名取实际配置）：

```bash
curl --user sqlrec -X POST http://localhost:30001/sql/v1 \
  -H 'Content-Type: application/json' \
  -d '{"sqls":["SHOW TABLES"]}'

curl http://localhost:30001/metrics
```

开启认证时，SQL 和推荐 API 的 POST 请求必须使用 `Content-Type: application/json`，否则返回 `415`。Java 调用方可使用 `SqlRecApiClient.callFunctionApi(url, data, params, username, password)` 或 `callSqlApi(url, sqls, params, username, password)`；凭证仅放入 HTTP Header，不进入执行参数。原有方法继续匿名调用，不会自动把本机入口凭证发送给远端。

所有认证通过的客户端拥有相同能力，不区分角色或资源权限；仅提供在线推荐服务时，可关闭不需要的 REST SQL、UI 和 Thrift 入口。HTTP Basic 和 Thrift SASL PLAIN 不提供传输加密，部署时通过 HTTPS、TLS 隧道等保护连接。

#### Python / PyHive

安装纯 Python SASL 依赖，无需系统 `libsasl`：

```bash
pip install 'PyHive[hive_pure_sasl]'
```

开启认证后，使用 `auth="CUSTOM"`，账号密码对应服务端的 `AUTH_USERNAME` 和 `AUTH_PASSWORD`：

```python
from pyhive import hive

with hive.Connection(
    host="localhost",
    port=30000,
    username="sqlrec",
    password="your-password",
    auth="CUSTOM",
    database="default",
) as connection:
    with connection.cursor() as cursor:
        cursor.execute("SELECT 1")
        print(cursor.fetchall())
```

关闭认证时，改用 `auth="NOSASL"` 并省略 `password`。服务端支持 Hive CLI 协议 V6–V10，按客户端请求协商版本，兼容 PyHive 的 V6 请求。密码在 SASL 握手中校验，`OpenSession` 不要求重复携带密码。

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
