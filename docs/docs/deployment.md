# 服务部署

本文档介绍如何部署 SQLRec 系统。

## 系统要求

SQLRec 目前支持 AMD64 的 Linux 系统，后续会支持 MacOS。

**硬件要求**：
- 内存：至少 32GB
- 磁盘：至少 256GB
- 网络：可靠的互联网连接（如果使用加速器，注意使用 tun 模式）

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
kubectl get pod --ALL

# download resource
./download_resource.sh

# deploy sqlrec and dependencies services
./deploy_components.sh

# verify pod status, wait all pod ready
kubectl get pod --ALL

# verify sqlrec service
cd ..
bash ./bin/beeline.sh
```

**注意事项**：
- 上述基于 Minikube 的部署方案仅用于测试
- 如果需要重新部署，可以先通过 `minikube delete` 删除集群
- 有一些组件没有默认部署，比如 Kyuubi、Jupyter 等，如果需要，可以在 deploy 目录执行对应的部署脚本
- 可以在 `env.sh` 自定义密码、网络端口等参数

## 生产环境部署

生产环境需要先部署可靠的大数据基础设施，然后参考 deploy 下的脚本初始化数据库、部署 SQLRec Deployment。

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
| `sqlrec-lib-pv` | 存储依赖 JAR 包（JuiceFS Hadoop JAR 等） | 128Gi |
| `sqlrec-client-pv` | 存储客户端组件（Hadoop、Hive、Spark、Java） | 128Gi |

**Hadoop 配置文件要求**：

SQLRec 启动时需要加载 Hadoop 配置，启动脚本 `bin/sqlrec` 如下：

```bash
#!/bin/bash
set -ex

export PATH=$PATH:${HADOOP_HOME}/bin
export HADOOP_CLASSPATH=`hadoop classpath`
export CLASSPATH=$CLASSPATH:${HADOOP_CLASSPATH}
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-${HADOOP_HOME}/etc/hadoop}

java -cp ./*:${CLASSPATH} com.sqlrec.frontend.Main
```

**关键配置文件**：

| 文件 | 说明 | 必需配置项 |
|------|------|------------|
| `core-site.xml` | Hadoop 核心配置 | `fs.defaultFS`、JuiceFS 相关配置 |
| `hdfs-site.xml` | HDFS 配置 | 副本数、块大小等 |
| `hive-site.xml` | Hive 配置 | `hive.metastore.uris` |

### SQLRec 服务配置

SQLRec 服务通过 Kubernetes Deployment 部署，主要配置项如下：

**必需环境变量**：

| 环境变量 | 说明 |
|----------|------|
| `NAMESPACE` | Kubernetes 命名空间 |
| `MODEL_BASE_PATH` | 模型存储基础路径 |
| `META_DB_URL` | PostgreSQL 连接 URL |
| `META_DB_USER` | PostgreSQL 用户名 |
| `META_DB_PASSWORD` | PostgreSQL 密码 |
| `HIVE_METASTORE_URI` | Hive Metastore Thrift URI |
| `FLINK_SQL_GATEWAY_ADDRESS` | Flink SQL Gateway 地址 |
| `FLINK_SQL_GATEWAY_PORT` | Flink SQL Gateway 端口 |

**服务端口**：

| 端口 | 服务 | 说明 |
|------|------|------|
| 30300 | Thrift Server | JDBC/Beeline 连接端口 |
| 30301 | REST Server | REST API 端口 |
| 30302 | Debug | 远程调试端口 |

**Kubernetes 权限**：

SQLRec 需要以下 Kubernetes 权限来管理模型训练和服务部署：

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
# 创建数据库
psql -c "CREATE DATABASE sqlrec;"

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
envsubst < deploy/sqlrec/sqlrec.yaml | kubectl apply -f - -n ${NAMESPACE}
```

8. **验证部署**

```bash
# 检查 Pod 状态
kubectl get pod -n ${NAMESPACE}

# 连接测试
bash ./bin/beeline.sh
```

## 镜像构建

SQLRec 提供了构建脚本 `bin/build_docker.sh` 用于构建 Docker 镜像。

**构建步骤**：

**重要**：必须在项目根目录执行构建脚本。

```bash
# 进入项目根目录
cd /path/to/sqlrec

# 执行构建脚本
bash ./bin/build_docker.sh
```

**构建的镜像**：

| 镜像 | Dockerfile | 说明 |
|------|------------|------|
| `sqlrec/sqlrec:${VERSION}` | `docker/Dockerfile` | SQLRec 服务镜像 |
| `sqlrec/tzrec:${VERSION}-cpu` | `docker/sqlrec-model-tzrec.Dockerfile` | 模型训练/推理镜像（CPU 版本） |

**Minikube 环境**：

如果在 Minikube 环境中部署，构建脚本会自动配置 Minikube 的 Docker 环境，使构建的镜像可以直接被 Minikube 使用：

```bash
if command -v minikube >/dev/null 2>&1; then
  eval $(minikube -p minikube docker-env)
fi
```

**手动构建**：

如果需要手动构建镜像：

```bash
# 进入项目根目录
cd /path/to/sqlrec

# 构建 SQLRec 服务镜像
docker build -t sqlrec/sqlrec:0.1.0 -f ./docker/Dockerfile .

# 构建模型镜像
docker build -t sqlrec/tzrec:0.1.0-cpu -f ./docker/sqlrec-model-tzrec.Dockerfile .
```
