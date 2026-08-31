<h1 align="center">SQLRec</h1>

<p align="center">
  <a href="README.md">English</a> | 中文
</p>

<p align="center">
  <a href="https://github.com/sqlrec/sqlrec/blob/main/LICENSE">
    <img src="https://img.shields.io/github/license/sqlrec/sqlrec" alt="License">
  </a>
  <a href="https://github.com/sqlrec/sqlrec/stargazers">
    <img src="https://img.shields.io/github/stars/sqlrec/sqlrec" alt="Stars">
  </a>
  <a href="https://github.com/sqlrec/sqlrec/network/members">
    <img src="https://img.shields.io/github/forks/sqlrec/sqlrec" alt="Forks">
  </a>
  <a href="https://github.com/sqlrec/sqlrec/commits">
    <img src="https://img.shields.io/github/last-commit/sqlrec/sqlrec" alt="Last Commit">
  </a>
</p>

一个支持使用SQL进行开发的推荐引擎，目标是让懂数据科学的人，包括数据分析师、数据工程师、后端开发等，都能快速搭建生产可用的推荐系统。系统架构参考下图，SQLRec将底层的组件访问、模型训练、推理等流程使用SQL封装，上层推荐业务逻辑仅使用SQL进行描述即可。

![system\_architecture](docs/public/sqlrec_arch.svg)

sqlRec有以下特点：

- 云原生，自带基于minikube的部署脚本，可以一键部署SQLRec系统和相关的依赖服务
- 扩展了SQL语法，让使用SQL描述推荐系统业务逻辑变得可能
- 基于calcite实现了一个高效的SQL执行引擎，可以满足推荐系统的实时性要求
- 基于已有的大数据生态，接入简单
- 易于扩展，可以自定义UDF、Table类型、Model类型

详细的资料参考[SQLRec用户手册](https://sqlrec.github.io/sqlrec)。

## 快速开始

### 直接运行无外部依赖的 Docker Demo（推荐）

Demo 镜像已经内置 `demo_rec` API、SQL 函数和三张以 `demo_` 开头的 filesystem 表。数据直接写入进程内存，因此不需要 Redis、PostgreSQL、Hive Metastore、Flink、Kubernetes 或其他外部组件。

```bash
docker run --rm -d --name sqlrec-demo \
  -p 30000:30000 \
  -p 30001:30001 \
  sqlrec/sqlrec-demo:latest
```

服务启动后，进入 CLI，写入测试数据并调用推荐函数：

```bash
docker exec -it sqlrec-demo bash /app/cli.sh
```

```sql
insert into demo_user_interest_category values (1000001, 'pc', 100);
insert into demo_category_hot_item values
  ('pc', 1000001, 100),
  ('pc', 1000002, 90);
cache table quick_start_user as select cast(1000001 as bigint) as user_id;
call demo_rec(quick_start_user);
```

Demo 默认开启 SQL API。由于 CLI 与 HTTP 服务使用不同的内存数据，需要先向 HTTP 进程写入测试数据，再调用推荐 API：

```bash
curl -X POST http://localhost:30001/sql/v1 \
  -H "Content-Type: application/json" \
  -d @- <<'JSON'
{"sqls":["insert into demo_user_interest_category values (1000001, 'pc', 100)","insert into demo_category_hot_item values ('pc', 1000001, 100), ('pc', 1000002, 90)"]}
JSON

curl -X POST http://localhost:30001/api/v1/demo_rec \
  -H "Content-Type: application/json" \
  -d '{"data":{"user_info":[{"user_id":1000001}]}}'
```

可以打开 [http://localhost:30001/ui/static/index.html](http://localhost:30001/ui/static/index.html) 查看 UI。CLI 数据会在该进程退出时清除，HTTP 服务的数据会保留到容器停止。体验完成后执行 `docker stop sqlrec-demo`。

本地元数据模式不允许通过 CLI 或 SQL API 执行 DDL。表、函数和 API 等定义需要写入本地 SQL 文件，将其目录挂载到容器并通过 `SQL_SCHEMA_DIR` 指向该目录；也可以部署完整集群后直接执行 DDL。

表目录和更多 SQL 示例请参考[快速开始文档](https://sqlrec.github.io/sqlrec/docs/quick_start)。

### 完整服务部署（可选）

下面的步骤会部署持久化元数据及完整示例所需的外部数据、计算和模型基础设施。部署完成后，可以在集群中复现与上述 Docker Demo 完全相同的 quick-start 表、SQL 函数和 API。

SQLRec目前支持AMD64的Linux系统，后续会支持MacOS。注意，部署需要至少32GB的内存、256GB磁盘空间、可靠的互联网连接（如果使用加速器，注意使用tun模式）。

按下述命令部署SQLRec系统：

```bash
# 克隆 SQLRec 仓库
git clone https://github.com/sqlrec/sqlrec.git
cd ./sqlrec/deploy

# 部署 minikube
./deploy_minikube.sh

# 查看 Pod 状态，等待所有 Pod 就绪
alias kubectl="minikube kubectl --"
kubectl get pods --all-namespaces

# 下载部署资源
./download_resource.sh

# 部署 SQLRec 及依赖服务
./deploy_components.sh

# 查看 Pod 状态，等待所有 Pod 就绪
kubectl get pods --all-namespaces

# 连接 SQLRec，验证服务
cd ..
bash ./bin/beeline.sh
```

注意：

- 上述基于 minikube 的部署方案仅用于测试。生产环境需要先部署可靠的大数据基础设施，再参考 `deploy/` 下的脚本初始化数据库并部署 SQLRec。
- 如需重新部署，可以先执行 `minikube delete` 删除测试集群。
- Kyuubi、Jupyter 等组件默认不部署。如有需要，可在 `deploy/` 目录执行对应脚本，例如 `bash ./kyuubi/deploy.sh`。
- 可以在 `deploy/env.sh` 中自定义密码、网络端口等参数。

### 连接 SQLRec 服务

SQLRec 实现了 Hive Thrift 接口，可以使用 Beeline 连接：

```bash
bash ./bin/beeline.sh
```

### SQL 开发

执行 `bash ./bin/beeline.sh` 连接 SQLRec 服务。下面的表名、字段名、SQL 函数名和 API 名均与 `sqlrec/sqlrec-demo` 镜像中的 quick-start 示例一致；对应定义位于 [`sqlrec-demo/src/main/sql/quick_start/`](sqlrec-demo/src/main/sql/quick_start/)。

1. 创建三张 quick-start 表：

```sql
SET table.sql-dialect = default;

CREATE TABLE IF NOT EXISTS `demo_user_interest_category` (
  `user_id` BIGINT,
  `category` STRING,
  `score` FLOAT,
  PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
  'connector' = 'filesystem'
);

CREATE TABLE IF NOT EXISTS `demo_category_hot_item` (
  `category` STRING,
  `item_id` BIGINT,
  `score` FLOAT,
  PRIMARY KEY (item_id) NOT ENFORCED
) WITH (
  'connector' = 'filesystem'
);

CREATE TABLE IF NOT EXISTS `demo_exposure_item` (
  `user_id` BIGINT,
  `item_id` BIGINT,
  `bhv_time` BIGINT,
  PRIMARY KEY (item_id) NOT ENFORCED
) WITH (
  'connector' = 'filesystem'
);
```

这里特意沿用 demo 镜像的 `filesystem` connector，以确保示例语义一致；表数据保存在 SQLRec 服务进程内，集群部署所提供的持久化能力用于元数据。生产数据请按实际需求改用 Redis 等持久化 connector。

2. 写入测试数据：

```sql
INSERT INTO `demo_user_interest_category` VALUES
(1000001, 'pc', 100);

INSERT INTO `demo_category_hot_item` VALUES
('pc', 1000001, 100),
('pc', 1000002, 90);
```

3. 创建与 demo 镜像一致的 `demo_rec` SQL 函数：

```sql
create or replace sql function demo_rec;

define input table user_info(user_id bigint);

cache table exposed_item as
select item_id
from user_info join demo_exposure_item
on demo_exposure_item.user_id = user_info.user_id;

cache table cur_user_interest_category as
select category
from user_info join demo_user_interest_category
on demo_user_interest_category.user_id = user_info.user_id
limit 10;

cache table category_recall as
select
  item_id,
  'user_category_interest_recall:' || cur_user_interest_category.category as rec_reason
from cur_user_interest_category join demo_category_hot_item
on demo_category_hot_item.category = cur_user_interest_category.category
limit 300;

cache table dedup_category_recall as
call dedup(category_recall, exposed_item, 'item_id', 'item_id');

-- truncate to rec item num
cache table final_recall_item as
select item_id, rec_reason
from dedup_category_recall
limit 2;

-- gen rec meta data
cache table request_meta as
select
  user_info.user_id,
  cast(CURRENT_TIMESTAMP as BIGINT) as req_time,
  uuid() as req_id
from user_info;

-- gen final rec data
cache table final_rec_data as
select
  request_meta.user_id as user_id,
  item_id,
  cast('XXX' as VARCHAR) as item_name,
  rec_reason,
  request_meta.req_time as req_time,
  request_meta.req_id as req_id
from request_meta join final_recall_item on 1=1;

-- write exposed item to exposure table for deduplication
insert into demo_exposure_item
select user_id, item_id, req_time
from final_rec_data;

return final_rec_data;
```

上面的函数会按用户兴趣召回热门物品、过滤已曝光物品，并记录本次曝光。可以用以下 SQL 验证：

```sql
cache table quick_start_user as select cast(1000001 as bigint) as user_id;
call demo_rec(quick_start_user);
```

4. 将 SQL 函数暴露为同名 API：

```sql
create or replace api demo_rec with demo_rec;
```

### 推荐测试

获取 minikube 节点 IP，并调用 `demo_rec`：

```bash
MINIKUBE_NODE_IP=$(kubectl get node -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')
curl -X POST "http://${MINIKUBE_NODE_IP}:30001/api/v1/demo_rec" \
  -H "Content-Type: application/json" \
  -d '{"data":{"user_info":[{"user_id":1000001}]}}'
```

### 前端 UI

SQLRec 提供了用于监控和管理的 Web UI。可以访问 `http://<MINIKUBE_NODE_IP>:30001/ui/static/index.html`（将占位符替换为上一步获取的 minikube 节点 IP）。

前端UI可以让你：
- 查看SQL函数及其执行DAG（有向无环图）
- 浏览API配置
- 监控模型训练状态和检查点
- 查看服务统计信息和指标

## 性能测试

性能测试基于 MovieLens-1M 数据集，脚本位于 `benchmark/movielens/` 目录。执行以下命令初始化测试环境并运行压测：

```bash
cd benchmark/movielens
bash init.sh
bash benchmark.sh
```

默认的测试数据和推荐流程如下：

- MovieLens-1M 数据集：6040 个用户、3706 部电影、约 100 万条评分记录
- 测试 `main_rec` 推荐流程，包含 4 路召回：全局高热、用户兴趣类目高热、ItemCF 和 Milvus 向量检索，每路最多召回 300 条
- 向量维度为 64；本测试未启用召回模型服务，每次请求通过 `random_vec` 生成用户向量
- 召回后进行近 1 小时曝光去重、`rank_fun_simple` 排序和类目打散，最终返回 10 条结果；推荐日志异步写入 Kafka，同时记录曝光结果
- 单 SQLRec 实例部署；先以 1 线程、1 连接预热 10 秒，再以 10 线程、10 连接压测 30 秒

在 AMD Ryzen 5600H、32GB DDR4 内存、Debian 12 和 Minikube 环境下的测试结果如下：

```
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     6.73ms    3.16ms  90.29ms   94.46%
    Req/Sec   151.20     16.58   191.00     73.67%
  45231 requests in 30.02s, 87.90MB read
Requests/sec:   1506.47
Transfer/sec:      2.93MB
```

## 路线图

### 1.0版本什么时候发布

1.0之前版本都是beta版本，不建议线上使用，不保证接口兼容性。目前无规划发布时间，将在下述功能完善后发布：

- 完善的单元测试、集成测试、效果测试覆盖
- 优化代码质量，目前仍很多细节要打磨
- 支持降级和超时配置
- 完善的版本管理方法，可以方便回滚到之前的版本
- metric监控系统完善
- c++模型serving

### 后续功能规划

- 前端UI，用于查看当前执行DAG、SQL代码、统计信息等
- 进一步优化SQL语法兼容性、运行性能
- 更多开箱可用的UDF、模型等
- 支持更多的外部数据源，比如JDBC、MongoDB等
- Tensorboard可视化模型训练过程
- GPU训练、推理支持
- 支持认证、鉴权
- 最佳实践教程，包括搜索、推荐等

