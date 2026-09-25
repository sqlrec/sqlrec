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

## 项目介绍

一个支持使用SQL进行开发的推荐引擎，目标是让懂数据科学的人，包括数据分析师、数据工程师、后端开发等，都能快速搭建生产可用的推荐系统。系统架构参考下图，SQLRec将底层的组件访问、模型训练、推理等流程使用SQL封装，上层推荐业务逻辑仅使用SQL进行描述即可。

![system\_architecture](docs/public/sqlrec_arch.svg)

sqlRec有以下特点：

- 云原生，自带基于minikube的部署脚本，可以一键部署SQLRec系统和相关的依赖服务
- 扩展了SQL语法，让使用SQL描述推荐系统业务逻辑变得可能
- 基于calcite实现了一个高效的SQL执行引擎，可以满足推荐系统的实时性要求
- 基于已有的大数据生态，接入简单
- 易于扩展，可以自定义UDF、Table类型、Model类型

详细的资料参考[SQLRec用户手册](https://sqlrec.github.io/sqlrec)。

## 快速体验

直接运行无外部依赖的 Docker Demo：

```bash
docker run --rm -d --name sqlrec-demo \
  -p 30000:30000 \
  -p 30001:30001 \
  sqlrec/sqlrec-demo:latest
```

Demo 内置五个用户（每人三个兴趣类目）、五个类目（`pc`、`phone`、`book`、`sports`、`home`）和 25 个热门商品的 CSV 数据，各进程首次访问时会加载到内存。

也可以从宿主机直接运行容器中的 `cli.sh`，在 `sqlrec>` 提示符下执行以分号结束的 SQL：

```bash
docker exec -it sqlrec-demo /app/cli.sh
```

```sql
show tables;
select * from demo_user_interest_category;

cache table quick_start_user as
select cast(1000001 as bigint) as user_id;

call demo_rec(quick_start_user);
```

`quick_start_user` 是传给推荐函数的输入表，`CALL` 会返回推荐结果。按 `Ctrl+D` 退出 SQL 命令行。CLI 和 HTTP 服务的内存数据彼此独立。也可以从宿主机用 `1000001` 至 `1000005` 的用户 ID 调用内置推荐 API：

```bash
curl -X POST http://localhost:30001/api/v1/demo_rec \
  -H "Content-Type: application/json" \
  -d '{"data":{"user_info":[{"user_id":1000001}]}}'
```

API 会在内存中记录曝光并过滤已返回的商品。样例商品用完后，重启容器即可重置 Demo。

### 创建自己的表和函数

Docker Demo 会从 `SQL_SCHEMA_DIR` 目录中的 `.sql` 文件加载表、SQL 函数和 API。可以先在宿主机准备以下目录：

```text
sql/
├── table/hot_item.sql
├── function/recommend.sql
└── api/recommend.sql
```

在 `sql/table/hot_item.sql` 中定义表：

```sql
CREATE TABLE hot_item (
  item_id BIGINT,
  score FLOAT,
  PRIMARY KEY (item_id) NOT ENFORCED
) WITH (
  'connector' = 'filesystem'
);
```

在 `sql/function/recommend.sql` 中定义 SQL 函数：

```sql
CREATE OR REPLACE SQL FUNCTION recommend;

DEFINE INPUT TABLE user_info (
  user_id BIGINT
);

CACHE TABLE result AS
SELECT item_id, score
FROM hot_item
ORDER BY score DESC
LIMIT 10;

RETURN result;
```

在 `sql/api/recommend.sql` 中发布函数：

```sql
CREATE OR REPLACE API recommend WITH recommend;
```

停止内置 Demo，然后挂载完整的定义目录并重新启动 SQLRec：

```bash
docker stop sqlrec-demo

docker run --rm -d --name sqlrec-custom \
  -p 30000:30000 \
  -p 30001:30001 \
  -v "$(pwd)/sql:/workspace/sql:ro" \
  -e SQL_SCHEMA_DIR=/workspace/sql \
  sqlrec/sqlrec-demo:latest
```

本地元数据模式不允许通过 CLI 或 `/sql/v1` 执行 DDL。定义发生变化时，需要修改 SQL 文件并重启容器。Filesystem 表的数据只保存在内存中，仅适合 Demo 和测试。

写入数据并调用刚发布的 API：

```bash
curl -X POST http://localhost:30001/sql/v1 \
  -H "Content-Type: application/json" \
  -d '{"sqls":["insert into hot_item values (1001, 0.9), (1002, 0.8)"]}'

curl -X POST http://localhost:30001/api/v1/recommend \
  -H "Content-Type: application/json" \
  -d '{"data":{"user_info":[{"user_id":1000001}]}}'
```

打开 [http://localhost:30001/ui/static/index.html](http://localhost:30001/ui/static/index.html)，可以查看已加载的表、函数、API 和执行 DAG。体验完成后停止并删除 Demo：

```bash
docker stop sqlrec-custom
```

更多 CLI、数据写入和 API 调用示例请参考 [Docker 快速开始](https://sqlrec.github.io/sqlrec/docs/getting-started/docker)。

### 完整服务模式

与 Docker Demo 使用的本地元数据模式相比，完整服务模式主要增加了可持久化、可动态修改的元数据。用户可以通过 Beeline、JDBC 或其他 Hive Thrift 客户端执行并保存以下管理语句：

- `CREATE TABLE`、`CREATE SQL FUNCTION` 和 `CREATE API`；
- `CREATE MODEL`、`TRAIN MODEL` 和 `EXPORT MODEL`；
- `CREATE SERVICE` 及其他模型服务生命周期操作。

仓库内置的 Minikube 脚本主要用于开发和测试。系统要求和组件版本会随脚本迭代，请以当前 `deploy/` 配置为准。完整部署步骤及生产环境注意事项见[服务部署](https://sqlrec.github.io/sqlrec/docs/operations/deployment)。

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
