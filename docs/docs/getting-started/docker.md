# 快速开始

只需要 Docker 即可体验 SQLRec。Demo 已包含示例数据、推荐函数和 API，无需部署外部服务。

## 启动 Demo

```bash
docker run --rm -d --name sqlrec-demo \
  -p 30000:30000 \
  -p 30001:30001 \
  sqlrec/sqlrec-demo:latest
```

查看启动日志：

```bash
docker logs -f sqlrec-demo
```

服务启动后按 `Ctrl+C` 退出日志查看，容器仍在后台运行。

## 通过 API 调用推荐接口

```bash
curl -X POST http://localhost:30001/api/v1/demo_rec \
  -H "Content-Type: application/json" \
  -d '{"data":{"user_info":[{"user_id":1000001}]}}'
```

首次调用通常返回两条商品。响应中的 `data` 是结果行，包含 `user_id`、`item_id`、`item_name`、`rec_reason`、`req_time` 和 `req_id`；下面节选一条结果的部分字段，商品可能不同：

```json
{
  "data": [
    {"item_id": 1000001, "rec_reason": "user_category_interest_recall:book"}
  ]
}
```

可使用 `1000001` 至 `1000005` 的用户 ID。重复调用会过滤已推荐的商品；样例候选用完后，重启容器即可重置。

## 查看 UI

访问 [http://localhost:30001/ui/static/index.html](http://localhost:30001/ui/static/index.html)，查看表、函数、API 和执行 DAG。

## 停止 Demo

```bash
docker stop sqlrec-demo
```

启动时使用了 `--rm`，容器停止后会自动删除。

## 可选：进入 SQLRec CLI

Demo 运行期间，从宿主机打开 SQL 命令行：

```bash
docker exec -it sqlrec-demo /app/cli.sh
```

在 `sqlrec>` 提示符下输入以分号结束的 SQL：

```sql
show tables;
show functions;
show apis;
select * from demo_user_interest_category;

cache table quick_start_user as
select cast(1000001 as bigint) as user_id;

call demo_rec(quick_start_user);
```

`quick_start_user` 是推荐函数的输入表。按 `Ctrl+D` 退出 SQL 命令行。

::: tip Demo 数据范围
数据只保存在内存中。CLI 与 HTTP 服务使用各自的数据，CLI 中的修改不会影响 API；重新进入 CLI 会恢复初始数据，重启容器会重置 HTTP 服务的数据。

用 `INSERT` 增加记录，用 `UPDATE` 修改记录。要给 HTTP 服务写入测试数据，使用 `/sql/v1`；本地 CLI 的数据修改对它不可见。
:::

## 修改 Demo SQL

推荐逻辑位于 `sqlrec-demo/src/main/sql/quick_start/function/demo_rec.sql`。同一目录下的 `table/` 定义表，`api/` 将函数发布为 API，`data/` 保存初始 CSV 数据。

镜像通过 `SQL_SCHEMA_DIR=/app/sql` 加载定义。修改逻辑的写法见[编写推荐流程](../guides/recommendation-flow.md)；要加载自己的文件，按下面的步骤准备完整 SQL 目录。

## 管理本地 SQL 定义

本地文件模式在启动时加载 SQL 定义。不能通过 CLI 或 `/sql/v1` 执行 DDL；修改文件后需要重启容器。下面的示例定义一张热门商品表、一个推荐函数和一个 API。

### 准备 SQL 文件

在宿主机创建目录：

```bash
mkdir -p sql/table sql/function sql/api
```

将以下定义分别保存到三个文件中。

`sql/table/hot_item.sql`：

```sql
CREATE TABLE hot_item (
  item_id BIGINT,
  score FLOAT,
  PRIMARY KEY (item_id) NOT ENFORCED
) WITH (
  'connector' = 'filesystem'
);
```

`sql/function/recommend.sql`：

```sql
CREATE OR REPLACE SQL FUNCTION recommend;

DEFINE INPUT TABLE user_info (
  user_id BIGINT
);

CACHE TABLE result_table AS
SELECT item_id, score
FROM hot_item
ORDER BY score DESC
LIMIT 10;

RETURN result_table;
```

`sql/api/recommend.sql`：

```sql
CREATE OR REPLACE API recommend WITH recommend;
```

### 加载并调用

将整个目录挂载到容器，并指定 `SQL_SCHEMA_DIR`：

```bash
docker run --rm -d --name sqlrec-custom \
  -p 30000:30000 \
  -p 30001:30001 \
  -v "$(pwd)/sql:/workspace/sql:ro" \
  -e SQL_SCHEMA_DIR=/workspace/sql \
  sqlrec/sqlrec-demo:latest
```

`./sql` 应包含本次启动需要的全部定义。向 HTTP 服务写入数据，再调用推荐 API：

```bash
curl -X POST http://localhost:30001/sql/v1 \
  -H "Content-Type: application/json" \
  -d '{"sqls":["insert into hot_item values (1001, 0.9), (1002, 0.8)"]}'

curl -X POST http://localhost:30001/api/v1/recommend \
  -H "Content-Type: application/json" \
  -d '{"data":{"user_info":[{"user_id":1000001}]}}'
```

此 API 返回按 score 排序的热门商品，没有使用用户特征。推荐逻辑可继续在函数中扩展。

Filesystem 表只用于 Demo 或测试，写入不会回写文件；配置和数据限制见[内置 Connector](../reference/connectors/builtin-connectors.md)。

线上 serving 也可以将完整 SQL 目录随镜像或部署配置管理版本，更新时重新部署实例。如果需要在会话中直接执行并持久化 DDL，请按[服务部署](../operations/deployment.md)准备远程元数据环境，通过 Beeline 或 JDBC 连接。
