# 快速开始

只需要 Docker 即可体验 SQLRec。Demo 镜像包含快速开始所需的表、SQL 函数和 API；示例表使用 `filesystem` connector，数据保存在进程内存中，不依赖 Redis、PostgreSQL、Hive Metastore、Flink 或 Kubernetes。

## 启动 Demo

```bash
docker run --rm -d --name sqlrec-demo \
  -p 30000:30000 \
  -p 30001:30001 \
  sqlrec/sqlrec-demo:latest
```

通过日志确认服务已经启动：

```bash
docker logs -f sqlrec-demo
```

看到服务启动完成后按 `Ctrl+C` 退出日志查看，容器仍会在后台运行。

## 进入 SQLRec CLI

执行下面的命令进入容器内置的 SQLRec CLI，其使用方式与通过 beeline 连接 SQLRec 基本一致：

```bash
docker exec -it sqlrec-demo sh /app/cli.sh
```

可以先查看 Demo 已经加载的对象：

```sql
show tables;
show functions;
show apis;
```

## 写入测试数据

quick-start 的 filesystem 表启动时为空。在 CLI 中写入一条用户偏好和五条热门商品：

```sql
insert into user_interest_category1 values
  (1000001, 'pc', 100);

insert into category1_hot_item values
  ('pc', 1000001, 100),
  ('pc', 1000002, 90),
  ('pc', 1000003, 80),
  ('pc', 1000004, 70),
  ('pc', 1000005, 60);

select * from user_interest_category1;
select * from category1_hot_item;
```

数据只存在于当前 CLI 进程的内存中，退出 CLI 后会被清除。

## 获取推荐结果

Demo 已经定义 `test_rec` SQL 函数。继续在同一个 CLI 会话中创建输入表并调用函数：

```sql
cache table quick_start_user as
select cast(1000001 as bigint) as id;

call test_rec(quick_start_user);
```

函数会返回两条热门商品及其推荐理由、请求时间和请求 ID。曝光结果会写入当前进程内存中的 `exposure_item` 表；再次调用时，`test_rec` 会使用这些记录进行去重。

## 通过 API 调用推荐接口

容器中的 CLI 和 HTTP 服务运行在不同进程中，因此它们各自维护独立的 filesystem 内存数据。Demo 镜像默认开启 SQL API，先通过 `/sql/v1` 向 HTTP 服务进程写入测试数据：

```bash
curl -X POST http://localhost:30001/sql/v1 \
  -H "Content-Type: application/json" \
  -d @- <<'JSON'
{
  "sqls": [
    "insert into user_interest_category1 values (1000001, 'pc', 100)",
    "insert into category1_hot_item values ('pc', 1000001, 100), ('pc', 1000002, 90), ('pc', 1000003, 80)"
  ]
}
JSON
```

然后调用 `test_rec` 推荐 API：

```bash
curl -X POST http://localhost:30001/api/v1/test_rec \
  -H "Content-Type: application/json" \
  -d '{"data":{"user_info":[{"id":1000001}]}}'
```

接口会返回 `test_rec` 的推荐结果。通过 SQL API 写入的测试数据和推荐产生的曝光数据都会保留在 HTTP 服务进程内，直到容器停止。

## 查看 UI

浏览器访问 [http://localhost:30001/ui/static/index.html](http://localhost:30001/ui/static/index.html)，可以查看表、API、SQL 函数及其执行 DAG。

## Demo 目录结构

`SQL_SCHEMA_DIR` 统一设置为 `/app/sql`，SQLRec 会递归加载两个示例目录。两套示例使用不同的表、函数和 API 标识符，互不冲突：

```text
sqlrec-demo/src/main/sql/
├── quick_start/
│   ├── api/test_rec.sql
│   ├── function/test_rec.sql
│   └── table/
│       ├── category1_hot_item.sql
│       ├── exposure_item.sql
│       └── user_interest_category1.sql
└── movielens/
    ├── api/
    ├── function/
    ├── model/
    ├── service/
    ├── table/
    └── udf/
```

quick-start 的三张表只配置 `'connector' = 'filesystem'`，不指定数据文件路径。完整的 MovieLens 示例用于展示 Redis、Milvus、Kafka、模型训练和在线推理等完整链路。

## 在本地元数据模式开发 DDL

本地元数据模式会在进程启动时从 `SQL_SCHEMA_DIR` 递归加载 SQL 文件，因此不允许通过 CLI 或 SQL API 直接执行 `CREATE TABLE`、`CREATE SQL FUNCTION`、`CREATE API` 等 DDL 语句。Demo 中默认开启的 SQL API 仅用于查询和写入测试数据。

在本地开发新的表、函数或 API 时，将定义写入宿主机上的 SQL 文件，然后把整个目录挂载到容器，并将 `SQL_SCHEMA_DIR` 指向容器内的挂载路径。例如：

```bash
docker run --rm -d --name sqlrec-custom \
  -p 30000:30000 \
  -p 30001:30001 \
  -v "$(pwd)/sql:/workspace/sql:ro" \
  -e SQL_SCHEMA_DIR=/workspace/sql \
  sqlrec/sqlrec-demo:latest
```

`./sql` 目录应包含本次启动需要的全部 SQL 定义。修改文件后需要重启容器，SQLRec 才会重新加载这些定义。

如果希望像使用数据库一样在会话中直接执行和持久化 DDL，请按照[服务部署](/docs/deployment)搭建完整集群，再通过 beeline、JDBC 或其他客户端连接 SQLRec。

## 停止 Demo

```bash
docker stop sqlrec-demo
```

由于启动时使用了 `--rm`，容器停止后会自动删除。

更多数据源配置请参考[内置 Connector](/docs/connectors/builtin_connectors)。
