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

从宿主机直接运行容器中的 `cli.sh`，进入 SQL 命令行：

```bash
docker exec -it sqlrec-demo /app/cli.sh
```

看到 `sqlrec>` 提示符后，就进入了 SQL 命令行，可以直接输入以分号结束的 SQL。例如，查看 Demo 已加载的对象和数据：

```sql
show tables;
show functions;
show apis;
select * from demo_user_interest_category;
```

按 `Ctrl+D` 退出 SQL 命令行。CLI 在容器内启动独立进程，其内存数据与 HTTP 服务进程不共享。

## 查看内置测试数据

quick-start 内置五个用户、每人三条兴趣偏好，以及 25 条热门商品，覆盖 `pc`、`phone`、`book`、`sports`、`home` 五个类目，分别保存在两个 CSV 文件中。CLI 首次查询时会将数据加载到当前进程的内存：

| 用户 ID | 兴趣类目 |
| --- | --- |
| `1000001` | `pc`、`phone`、`book` |
| `1000002` | `phone`、`sports`、`home` |
| `1000003` | `book`、`home`、`pc` |
| `1000004` | `sports`、`pc`、`phone` |
| `1000005` | `home`、`book`、`sports` |

```sql
select * from demo_user_interest_category;
select * from demo_category_hot_item;
```

也可以通过 `INSERT` 增加或更新数据。修改只保存在当前 CLI 进程的内存中，退出 CLI 后会恢复到 CSV 中的初始数据。

## 获取推荐结果

Demo 已经定义 `demo_rec` SQL 函数。继续在同一个 CLI 会话中创建输入表并调用函数：

```sql
cache table quick_start_user as
select cast(1000001 as bigint) as user_id;

call demo_rec(quick_start_user);
```

函数会返回两条热门商品及其推荐理由、请求时间和请求 ID。

曝光结果会写入当前进程内存中的 `demo_exposure_item` 表；再次调用时，`demo_rec` 会使用这些记录进行去重。如果继续重复调用，内置候选商品最终都会被过滤；重新进入 CLI 或写入新商品即可继续测试。

## 通过 API 调用推荐接口

容器中的 CLI 和 HTTP 服务运行在不同进程中，各自从内置 CSV 加载初始数据。启动后可直接调用 `demo_rec` 推荐 API：

```bash
curl -X POST http://localhost:30001/api/v1/demo_rec \
  -H "Content-Type: application/json" \
  -d '{"data":{"user_info":[{"user_id":1000001}]}}'
```

接口会返回 `demo_rec` 的推荐结果。推荐产生的曝光数据保留在 HTTP 服务进程内，再次调用会过滤已曝光商品；内置候选商品用完后，重启容器即可重置。Demo 镜像仍默认开启 `/sql/v1`，可用它向 HTTP 服务进程增加测试数据；CLI 中的修改对 HTTP 进程不可见。

## 查看 UI

浏览器访问 [http://localhost:30001/ui/static/index.html](http://localhost:30001/ui/static/index.html)，可以查看表、API、SQL 函数及其执行 DAG。

## 修改 Demo SQL

想修改示例时，可以从 `sqlrec-demo/src/main/sql/quick_start/` 中的三类 SQL 开始：`table/` 定义表，`function/` 定义推荐流程，`api/` 将函数发布为 HTTP API。

Demo 镜像中的 `SQL_SCHEMA_DIR` 设置为 `/app/sql`，SQLRec 会递归加载以下目录：

```text
sqlrec-demo/src/main/sql/
├── quick_start/
│   ├── api/demo_rec.sql
│   ├── data/
│   │   ├── demo_category_hot_item.csv
│   │   └── demo_user_interest_category.csv
│   ├── function/demo_rec.sql
│   └── table/
│       ├── demo_category_hot_item.sql
│       ├── demo_exposure_item.sql
│       └── demo_user_interest_category.sql
└── movielens/
    ├── api/
    ├── function/
    ├── model/
    ├── service/
    ├── table/
    └── udf/
```

quick-start 的两张输入表通过 `${SQL_SCHEMA_DIR}` 指向内置 CSV，并在首次读取时加载到内存。filesystem 表将主键作为查找键，因此用户兴趣表能在同一 `user_id` 下保存三条记录；曝光表也以 `user_id` 为查找键，初始为空，可以保存每位用户的多条曝光。完整的 MovieLens 示例用于展示 Redis、Milvus、Kafka、模型训练和在线推理等完整链路。

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

如果希望像使用数据库一样在会话中直接执行和持久化 DDL，请按照[服务部署](/docs/operations/deployment)搭建完整集群，再通过 beeline、JDBC 或其他客户端连接 SQLRec。

## 停止 Demo

```bash
docker stop sqlrec-demo
```

由于启动时使用了 `--rm`，容器停止后会自动删除。

更多数据源配置请参考[内置 Connector](/docs/reference/connectors/builtin-connectors)。
