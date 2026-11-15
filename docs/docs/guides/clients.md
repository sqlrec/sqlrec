# 客户端连接

| 使用方式 | 适用场景 | 默认端口 |
| --- | --- | --- |
| 本地 CLI | Docker 体验、在本地进程执行 SQL | 无（进程内执行） |
| Beeline / JDBC / PyHive | 远程 SQL 会话、共享元数据管理、Flink 转发 | `30000` |
| HTTP | 调用推荐 API 或提交 SQL | `30001` |

## 本地 CLI

Docker Demo 运行后执行：

```bash
docker exec -it sqlrec-demo /app/cli.sh
```

CLI 直接运行引擎，与 HTTP 服务是独立进程，内存数据和 DDL 修改互不可见。每条 SQL 以分号结束，按 `Ctrl+D` 退出。加载自定义 SQL 文件见 [Docker 快速开始](../getting-started/docker.md#管理本地-sql-定义)。

源码目录构建后可运行 `bash bin/cli.sh`。批量执行使用 `-e "SQL;"` 或 `-f statements.sql`；任一语句失败会停止后续语句并返回退出码 `1`，全部成功返回 `0`。交互模式下可在出错后继续输入。

## Beeline 与 JDBC

先按[服务部署](../operations/deployment.md)准备远程环境，再在仓库根目录执行：

```bash
bash bin/beeline.sh
bash bin/beeline.sh -e 'SELECT 1;'
AUTH_USERNAME=alice AUTH_PASSWORD='my-password' bash bin/beeline.sh -f query.sql
NODE_IP=other-host SQLREC_THRIFT_PORT=10000 bash bin/beeline.sh
```

脚本从 `deploy/env.sh` 读取地址和凭证，可用同名环境变量覆盖；`-e`、`-f` 等执行参数传给 Beeline。它不读取 `SQLREC_ENV_CONF`，连接和凭证应通过上述环境变量设置。账号列表是服务端配置，客户端每次只传其中一组用户名和密码。仓库 Beeline 版本不适合包含空格的凭证。

- 开启认证：JDBC URL 为 `jdbc:hive2://<host>:<port>/default`，使用 SASL PLAIN 并提供账号密码。
- 关闭认证：URL 追加 `;auth=noSasl`；脚本使用 `AUTH_ENABLED=false bash bin/beeline.sh`。

一个会话中创建的 `CACHE TABLE` 可在后续 SQL 中使用，断开后不保留。远程模式的 Thrift 会话可将需要 Flink 的语句转发给 Gateway；本地 SQL 执行失败不会自动改由 Gateway 重试。

## Gateway 连接排障 {#gateway-errors}

只有远程元数据模式的 Thrift 会话支持 Gateway 转发，REST 和本地 CLI 不提供透明转发。

- 连接失败时检查 `FLINK_SQL_GATEWAY_ADDRESS`、端口和网络；地址为空时禁用转发，本地 SQL 仍可使用。
- 传输中断会使当前远程调用失败，下一次远程语句可重新连接；当前语句不会自动重放，写入语句需确认执行结果后再提交。
- 重连不能恢复旧远程操作句柄。收到 `FLINK_REMOTE_SESSION_LOST` 时，不能继续读取原操作结果，应确认任务状态后重新提交。

## Python / PyHive

安装纯 Python SASL 依赖：

```bash
pip install 'PyHive[hive_pure_sasl]'
```

开启认证时使用 `auth="CUSTOM"`：

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

关闭认证时改为 `auth="NOSASL"` 并省略 `password`。

## HTTP

推荐函数的发布、请求和响应见[发布和调用 API](./api.md)。开启认证时增加 `--user <用户名>`，curl 会提示输入密码；POST 使用 `Content-Type: application/json`。

直接提交 SQL 的例子：

```bash
curl --user sqlrec -X POST http://localhost:30001/sql/v1 \
  -H 'Content-Type: application/json' \
  -d '{"sqls":["SHOW TABLES"]}'
```

需由服务开启 `ENABLE_REST_SQL_API`。同一请求内的 SQL 共用会话状态，不同请求之间不保留 `CACHE TABLE`。语句失败通过对应结果的 `msg` 返回，HTTP 200 不代表每条 SQL 都成功。

Java 调用方可使用 `SqlRecApiClient.callFunctionApi(url, data, params, username, password)` 或 `callSqlApi(url, sqls, params, username, password)`。不带凭证的重载为匿名调用，不会自动使用本机账号。

服务端账号和认证开关见[入口认证](../operations/deployment.md#入口认证)。
