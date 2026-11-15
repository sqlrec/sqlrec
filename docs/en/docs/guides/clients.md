# Client Connections

| Client | Use | Default port |
| --- | --- | --- |
| Local CLI | Docker demo and SQL in a local process | None (in-process) |
| Beeline / JDBC / PyHive | Remote SQL sessions, shared metadata, and Flink forwarding | `30000` |
| HTTP | Recommendation APIs or direct SQL | `30001` |

## Local CLI

With the Docker demo running:

```bash
docker exec -it sqlrec-demo /app/cli.sh
```

The CLI runs the engine in a separate process from HTTP; in-memory data and DDL changes are not shared. End SQL statements with semicolons and exit with `Ctrl+D`. See [Docker Quick Start](../getting-started/docker.md#managing-local-sql-definitions) for custom SQL files.

After building a source checkout, run `bash bin/cli.sh`. Batch execution accepts `-e "SQL;"` or `-f statements.sql`. The first SQL failure stops subsequent statements and returns exit code `1`; successful batches return `0`. Interactive errors allow further input.

## Beeline and JDBC

Prepare the [remote service environment](../operations/deployment.md), then run from the repository root:

```bash
bash bin/beeline.sh
bash bin/beeline.sh -e 'SELECT 1;'
AUTH_USERNAME=alice AUTH_PASSWORD='my-password' bash bin/beeline.sh -f query.sql
NODE_IP=other-host SQLREC_THRIFT_PORT=10000 bash bin/beeline.sh
```

The script reads addresses and credentials from `deploy/env.sh`; matching environment variables override them. Execution options such as `-e` and `-f` pass through to Beeline. It does not read `SQLREC_ENV_CONF`; use the environment variables above for connections and credentials. Account lists are server configuration; clients send one username/password pair. The bundled Beeline version is unsuitable for credentials containing spaces.

- Authentication enabled: use `jdbc:hive2://<host>:<port>/default`, SASL PLAIN, and account credentials.
- Authentication disabled: append `;auth=noSasl`; with the script, use `AUTH_ENABLED=false bash bin/beeline.sh`.

A `CACHE TABLE` remains available to subsequent SQL in the same session and disappears on disconnect. Remote-metadata Thrift sessions can forward statements requiring Flink to Gateway. Failed local SQL is not retried through Gateway.

## Gateway Troubleshooting {#gateway-errors}

Only remote-metadata Thrift sessions support Gateway forwarding. REST and the local CLI do not transparently forward SQL.

- On connection failure, check `FLINK_SQL_GATEWAY_ADDRESS`, its port, and connectivity. An empty address disables forwarding; local SQL remains available.
- A transport failure fails the current remote call. The next remote statement can reconnect, but the failed statement is not replayed automatically. Check the outcome before resubmitting writes.
- Reconnection cannot restore old remote operation handles. After `FLINK_REMOTE_SESSION_LOST`, the original results cannot be fetched; check task status before submitting again.

## Python / PyHive

Install pure-Python SASL dependencies:

```bash
pip install 'PyHive[hive_pure_sasl]'
```

Use `auth="CUSTOM"` when authentication is enabled:

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

With authentication disabled, use `auth="NOSASL"` and omit `password`.

## HTTP

See [Publish and Call an API](./api.md) for recommendation requests and responses. When authentication is enabled, add `--user <username>`; curl prompts for a password. POST requests use `Content-Type: application/json`.

Submit SQL directly:

```bash
curl --user sqlrec -X POST http://localhost:30001/sql/v1 \
  -H 'Content-Type: application/json' \
  -d '{"sqls":["SHOW TABLES"]}'
```

The server must enable `ENABLE_REST_SQL_API`. SQL statements in one request share session state; separate requests do not retain `CACHE TABLE` results. Statement failures appear in each result's `msg`, so HTTP 200 does not mean all statements succeeded.

Java callers can use `SqlRecApiClient.callFunctionApi(url, data, params, username, password)` or `callSqlApi(url, sqls, params, username, password)`. Overloads without credentials call anonymously and do not automatically send local account credentials.

See [Entry Authentication](../operations/deployment.md#entry-authentication) for server configuration.
