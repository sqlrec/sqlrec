# Quick Start

Docker is all you need to try SQLRec. The demo image includes the tables, SQL function, and API required by the quick start. Its example tables use the `filesystem` connector and keep data in process memory, so Redis, PostgreSQL, Hive Metastore, Flink, and Kubernetes are not required.

## Start the Demo

```bash
docker run --rm -d --name sqlrec-demo \
  -p 30000:30000 \
  -p 30001:30001 \
  sqlrec/sqlrec-demo:latest
```

Follow the logs to confirm that the service has started:

```bash
docker logs -f sqlrec-demo
```

After startup completes, press `Ctrl+C` to stop following the logs. The container continues running in the background.

## Open the SQLRec CLI

Run `cli.sh` inside the container directly from the host to open the SQL CLI:

```bash
docker exec -it sqlrec-demo /app/cli.sh
```

At the `sqlrec>` prompt, you can run SQL statements directly. End each statement with a semicolon. For example, inspect the demo objects and data:

```sql
show tables;
show functions;
show apis;
select * from demo_user_interest_category;
```

Press `Ctrl+D` to leave the SQL CLI. The CLI runs in its own process inside the container, with memory separate from the HTTP service.

## Inspect the Bundled Test Data

The quick start includes five users with three interests each and 25 hot items across five categories (`pc`, `phone`, `book`, `sports`, `home`) in two CSV files. The CLI loads them into its process memory on first access:

| User ID | Interest categories |
| --- | --- |
| `1000001` | `pc`, `phone`, `book` |
| `1000002` | `phone`, `sports`, `home` |
| `1000003` | `book`, `home`, `pc` |
| `1000004` | `sports`, `pc`, `phone` |
| `1000005` | `home`, `book`, `sports` |

```sql
select * from demo_user_interest_category;
select * from demo_category_hot_item;
```

You can still use `INSERT` to add or update rows. Those changes remain in the current CLI process and reset to the CSV contents when you exit.

## Get Recommendations

The demo defines a `demo_rec` SQL function. Continue in the same CLI session, create its input table, and call it:

```sql
cache table quick_start_user as
select cast(1000001 as bigint) as user_id;

call demo_rec(quick_start_user);
```

The function returns two hot items with their recommendation reasons, request timestamp, and request ID. Exposure records are written to the current process's in-memory `demo_exposure_item` table; later calls use these records for deduplication.

## Call the Recommendation API

The CLI and HTTP service run in separate processes inside the container. Each loads the bundled CSV data into its own memory, so you can call the `demo_rec` API immediately:

```bash
curl -X POST http://localhost:30001/api/v1/demo_rec \
  -H "Content-Type: application/json" \
  -d '{"data":{"user_info":[{"user_id":1000001}]}}'
```

The endpoint returns the result of `demo_rec`. Exposure data stays in the HTTP service process, so later calls exclude previously returned items. Restart the container after the bundled candidates are exhausted. The demo also enables `/sql/v1` for adding test data to the HTTP process; changes made in the CLI are not visible there.

## Open the UI

Open [http://localhost:30001/ui/static/index.html](http://localhost:30001/ui/static/index.html) to inspect tables, APIs, SQL functions, and their execution DAGs.

## Demo Directory Layout

`SQL_SCHEMA_DIR` is consistently set to `/app/sql`, and SQLRec recursively loads both example directories. The two examples use distinct table, function, and API identifiers:

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

The two quick-start input tables use `${SQL_SCHEMA_DIR}` to locate the bundled CSV files and load them into memory on first access. Filesystem tables use the primary key as a lookup key, so the user-interest table retains three rows per `user_id`. The exposure table also uses `user_id` as its lookup key; it starts empty and can retain multiple exposures per user. The complete MovieLens example demonstrates a full pipeline involving Redis, Milvus, Kafka, model training, and online inference.

## Managing Local SQL Definitions

Local metadata mode recursively loads SQL files from `SQL_SCHEMA_DIR` when the process starts. It does not allow DDL through the CLI or SQL API. The SQL API enabled by the demo is mainly intended for queries and test-data writes.

To develop a new table, function, or API locally, write its definition in SQL files on the host, mount the complete directory into the container, and point `SQL_SCHEMA_DIR` to the mounted path. For example:

```bash
docker run --rm -d --name sqlrec-custom \
  -p 30000:30000 \
  -p 30001:30001 \
  -v "$(pwd)/sql:/workspace/sql:ro" \
  -e SQL_SCHEMA_DIR=/workspace/sql \
  sqlrec/sqlrec-demo:latest
```

The `./sql` directory must contain every SQL definition required for that run. Restart the container after changing a file so that SQLRec reloads the definitions. For online serving, version this directory with the image or deployment configuration and redeploy all instances when it changes, reducing dependencies on remote metadata services.

If you need to execute and persist DDL interactively like a database, follow [Service Deployment](/en/docs/operations/deployment) to set up the complete cluster, then connect through beeline, JDBC, or another SQLRec client.

## Stop the Demo

```bash
docker stop sqlrec-demo
```

Because the container was started with `--rm`, Docker removes it after it stops.

For more data-source configuration, see [Built-in Connectors](/en/docs/reference/connectors/builtin-connectors).
