# Quick Start

Docker is the only prerequisite. The demo includes sample data, a recommendation function, and its API; no external services are needed.

## Start the Demo

```bash
docker run --rm -d --name sqlrec-demo \
  -p 30000:30000 \
  -p 30001:30001 \
  sqlrec/sqlrec-demo:latest
```

Watch the startup log:

```bash
docker logs -f sqlrec-demo
```

After startup, press `Ctrl+C` to leave the log viewer. The container keeps running in the background.

## Call the Recommendation API

```bash
curl -X POST http://localhost:30001/api/v1/demo_rec \
  -H "Content-Type: application/json" \
  -d '{"data":{"user_info":[{"user_id":1000001}]}}'
```

The first call usually returns two items. The response's `data` contains result rows with `user_id`, `item_id`, `item_name`, `rec_reason`, `req_time`, and `req_id`. This excerpt shows fields from one result row; the item may differ:

```json
{
  "data": [
    {"item_id": 1000001, "rec_reason": "user_category_interest_recall:book"}
  ]
}
```

Use user IDs `1000001` through `1000005`. Repeated calls exclude previously recommended items. Restart the container when the sample candidates are exhausted.

## View the UI

Open [http://localhost:30001/ui/static/index.html](http://localhost:30001/ui/static/index.html) to inspect tables, functions, APIs, and execution DAGs.

## Stop the Demo

```bash
docker stop sqlrec-demo
```

The container is removed automatically because it was started with `--rm`.

## Optional: Open the SQLRec CLI

While the demo is running, open the SQL CLI from the host:

```bash
docker exec -it sqlrec-demo /app/cli.sh
```

At the `sqlrec>` prompt, enter statements ending with a semicolon:

```sql
show tables;
show functions;
show apis;
select * from demo_user_interest_category;

cache table quick_start_user as
select cast(1000001 as bigint) as user_id;

call demo_rec(quick_start_user);
```

`quick_start_user` is the function's input table. Press `Ctrl+D` to exit.

::: tip Demo Data Scope
Data is held in memory. The CLI and HTTP service have separate data, so CLI changes do not affect the API. Reopening the CLI restores its initial data; restarting the container resets the HTTP service's data.

Use `INSERT` to add rows and `UPDATE` to change them. To write test data to the HTTP service, use `/sql/v1`; local CLI changes are not visible to it.
:::

## Modify the Demo SQL

The recommendation logic is in `sqlrec-demo/src/main/sql/quick_start/function/demo_rec.sql`. Alongside it, `table/` defines tables, `api/` publishes the function, and `data/` holds initial CSV data.

The image loads definitions through `SQL_SCHEMA_DIR=/app/sql`. See [Writing a Recommendation Flow](../guides/recommendation-flow.md) for SQL logic. To load your own files, prepare a complete SQL directory as shown below.

## Managing Local SQL Definitions

Local file mode loads SQL definitions at startup. It does not accept DDL through the CLI or `/sql/v1`; restart the container after changing files. This example defines a hot-item table, a recommendation function, and an API.

### Prepare the SQL Files

Create the directories on the host:

```bash
mkdir -p sql/table sql/function sql/api
```

Save the following definitions in separate files.

`sql/table/hot_item.sql`:

```sql
CREATE TABLE hot_item (
  item_id BIGINT,
  score FLOAT,
  PRIMARY KEY (item_id) NOT ENFORCED
) WITH (
  'connector' = 'filesystem'
);
```

`sql/function/recommend.sql`:

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

`sql/api/recommend.sql`:

```sql
CREATE OR REPLACE API recommend WITH recommend;
```

### Load and Call

Mount the complete directory and set `SQL_SCHEMA_DIR`:

```bash
docker run --rm -d --name sqlrec-custom \
  -p 30000:30000 \
  -p 30001:30001 \
  -v "$(pwd)/sql:/workspace/sql:ro" \
  -e SQL_SCHEMA_DIR=/workspace/sql \
  sqlrec/sqlrec-demo:latest
```

`./sql` must contain all definitions needed for this startup. Write data to the HTTP service, then call the API:

```bash
curl -X POST http://localhost:30001/sql/v1 \
  -H "Content-Type: application/json" \
  -d '{"sqls":["insert into hot_item values (1001, 0.9), (1002, 0.8)"]}'

curl -X POST http://localhost:30001/api/v1/recommend \
  -H "Content-Type: application/json" \
  -d '{"data":{"user_info":[{"user_id":1000001}]}}'
```

This API returns hot items ordered by score without using user features. Extend the function to add your own recommendation logic.

Filesystem tables are for demos and tests; writes do not update source files. See [Built-in Connectors](../reference/connectors/builtin-connectors.md) for configuration and data limits.

For online serving, version the complete SQL directory with the image or deployment configuration and redeploy instances when it changes. To execute and persist DDL in a session, prepare the remote metadata environment described in [Service Deployment](../operations/deployment.md) and connect through Beeline or JDBC.
