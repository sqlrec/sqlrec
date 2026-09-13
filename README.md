<h1 align="center">SQLRec</h1>

<p align="center">
  English | <a href="README_zh.md">中文</a>
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

## Project Introduction

A recommendation engine that supports SQL-based development. The goal is to enable data scientists, including data analysts, data engineers, and backend developers, to quickly build production-ready recommendation systems. The system architecture is shown in the figure below. SQLRec encapsulates underlying component access, model training, inference, and other processes using SQL, allowing upper-level recommendation business logic to be described using only SQL.

![system_architecture](docs/public/sqlrec_arch.svg)

SQLRec has the following features:

- Cloud-native, comes with minikube-based deployment scripts for one-click deployment of the SQLRec system and related dependency services
- Extended SQL syntax, making it possible to describe recommendation system business logic using SQL
- Implemented an efficient SQL execution engine based on Calcite, meeting the real-time requirements of recommendation systems
- Built on existing big data ecosystem, easy to integrate
- Easy to extend, supports custom UDFs, Table types, and Model types

For detailed information, refer to the [SQLRec User Manual](https://sqlrec.github.io/sqlrec/en/).

## Quick Experience

Run the dependency-free Docker demo:

```bash
docker run --rm -d --name sqlrec-demo \
  -p 30000:30000 \
  -p 30001:30001 \
  sqlrec/sqlrec-demo:latest
```

The demo stores its example data in process memory. Insert data into the HTTP service process:

```bash
curl -X POST http://localhost:30001/sql/v1 \
  -H "Content-Type: application/json" \
  -d @- <<'JSON'
{
  "sqls": [
    "insert into demo_user_interest_category values (1000001, 'pc', 100)",
    "insert into demo_category_hot_item values ('pc', 1000001, 100), ('pc', 1000002, 90)"
  ]
}
JSON
```

Call the built-in recommendation API:

```bash
curl -X POST http://localhost:30001/api/v1/demo_rec \
  -H "Content-Type: application/json" \
  -d '{"data":{"user_info":[{"user_id":1000001}]}}'
```

### Define Your Own Table and Function

In the Docker demo, tables, SQL functions, and APIs are loaded from `.sql` files under `SQL_SCHEMA_DIR`. For example, prepare this directory on the host:

```text
sql/
├── table/hot_item.sql
├── function/recommend.sql
└── api/recommend.sql
```

Define a table in `sql/table/hot_item.sql`:

```sql
CREATE TABLE hot_item (
  item_id BIGINT,
  score FLOAT,
  PRIMARY KEY (item_id) NOT ENFORCED
) WITH (
  'connector' = 'filesystem'
);
```

Define a SQL function in `sql/function/recommend.sql`:

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

Publish the function in `sql/api/recommend.sql`:

```sql
CREATE OR REPLACE API recommend WITH recommend;
```

Stop the built-in demo, then mount the complete definition directory and restart SQLRec:

```bash
docker stop sqlrec-demo

docker run --rm -d --name sqlrec-custom \
  -p 30000:30000 \
  -p 30001:30001 \
  -v "$(pwd)/sql:/workspace/sql:ro" \
  -e SQL_SCHEMA_DIR=/workspace/sql \
  sqlrec/sqlrec-demo:latest
```

Local metadata mode does not accept DDL through the CLI or `/sql/v1`. Edit the SQL files and restart the container whenever a definition changes. Filesystem table data is held in memory and is intended only for demos and tests.

Insert data and call the newly published API:

```bash
curl -X POST http://localhost:30001/sql/v1 \
  -H "Content-Type: application/json" \
  -d '{"sqls":["insert into hot_item values (1001, 0.9), (1002, 0.8)"]}'

curl -X POST http://localhost:30001/api/v1/recommend \
  -H "Content-Type: application/json" \
  -d '{"data":{"user_info":[{"user_id":1000001}]}}'
```

Open [http://localhost:30001/ui/static/index.html](http://localhost:30001/ui/static/index.html) to inspect the loaded tables, functions, APIs, and execution DAG. Stop and remove the demo when finished:

```bash
docker stop sqlrec-custom
```

For more CLI, data-loading, and API examples, see the [Docker Quick Start](https://sqlrec.github.io/sqlrec/en/docs/getting-started/docker).

### Complete Service Mode

Compared with the Docker demo's local metadata mode, the complete service mode primarily adds persistent, mutable metadata. Through Beeline, JDBC, or another Hive Thrift client, you can execute and retain management statements such as:

- `CREATE TABLE`, `CREATE SQL FUNCTION`, and `CREATE API`;
- `CREATE MODEL`, `TRAIN MODEL`, and `EXPORT MODEL`;
- `CREATE SERVICE` and other model-service lifecycle operations.

The bundled Minikube scripts are intended for development and testing. System requirements and component versions evolve with the scripts; use the current `deploy/` configuration as authoritative. See [Service Deployment](https://sqlrec.github.io/sqlrec/en/docs/operations/deployment) for the complete deployment process and production considerations.

## Roadmap

### When will version 1.0 be released

Versions before 1.0 are beta versions, not recommended for production use, and interface compatibility is not guaranteed. There is no planned release date yet. It will be released after the following features are completed:

- Comprehensive unit test, integration test, and effectiveness test coverage
- Code quality optimization, many details still need to be polished
- Support for degradation and timeout configuration
- Complete version management method, easy to roll back to previous versions
- Metric monitoring system improvement
- C++ model serving

### Future Feature Planning

- Frontend UI for viewing current execution DAG, SQL code, statistics, etc.
- Further optimize SQL syntax compatibility and runtime performance
- More ready-to-use UDFs, models, etc.
- Support for more external data sources, such as JDBC, MongoDB, etc.
- Tensorboard visualization of model training process
- GPU training and inference support
- Support for authentication and authorization
- Best practice tutorials, including search, recommendation, etc.
