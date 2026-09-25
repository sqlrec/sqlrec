# SQLRec Architecture

SQLRec uses SQL to orchestrate recommendation workflows: read features and candidates, call functions and model services, return results, and write to external stores. The module overview comes first; the rest of the page follows a request to explain the execution model and data boundaries. Links at the end cover syntax, parameters, and deployment steps.

## Module Overview

```text
                        ┌──────────────────────────────────────────┐
                        │             sqlrec-frontend              │
                        │         Thrift / REST / CLI / UI         │
                        └───────────────┬──────────────────────────┘
                                        │ calls
                        ┌───────────────▼──────────────────────────┐
                        │               sqlrec-core                │
                        │ statement routing / Calcite compilation  │
                        │  function execution / metadata / models  │
                        └──┬───────────────┬───────────────┬───────┘
                           │               │               │
              ┌────────────▼───┐   ┌───────▼──────┐  ┌─────▼────────┐
              │  sqlrec-sql-   │   │ sqlrec-model │  │  sqlrec-udf  │
              │     parser     │   │    model     │  │   built-in   │
              │  two parsers   │   │   backends   │  │  functions   │
              └────────────────┘   └──────────────┘  └──────────────┘
                           │               │               │
                        ┌──▼───────────────▼───────────────▼───────┐
                        │              sqlrec-common               │
                        │  table abstractions / context / config   │
                        └───────────────┬──────────────────────────┘
                                        │ runtime SPI discovery
                        ┌───────────────▼──────────────────────────┐
                        │            sqlrec-connectors             │
                        │    Redis / JDBC / MongoDB / Milvus /     │
                        │            Kafka / filesystem            │
                        └──────────────────────────────────────────┘
```

The upper half shows the main modules used after a request enters the engine; the lower half shows shared abstractions and storage adapters. The arrow to Connectors denotes runtime discovery through an SPI: `sqlrec-common` does not depend on concrete Connectors. The frontend serves `sqlrec-ui` static assets; `sqlrec-demo` provides examples, and `sqlrec-flink` contains Flink integration tests.

These objects appear throughout the execution flow:

| Object | Meaning in the execution flow |
| --- | --- |
| Connector table | The SQL entry point to external data or local files; determines available reads and writes |
| SQL function | A reusable recommendation flow made of several statements |
| Schema | Tables and functions visible to the current executor, including named `CACHE TABLE` results |
| Execution context | Variables, metric tags, and cancellation state for the current call |
| API / Service | An API publishes a SQL function to callers; a Service exposes a model inference endpoint |

## From Request to Result

```text
Client
  ├─ Thrift / REST SQL / CLI ─→ parse and route SQL
  │                              ├─ local ─→ compile with Calcite ─→ execute ─→ return results
  │                              │                            └─ Connectors / SQL functions
  │                              └─ other SQL ─→ Flink SQL Gateway (remote-mode Thrift sessions)
  └─ REST API ─→ resolve API to SQL function ─→ execute function ─→ return JSON
```

SQLRec uses two parsers. The Flink parser gets the first chance to parse standard Flink SQL; a separate SQLRec parser handles extensions such as `CACHE TABLE`, `CALL`, and model management. The executor then routes the statement to local Calcite execution, metadata handling, or Flink.

The local path handles online SQL queries and writes, SQL functions, and control statements. Calcite compiles queries into executable code, which reads or writes data through Connectors. SQLRec handles resource commands such as `SHOW` and model or service management itself. In remote metadata mode, Thrift sessions forward statements not handled locally, such as some streaming DDL, to Flink SQL Gateway. `USE` and `SET` apply to both local and remote sessions. REST `/sql/v1` and CLI have no transparent forwarding path, and local file metadata mode does not depend on Flink Gateway.

A Thrift session owns its SQL executor. REST `/sql/v1` creates one executor per request and runs that request's statements in order. `POST /api/v1/<name>` resolves the published API and its SQL function, then creates input tables and an execution context for that call. See [Publishing and Calling APIs](../guides/api.md) for the API contract.

The entry point determines how long state remains visible and whether forwarding is available:

| Entry point | Executor and state lifetime | Flink forwarding |
| --- | --- | --- |
| Thrift | One executor, default database, and named intermediate tables per session | Available in remote mode |
| REST `/sql/v1` | Statements in one request share an executor, released afterward | No transparent forwarding |
| CLI | A local executor for the current CLI run | No transparent forwarding |
| REST `/api/v1/<name>` | Function input tables and execution context created for each call | No forwarding |

## Execution Model

### Plain SQL and SQL Functions

Plain `SELECT`, `INSERT`, `UPDATE`, and `DELETE` statements are validated, optimized, and compiled by Calcite, then run in the current execution context. The context holds request variables, metric tags, and cancellation state. Query results are materialized in process memory before returning to the client. Queries containing `WITH` do not take the local Calcite path; a remote-mode Thrift session can forward them to Flink.

A SQL function is a sequence of separately submitted statements rather than one query. For example:

```sql
CREATE SQL FUNCTION recommend;

DEFINE INPUT TABLE user_info (user_id BIGINT);

CACHE TABLE candidates AS
SELECT item_id, score FROM hot_item LIMIT 10;

RETURN candidates;
```

The top-level `RETURN` ends the definition and saves the function. At call time, SQLRec obtains the compiled function (compiling it on the first access and reusing it afterward), adds input tables to the call's schema, and executes the body. Independent API requests do not share input tables or intermediate results. See [Writing a SQL Recommendation Flow](../guides/recommendation-flow.md) for function syntax and examples.

A function call has three main steps:

1. Check declared input tables and obtain the function definition and compiled form;
2. Execute queries, `CACHE TABLE`, `CALL`, and control statements according to table read/write dependencies;
3. Take the result from the reached `RETURN` and convert it into the Thrift/REST response.

For example, a business call to `POST /api/v1/recommend` first maps the API name to a SQL function. Input rows in the request become tables visible to that function. The function reads Connectors, creates intermediate `CacheTable` results, optionally calls a model Service, and returns JSON from `RETURN`. Plain SQL uses the same local query engine but does not automatically reuse a compiled SQL function.

These statements determine how data moves through a function:

| Statement | Effect at execution time |
| --- | --- |
| `CACHE TABLE t AS ...` | Runs a query or synchronous call and names its result as an in-memory table `t` for the current execution |
| `CALL f(...)` | Calls a SQL function or Java table function; a synchronous result can be processed further or returned |
| `IF ... THEN ... ELSE ...` | Chooses a branch based on a query; `TIMEIN` can fall back to `ELSE` after timeout or failure |
| `RETURN` | Returns a table, query, or synchronous call result and stops later work in the current function |

`CACHE TABLE` is not persistent storage. It lives in the current executor's schema: later statements in a Thrift session or the same REST SQL request can reference it, while independent REST/API requests cannot share it. A plain query result is not automatically registered as a named `CacheTable`. Write to an external Connector table when data must survive across requests.

### Dependencies, Parallelism, and Async Calls

SQLRec determines dependencies between SQL-function statements from the tables they read and write. Statements that need prior results wait for them; independent nodes can run concurrently when `PARALLELISM_EXEC` is enabled. Nodes not started after `RETURN` are skipped. `CALL ... PARTITION BY` splits an input table into partitions, calls the function concurrently, and merges the results. `CALL ... ASYNC` submits background work and returns immediately, so it cannot provide a synchronous result to `CACHE TABLE` or `RETURN`.

Local queries and intermediate results are primarily materialized in JVM memory, so their size is constrained by process memory. This path has no general spill-to-disk or streaming intermediate-result mechanism; use the Flink path for continuous processing. See the [SQL Reference](./sql.md) for complete control-flow syntax.

## Data Sources and Caches

### Connector Capabilities

All Connectors expose Calcite tables, with query capabilities determined by the backing store:

| Connector | Main capabilities in local execution |
| --- | --- |
| Redis | Primary-key lookup; no full scan; SQL writes and Flink Lookup/writes |
| JDBC, MongoDB | Primary-key lookup or filtered scan; SQL writes |
| Milvus | Key/filter lookup and vector search; SQL writes and a Flink Sink |
| Kafka | Calcite table is write-only; streaming reads run through Flink |
| filesystem | Loads local CSV/JSON into memory on first use; subsequent reads and writes affect the table object's in-memory data, without writing it back to the source file |

A KV Join batches lookups when the right-side join column is its primary key; joining on a non-key column scans once per key. Milvus can be the right side of a vector lookup Join. See [Built-in Connectors](./connectors/builtin-connectors.md) for configuration, query constraints, and write behavior.

### Four Different Kinds of “Cache”

| Data | Scope and purpose | How it changes |
| --- | --- | --- |
| `CACHE TABLE` | Named intermediate result in the current executor's schema | Created by the statement; `FLUSH` does not remove it |
| Connector primary-key row cache | Per Calcite table object for Redis, JDBC, and MongoDB; speeds up key lookups | Expires by configured TTL and size; SQL writes invalidate affected keys |
| filesystem in-memory snapshot | Whole-table data loaded by each filesystem table object | SQL writes change memory only; no TTL or automatic file-change detection |
| Definition caches | Process-wide schema, SQL/Java functions, APIs, and service configuration | Refreshed or expired by their own policies; `FLUSH` invalidates them |

### Connector Row Cache and Query Paths

The primary-key row cache is used only through `getByPrimaryKey()`: Redis primary-key equality (including an extractable key condition within `AND`), standalone JDBC/MongoDB primary-key equality, and KV Joins on the right table's primary key use it. JDBC/MongoDB compound, non-key, range, and full-scan queries go directly to the source. Milvus, filesystem, and Flink Redis Lookup do not use this primary-key cache; the Kafka Calcite table cannot be read.

If `id` is the primary key, these conditions illustrate common paths:

| Query condition | Redis | JDBC / MongoDB |
| --- | --- | --- |
| `WHERE id = 1` | Primary-key cache | Primary-key cache |
| `WHERE id = 1 AND status = 'active'` | Fetch by key, then filter | Compound scan; no primary-key cache |
| `WHERE status = 'active'` | No key; full scan unsupported | Filtered scan; no primary-key cache |

External or Flink writes do not proactively clear the row cache on an existing Calcite table object. `FLUSH` also does not directly clear row caches, filesystem snapshots, or session `CACHE TABLE` results; rebuilding a table object creates a fresh cache or reloads the file.

## Metadata Management

Metadata describes databases, tables, Java UDFs, SQL functions, APIs, models, and services. It determines what SQLRec can resolve and call; it does not hold the business data in Connectors. Both modes supply definitions to the executor through the same metadata access layer. They differ in where definitions come from and how changes persist. An empty `SQL_SCHEMA_DIR` selects remote mode; a nonempty directory selects local SQL-file mode.

| Aspect | Remote metadata mode | Local SQL-file mode |
| --- | --- | --- |
| Tables and Java UDFs | Read databases, table schemas, and function definitions from Hive Metastore (HMS) | Recursively parse `.sql` files under `SQL_SCHEMA_DIR` at startup and build an in-process schema |
| SQL functions, APIs, models, Checkpoints, Services | Stored in PostgreSQL and read or updated by SQLRec management commands | Function, API, model, and Service definitions from SQL files initialize in-process objects; there is no shared metadata store |
| Updating definitions | Resource commands update PostgreSQL; remote-mode Thrift sessions can forward Flink DDL for tables to Flink SQL Gateway, which updates HMS | Edit SQL files and restart or redeploy instances; `FLUSH` does not reparse files |
| Dependencies and use | Requires HMS and PostgreSQL and lets instances share definitions; Flink Gateway is needed only for forwarded statements | Suitable for demos, local development, and online serving: SQL files can be versioned with the image or deployment configuration, removing HMS, PostgreSQL, and Flink Gateway dependencies; any referenced Connectors or model endpoints must still be reachable |

Local SQL-file mode **does not allow DDL execution**. Definitions are loaded from SQL files. Queries, data writes, `CACHE TABLE`, and calls to predefined functions remain available. For online serving, deploy the complete SQL directory with the application version so each instance loads the same definitions at startup; redeploy or restart all instances after a change.

In either mode, table definitions become Calcite tables, and SQLRec caches database lists, table schemas, and function/API/Service definitions. In remote mode, relevant caches refresh on later access after `SCHEMA_CACHE_EXPIRE`; `FLUSH` invalidates them immediately. HMS table refresh uses `transient_lastDdlTime` to decide whether to reuse a table object, so an external schema change without an updated timestamp can leave an old object in use. Local metadata is read from files only during startup initialization: `FLUSH` invalidates in-process caches, which rebuild on later access, but file changes still require a restart. See [Service Deployment](../operations/deployment.md) for configuration and the [Docker Quick Start](../getting-started/docker.md) for a local-file example.

## Models and Online Services

Model features revolve around three objects: a **Model** defines the backend and input/output contract, a **Checkpoint** identifies a trained, downloaded, or exported version, and a **Service** connects the model to an online inference endpoint (deploying a hosted model or pointing to an existing URL). A typical trained model follows:

```text
CREATE MODEL → TRAIN MODEL → check Checkpoint status
             → EXPORT MODEL → CREATE SERVICE → call_service(...)
```

Training and export submit Kubernetes Jobs. Successful submission does not mean the job has completed; check the Checkpoint status. `CREATE SERVICE` deploys an online workload for a hosted model. The `call_service` function in SQL sends input rows to that service and merges predictions into the rows. Backend paths differ: Hugging Face Transformers does not require export, while an `external` model uses an existing HTTP service and needs no training, export, or locally deployed inference container.

`CREATE SERVICE` manages a model inference endpoint; `CREATE API` publishes a SQL function for business callers. A public API can query Connectors and call a model Service within its SQL function. See [Model Training and Online Inference](../guides/model-lifecycle.md) and [Built-in Models](./models/builtin-models.md) for steps and backend differences.

## Runtime Boundaries

Online queries in remote mode require metadata services (HMS and PostgreSQL) and the storage systems they use. Forwarded statements require Flink SQL Gateway; training and deploying hosted models require Kubernetes. Local SQL-file mode also suits online serving: it reduces metadata-service dependencies and keeps definitions under version control. REST runs synchronous business work on a separate executor group; Thrift manages an executor per session. Metrics and traces cover requests and execution nodes. See [Service Deployment](../operations/deployment.md) for dependencies, ports, and resource requirements.

Keep three boundaries in mind: local SQLRec queries materialize in process memory; `CACHE TABLE` is visible only within its execution scope; external data, model services, and Flink jobs have separate lifecycles. Size deployment dependencies and resources for the paths you actually enable.

## Further Reading

- [Writing a SQL Recommendation Flow](../guides/recommendation-flow.md) and [SQL Reference](./sql.md): functions, control flow, and full syntax.
- [Connecting Data Sources](../guides/data-sources.md) and [Built-in Connectors](./connectors/builtin-connectors.md): table definitions, queries, and cache settings.
- [Publishing and Calling APIs](../guides/api.md) and [Model Training and Online Inference](../guides/model-lifecycle.md): public APIs and model lifecycle.
- [Custom Connectors](../development/custom-connector.md), [Custom UDFs](../development/custom-udf.md), and [Custom Models](../development/custom-model.md): extension points and implementation requirements.
