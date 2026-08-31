# SQLRec Architecture

## Project Overview

### Positioning

SQLRec is a recommendation engine that **describes the entire business logic of a recommendation system in SQL**. The goal is to enable data analysts, data engineers, and backend developers (not necessarily proficient in Java/Python engineering) to quickly build production-ready recommendation systems. It encapsulates the following capabilities uniformly into SQL:

- Underlying storage access (Redis / JDBC / MongoDB / Milvus / Kafka / HDFS / local files)
- Model training, export, and online inference service deployment (XGBoost / LightGBM / CatBoost / DSSM / Wide&Deep / external services)
- Recommendation business orchestration (recall → ranking → diversification → persistence)

### Core Design Decisions

| Decision | Description |
| --- | --- |
| **Calcite as the online execution engine** | The self-developed SQL engine is built on Apache Calcite 1.32's Enumerable execution layer (Janino dynamic compilation to Java bytecode), meeting the real-time requirements of online recommendation scenarios |
| **Flink Parser as the syntax frontend** | Reuses Flink 1.19's parser codegen (`FlinkSqlParserImpl`), extending custom DDL/DML AST nodes on top of it, ensuring compatibility with Flink SQL syntax |
| **Dual execution stacks** | The online path goes through Calcite (in-process execution); the streaming/offline path passes statements through to a remote Flink SQL Gateway |
| **Dual metadata modes** | In remote mode, table/Java-UDF metadata comes from Hive Metastore and model/service/API/SQL-function definitions from PostgreSQL; setting `SQL_SCHEMA_DIR` selects local SQL-file + in-memory metadata and removes the HMS/PG dependency |
| **K8s as the compute foundation** | Model training/export runs as K8s Jobs, inference services as K8s Deployments; the engine generates YAML and applies it via serverSideApply |
| **Full in-memory execution model** | All intermediate results (CacheTable) are materialized as in-memory Lists, without disk persistence/streaming; data scale is managed via control flow (cache/if/assert) |

### Technology Stack

| Category | Components |
| --- | --- |
| Language | Java 21 (core/frontend/model/parser/demo), Java 11 (common/udf/connectors/ui/flink) |
| SQL engine | Apache Calcite 1.32 (Enumerable + VolcanoPlanner) |
| Syntax frontend | Flink 1.19 `flink-sql-parser` (codegen extension) |
| Metadata | Hive Metastore 3.1.3 (thrift client), PostgreSQL (MyBatis 3.5.19) |
| Storage | Lettuce (Redis), HikariCP (JDBC), MongoDB driver, Milvus SDK, Kafka client, Hadoop HDFS client |
| K8s | fabric8 kubernetes-client |
| Network entry | Thrift (Hive TCLIService protocol, compatible with beeline/Kyuubi), Netty (REST), JLine (CLI) |
| Observability | Micrometer + Prometheus, OpenTelemetry 1.29 (OTLP trace), Jaeger |
| Cache | Caffeine 2.9, in-house `ObjCache` |
| Model serving | C++ (catboost_server / onnx_server / tzrec server, embedding cpp-httplib), Python (training scripts) |

## Overall Architecture

### Module Division and Dependencies

```
                        ┌──────────────────────────────────────────┐
                        │            sqlrec-frontend               │
                        │  ThriftServer / RestServer / Cli / UI    │
                        └───────────────┬──────────────────────────┘
                                        │ depends on
                        ┌───────────────▼──────────────────────────┐
                        │               sqlrec-core                │
                        │  compiler / executor / runtime / rules   │
                        │  node / schema / db / k8s / model / udf  │
                        └──┬───────────────┬───────────────┬───────┘
                           │               │               │
              ┌────────────▼───┐   ┌───────▼──────┐  ┌────▼─────────┐
              │ sqlrec-sql-    │   │ sqlrec-model │  │ sqlrec-udf   │
              │ parser (Flink  │   │ (gbdt/tzrec/ │  │ (scalar/table│
              │  codegen ext.) │   │  external)   │  │  UDF impl.)  │
              └────────────────┘   └──────────────┘  └──────────────┘
                           │               │               │
                        ┌──▼───────────────▼───────────────▼───────┐
                        │              sqlrec-common               │
                        │  config / schema abstractions / runtime  │
                        │  context / rest client / utils           │
                        └───────────────┬──────────────────────────┘
                                        │ SPI (HmsTableFactory)
                        ┌───────────────▼──────────────────────────┐
                        │            sqlrec-connectors             │
                        │ filesystem / jdbc / kafka / milvus /     │
                        │ mongodb / redis (6 sub-modules)          │
                        └──────────────────────────────────────────┘

   Standalone modules: sqlrec-demo (demo business), sqlrec-flink (Flink integration tests),
   sqlrec-ui (static UI resources)
```

Module responsibilities:

| Module | Responsibilities |
| --- | --- |
| **sqlrec-common** | Shared foundation layer: configuration, Calcite-type-dependent table abstractions (`SqlRecTable`/`SqlRecKvTable`/`CacheTable`/`VectorSearchable`), execution contexts, REST client, and general utilities |
| **sqlrec-sql-parser** | Extends the Flink parser via FMPP + Freemarker codegen, producing AST nodes (`com.sqlrec.sql.parser.Sql*`) for `CREATE/DROP MODEL`, `TRAIN/EXPORT MODEL`, `CREATE/DROP SERVICE`, `CREATE/DROP SQL FUNCTION`, `CREATE/DROP API`, `CACHE`, `IF...CACHE`, `ASSERT`, `CALL`, `RETURN`, `FLUSH`, `SET`, etc. |
| **sqlrec-core** | Engine core: compiler, executor, runtime Bindable system, Calcite optimization rules and physical nodes, schema assembly, metadata access, K8s management, model/service management |
| **sqlrec-udf** | Built-in UDFs: scalar (`get`/`get_or_default`/`l2_norm`/`ip`/`random_vec`/`uuid`/`array_contains*`) and table (`call_service`/`call_sqlrec_api`/`window_diversify`/`dpp_diversity`/`rule_diversity`/`dedup`/`shuffle`/`weighted_merge`/`json_to_table`/`add_col`/`truncate_table`/`tag_to_vec`/`sleep`/`get_variables`/`set_variables`/`get_growthbook_features`/`feature_coverage_metrics`, etc.) |
| **sqlrec-connectors** | 6 datasource connectors with dual-stack adaptation: Calcite side (`*CalciteTable(+Factory)` implementing the `HmsTableFactory` SPI) + Flink side (`*DynamicTableFactory` implementing the Flink `Factory` SPI, redis/milvus only) |
| **sqlrec-model** | Model backends implementing the `ModelController` SPI: GBDT family (XGBoost/LightGBM/CatBoost, Python training + ONNX/CatBoost C++ serving), TZRec family (DSSM/Wide&Deep), External (pass-through to external URLs). Includes K8s YAML generation (`K8sYamlBuilder`) |
| **sqlrec-frontend** | Three entry points: Thrift (Hive2 protocol, compatible with beeline/kyuubi/JDBC), REST (Netty, `/sql/v1`, `/api/v1/*`, `/metrics`, `/ui/*`), CLI (JLine REPL). Session management, operation lifecycle, Prometheus metrics |
| **sqlrec-demo** | Dependency-free quick-start demo plus MovieLens SQL definitions for a complete recall → ranking → diversification pipeline |
| **sqlrec-flink** | Flink environment integration tests (connector, BatchCallServiceUDTF, etc.) |
| **sqlrec-ui** | Frontend static resources (pom placeholder only) |

### Runtime Panorama (Journey of a SQL Statement)

```
 Client (beeline / kyuubi / curl / cli.sh)
    │
    ├─ Thrift:30000 ──► TCLIServiceImpl ─► SessionManager ─► SqlExecutor.executeSqlAsync()
    ├─ REST:30001 ────► Netty HttpServerHandler ─► RestSqlExecutor / RestFunctionExecutor
    └─ CLI ───────────► Cli (JLine) ─► RestFunctionExecutor / SqlExecutor
    │
    ▼
 CompileManager.parseFlinkSql()  ── SqlPreProcesser preprocessing → FlinkSqlParserImpl parsing
    │
    ▼
 SqlExecutor.executeSqlAsync() statement dispatch:
    ├─ SQL function compilation state machine (FunctionCompiler) ── CREATE SQL FUNCTION
    │     multi-statement session-based compilation
    ├─ USE DATABASE / FLUSH / SET
    ├─ Resource queries (SHOW *, DESCRIBE, SHOW CREATE)
    ├─ Compilable CRUD (SqlTypeChecker.isFlinkSqlCompilable)
    │      └─► CompileManager.compileSql()
    │             ├─ SqlCallSqlFunction → FunctionProxyBindable (function proxy,
    │             │                                    supports async/PARTITION BY)
    │             ├─ SqlIfCache        → IfBindable (conditional branch)
    │             ├─ SqlCache          → CacheTableBindable (cache table materialization)
    │             ├─ SqlAssert         → AssertBindable (assertion)
    │             ├─ SqlSet            → SetBindable (variables)
    │             └─ plain SQL         → NormalSqlCompiler (full Calcite pipeline)
    │                                       parse → validate → sql2rel → VolcanoPlanner
    │                                       → EnumerableRelImplementor → Janino → CalciteBindable
    ├─ Resource editing (CREATE/DROP/TRAIN/EXPORT MODEL, SERVICE, API, SQL FUNCTION)
    │      └─► ModelManager / ServiceManager → K8sManager.applyYaml / MetadataAccess
    └─ Others (e.g. streaming DDL like CREATE TABLE)
           └─► return null → SessionManager forwards to remote Flink SQL Gateway via ClientProxy
    │
    ▼
 bindable.bind(schema, context)  ── SqlRecDataContextImpl provides the Calcite DataContext;
    │                              execution results materialized as CacheTable registered
    │                              into the schema (referenced by subsequent statements)
    ▼
 ThriftUtils.convertObjectArrayToTRowSet / JsonUtils.toJson → return to client
```

## Service Entry Layer (sqlrec-frontend)

### Startup Flow

`Main.main()` (`sqlrec-frontend/src/main/java/com/sqlrec/frontend/Main.java`):

1. Validates that at least one of `ENABLE_REST_SERVER` / `ENABLE_THRIFT_SERVER` is true;
2. Each enabled server starts in its own thread (`startServer`); if any server exits, the main latch counts down and the process calls `System.exit(1)` (fail-fast).

Both servers perform the same three-step initialization at startup: `FunctionUpdater.initFunctionUpdateService()` (periodic function hot-update, remote metadata mode only), `PrometheusMetricsUtils.initMetrics()`, and `CalciteSchemaFactory.createCalciteSchema()` (schema warm-up).

### Thrift Service (Hive Protocol Compatible)

- `ThriftServer.java`: `TThreadPoolServer` + `TBinaryProtocol`, exposing the Hive `TCLIService` (port 30000). **One thread per connection**; worker limit not explicitly configured.
- `TCLIServiceImpl` delegates to `SessionManager`. Sessions use four `ConcurrentHashMap`s: `clientMap` (remote Flink Gateway proxy), `sqlExecutorMap` (one `SqlExecutor` per session), `operationMap`, `operationToSessionMap`.
- **Local/remote execution decision** (`SessionManager`):
  1. Synchronously invoke `sqlExecutor.executeSqlAsync()`;
  2. If it returns non-null and (local file metadata mode OR the statement is not `USE`/`SET`) → wrap as a local `SqlOperation`;
  3. Otherwise forward via `ClientProxy` to the remote Flink SQL Gateway (`FLINK_SQL_GATEWAY_ADDRESS:30018`) — i.e., `USE`/`SET` apply to both local and remote sessions; unrecognized DDL (e.g. `CREATE TABLE` for Kafka streaming tables) is executed by Flink and written back to HMS.
- `SessionTimeoutChecker`: single-threaded scheduled executor that scans `getLastAccessTime()` at `SESSION_CHECK_INTERVAL` (default 5 minutes); sessions exceeding `SESSION_IDLE_TIMEOUT` (default 30 minutes) are closed by `cleanupSession`.
- Result fetching: `FetchResults` returns all rows at once (`hasMoreRows` always false); after fetching, `setEnumerable(null)`.

### REST Service

- `RestServer.java`: Netty `NioEventLoopGroup(1)` + default worker group, `HttpServerCodec` + `HttpObjectAggregator(65536)` + `HttpServerHandler` (port 30001).
- Routing (`HttpServerHandler.java`):
  - `POST /sql/v1` → `RestSqlExecutor` (controlled by the `ENABLE_REST_SQL_API` switch): parses `RequestData{sqls, params, metricTags}`, **creates a new `SqlExecutor` per request**, executes SQL sequentially, returns `ExecuteDataList`;
  - `POST /api/v1/<name>` → `RestFunctionExecutor`: looks up `SqlApi` by API name → `CompileManager.getApiBindSqlFunction()` compiles/gets cached function → bind and execute;
  - `GET /metrics` → Prometheus scrape;
  - `/ui/*` → UI static resources and UI API.
- Each request records micrometer timer/counter (tagged by path/method/status).

### CLI

`Cli` supports `-e` (execute statement), `-f` (execute file), and interactive REPL (JLine3, `SqlLineParser` for multi-line SQL assembly, `SqlHighlighter` for syntax highlighting, `SqlOutputFormatter` for tabular output).

## SQL Language Layer (sqlrec-sql-parser)

### Extension Mechanism

Reuses the Flink parser codegen pipeline: `config.fmpp` + `Parser.tdd` (declares the generated class as `FlinkSqlParserImpl`, imports `com.sqlrec.sql.parser.*`) + `parserImpls.ftl` (grammar production templates) + `compoundIdentifier.ftl`/`sqlRecIdentifier.ftl`. Extended keywords: `FLUSH`, `ASSERT`, `SERVICE`, `MODEL`, `TRAIN`, `EXPORT`, `CHECKPOINT`, `CACHE`, `PARTITION`, `LIKE`, `FUNCTION`, `API`, etc.

### Extended Statements

| Statement | AST Class | Semantics |
| --- | --- | --- |
| `CREATE MODEL ... / DROP MODEL` | `SqlCreateModel` / `SqlDropModel` | Define a model (input/output fields, algorithm params, paths) |
| `TRAIN MODEL m CHECKPOINT c FROM table WHERE ...` | `SqlTrainModel` | Trigger a K8s training Job |
| `EXPORT MODEL m CHECKPOINT = c [ON table] [WHERE ...]` | `SqlExportModel` | Triggers an export Job from a training checkpoint |
| `ALTER MODEL m DROP CHECKPOINT c` | `SqlAlterModelDropCheckpoint` | Clean up a checkpoint |
| `CREATE SERVICE s ON MODEL m [CHECKPOINT = c]` | `SqlCreateService` | Deploys an online inference Deployment |
| `CREATE SQL FUNCTION f ...` | `SqlCreateSqlFunction` | Begin a multi-statement function definition |
| `DEFINE INPUT TABLE t LIKE other / (col type...)` | `SqlDefineInputTable` | Declare a function input table schema |
| `RETURN [t / SELECT ... / CALL ...]` | `SqlReturn` | Return no data, a cache table, query, or synchronous call; top-level RETURN terminates the definition, while RETURN in IF exits early |
| `CALL f(t1, t2) [LIKE ...] [ASYNC] [PARTITION BY t SIZE n]` | `SqlCallSqlFunction` | Invoke a SQL/Java function, supports async and partitioned parallelism |
| `CACHE TABLE t AS SELECT ... / CALL ...` | `SqlCache` | Materialize a cache table |
| `IF [TIMEIN] (cond) THEN (stmt) [ELSE (stmt)]` | `SqlIfCache` | Conditionally execute CACHE, CRUD, ASSERT, CALL, SET, or RETURN; with a positive timeout, TIMEIN falls back to ELSE on timeout or failure |
| `ASSERT SELECT ...` | `SqlAssert` | Non-empty assertion (data quality gate) |
| `CREATE API a WITH f` | `SqlCreateApi` | Publish a function as a REST API |
| `FLUSH` | `SqlFlush` | Invalidate all caches |

All AST nodes implement `unparse` (via `SqlUnparseUtils`), supporting `SHOW CREATE` round-trip and normalized storage.

### Preprocessing (SqlPreProcesser)

`SqlPreProcesser.java` (`sqlrec-core/src/main/java/com/sqlrec/compiler/SqlPreProcesser.java`) performs dialect compatibility before parsing: `use default` casing, `set k=v` → `set 'k'='v'`, etc.

## Compilation Subsystem (sqlrec-core/compiler)

### CompileManager (Compilation Entry and Dispatch)

`CompileManager.java` (`sqlrec-core/src/main/java/com/sqlrec/compiler/CompileManager.java`):

- `parseFlinkSql()`: preprocessing + Flink conformance parsing (Lex.JAVA).
- `compileSql()`: first validates via `SqlTypeChecker.isFlinkSqlCompilable()`, then dispatches by AST type to 6 Bindable constructors (see §6).
- **SQL function cache**: `static ConcurrentHashMap<String, SqlFunctionBindable> functionBindableMap` (process-level shared); `Caffeine sqlApiCache` caches API name → function name mapping (TTL = `SCHEMA_CACHE_EXPIRE`, default 60s).
- **Circular dependency detection**: instance field `compilingSqlFunctions` (ArrayList chain) records the compilation stack; `compileSqlFunction()` detects cycles during recursive compilation.
- `getApiBindSqlFunction()`: API name → SqlApi → function name → `getSqlFunction()` (cache hit or compile).

### FunctionCompiler (Multi-Statement Function Compilation State Machine)

`FunctionCompiler.java` (`sqlrec-core/src/main/java/com/sqlrec/compiler/FunctionCompiler.java`). SQL functions are submitted **as multiple independent statements** in a session, driven by a state machine:

```
FUNCTION_DEFINITION (CREATE SQL FUNCTION f)
   → FUNCTION_PARAM (0..n DEFINE INPUT TABLE)
   → FUNCTION_BODY (any SQL / CACHE / CALL / IF, including early RETURN inside IF)
   → FUNCTION_RETURN (top-level RETURN / RETURN t / RETURN SELECT / RETURN CALL)
   → isFunctionCompileFinish
```

- Input tables without `LIKE` are registered into a temporary schema as placeholder `CacheTable(enumerable=null)` so that function body SQL passes validation;
- Tables of `CACHE` statements inside the function body are likewise registered as placeholder CacheTables (named with sequence numbers when duplicated);
- RETURN inside IF participates in result-schema validation but does not advance the state machine. Only a top-level RETURN terminates the definition, and all return points must be either empty or use the same schema;
- Upon completion, `SqlExecutor.saveSqlFunction()` persists it (JSON statement list) to the metadata database and `CacheManager.invalidateAll()` immediately evicts old compilation artifacts.

### NormalSqlCompiler (Full Calcite Compilation Pipeline)

`NormalSqlCompiler.java` (`sqlrec-core/src/main/java/com/sqlrec/compiler/NormalSqlCompiler.java`):

1. Calcite parser (Lex.MYSQL, DEFAULT conformance) re-parsing (since Flink AST and Calcite AST are not fully compatible, unparse to SQL string then re-parse);
2. `RootFirstCatalogReader` (in-house, root schema first lookup) + `SqlValidator` validation;
3. `SqlTypeChecker.isSqlContainKvTable()` determines whether KV tables are involved (decides whether to load KV-specific rules);
4. `RuleManager.createPlanner()` creates the VolcanoPlanner; `SqlToRelConverter` (trim unused fields, expand, inSubQueryThreshold) conversion;
5. `Programs.sequence(subQuery, custom program, calc)` optimization (`getProgram()`);
6. `EnumerableRelImplementor.implementRoot()` generates the Java expression tree;
7. `EnumerableInterpretable.toBindable()` (Janino compilation) produces `CalciteBindable`, while retaining logical plan / physical plan / Java source for logging and debugging.

### SqlTypeChecker (Compilability Determination)

`SqlTypeChecker.java` (`sqlrec-core/src/main/java/com/sqlrec/compiler/SqlTypeChecker.java`): recursively determines whether a statement can execute on local Calcite — the top level must be SELECT/INSERT/UPDATE/DELETE/ORDER BY/UNION (or control-flow nodes recursively determined), and all referenced tables must resolve to `SqlRecTable` (Calcite tables provided by connectors). This determination also decides "local execution vs forwarding to Flink Gateway".

### FunctionUpdater (Function Hot-Update)

`FunctionUpdater.java` (`sqlrec-core/src/main/java/com/sqlrec/compiler/FunctionUpdater.java`): single-threaded scheduled executor (default 300s period), traverses the dependency graph for each function in `functionBindableMap` (`inProgress` set prevents cycles):

- Function in DB has `updatedAt > bindable.createTime` → recompile;
- Dependent function updated/deleted → cascading recompile; function deleted from DB → removed from cache;
- Dependent tables' (minus function input placeholder tables) HMS update time later than `createTime` → recompile;
- Dependent Java UDF class name changed/missing → recompile (todo: cannot detect bytecode changes with the same class name).

## Execution Subsystem (executor + runtime)

### SqlExecutor (Statement Dispatch Hub)

`SqlExecutor.java` (`sqlrec-core/src/main/java/com/sqlrec/executor/SqlExecutor.java`). One instance per session/REST request, holding:

- `schema`: built by `CalciteSchemaFactory.createCalciteSchema()` at startup;
- `context`: `ExecuteContextImpl` (variable table, metrics tags, function call stack, cancellation flag);
- `defaultSchema`: mutable via `USE DATABASE`;
- `functionCompiler`: in-session state for multi-statement function compilation.

`executeSqlAsync()` dispatch order: function compilation state machine → `USE` → `FLUSH` (invalidate all caches) → resource queries (SHOW/DESC family) → compilable CRUD (compile + bind + assemble `SqlProcessResult`) → resource editing (CREATE/DROP/TRAIN/EXPORT MODEL, SERVICE, API, SQL FUNCTION, followed by `CacheManager.invalidateAll()`) → fallback returns null (forwarded to Flink Gateway by the frontend).

`executeSql()`: synchronous wrapper, polls `result.isCompleted()` + `SQL_SYNC_EXECUTE_TIMEOUT` (default 180s) timeout.

### Bindable System (Runtime Execution Tree)

All executable units implement `BindableInterface` (`bind(schema, context)` returns `Enumerable<Object[]>`, exposing metadata such as return fields/read-write tables/cache table names/dependent functions):

```
BindableInterface
 ├─ CalciteBindable        Plain SQL: wraps the Calcite Bindable; bind() creates SqlRecDataContextImpl,
 │                          executes and fully materializes results into a List (column-width
 │                          adaptive single/multi column)
 ├─ CacheTableBindable     CACHE statement: executes child Bindable → registers result as a
 │                          CacheTable in the schema; returns one row (table_name, count)
 ├─ FunctionProxyBindable  CALL statement proxy: static binding (delegate) or runtime function
 │                          name resolution via variables; supports ASYNC (submit to virtual
 │                          thread and return null) and PARTITION BY t SIZE n (split by partition
 │                          table row count, clone context, independent schema per partition,
 │                          concurrent execution and merge)
 ├─ SqlFunctionBindable    SQL function body: dependency-aware serial/parallel node execution;
 │                          nodes not yet executed are skipped after RETURN
 ├─ ReturnBindable         RETURN statement: returns a cache table or delegates SELECT/CALL,
 │                          then writes the result to the current function frame's ReturnState
 ├─ ProxyAllBindable       Node execution wrapper for timeout, cancellation, tracing, metrics,
 │                          and logging; uniformly recovers ignorable failures using
 │                          isIgnoreException() and cache-table metadata
 ├─ CallSqlFunctionBindable Function call binding (validates input table existence)
 ├─ JavaFunctionBindable   Java table UDF binding
 ├─ IfBindable             Conditional branch: condition is a one-row, one-column SELECT;
 │                          supports CACHE, CRUD, CALL, SET, ASSERT, and RETURN; TIMEIN runs THEN
 │                          with isolated ReturnState and discards it before fallback on failure
 ├─ AssertBindable         Assertion: throws an exception if the SELECT result is empty
 └─ SetBindable            Variable setting (context.setVariable)
```

**Context propagation**: `ExecuteContextImpl.clone()` shares variableMap/metricsTagMap (bidirectionally visible between parent and child), copies funNameStack (cycle detection), and holds a parent reference to propagate the cancellation flag up the parent chain (`isCancelled()` traverses upward).

**Parallel execution**: `ExecutorServiceUtils` provides a **global virtual thread pool** (`newVirtualThreadPerTaskExecutor`), used for CACHE timeout control, ASYNC CALL, and partition parallelism.

### Execution Results and CacheTable

- `SqlProcessResult{enumerable, fields}` + `isCompleted()`;
- `CacheTable` (common) = `ScannableTable` + in-memory `Enumerable` + field list + createSql; once registered into the session schema, subsequent SQL can directly reference it via `SELECT * FROM cache_table`;
- `FLUSH` / any resource editing / function save → `CacheManager.invalidateAll()` uniformly invalidates the four major caches (CalciteSchemaFactory, JavaFunctionUtils, CompileManager, ServiceManager).

## Calcite Integration (rules + node)

### Optimization Rules

`RuleManager.java` (`sqlrec-core/src/main/java/com/sqlrec/rules/RuleManager.java`) `createPlanner(boolean addKvTableRules)`:

- Registers default traits (Convention, optional Collation) + Calcite default rules (interpreter disabled);
- **Always** replaces: `ENUMERABLE_TABLE_MODIFICATION_RULE → SqlRecTableModifyRule` (write path uses in-house modify nodes), `ENUMERABLE_UNION_RULE → SqlRecUnionRule`;
- **When KV tables present**, additionally adds: `FILTER_SCAN/FILTER_INTERPRETER_SCAN` (predicate pushdown to table scan), `FILTER_INTO_JOIN`, `KV_JOIN` (replaces EnumerableJoin/MergeJoin and removes interfering rules like JOIN_COMMUTE), `VECTOR_JOIN` (two variants with/without filter);
- Connectors can inject custom rules via `HmsTableFactory.getRules()` (`addTableFactoryRules`).

### Physical Operators (node package)

| Operator | Description |
| --- | --- |
| `FilterableTableScan` | Table scan carrying pushed-down predicates (filter entry for KV/vector tables) |
| `SqlrecEnumerableKvJoin` | KV join: left table fully collected join keys → right table `SqlRecKvTable.getByPrimaryKey()` batch point lookup (or per-key filter scan for non-primary keys) → string key map merge (`MergeUtils.snakeMerge`); supports LEFT join null padding |
| `SqlrecEnumerableVectorLookupJoin` | Two-input vector lookup join: enumerates the left input and performs filtered ANN lookups against the right `VectorSearchable` table (`DEFAULT_VECTOR_SEARCH_LIMIT` default 100) |
| `SqlrecEnumerableUnion` | Union merge (with `IGNORE_UNION_EXCEPTION` branch degradation) |
| `SqlrecEnumerableTableModify` | INSERT/UPDATE/DELETE write path: dispatched to connector write interfaces (batch upsert/delete) |

### KV Join Data Flow (KvJoinUtils)

`KvJoinUtils.kvJoin()` (`sqlrec-core/src/main/java/com/sqlrec/utils/KvJoinUtils.java`):

1. Left table materialized into a List, join key set extracted (nulls skipped);
2. If the join key column = right table primary key → `getByPrimaryKey(joinKeys)` one batch point lookup; otherwise **per key** construct an `EQUALS` filter and call `scan(filter)` (N+1 access pattern);
3. Right table results build a HashMap keyed by `key.toString()` (avoids Integer/Long equals semantic differences, but introduces cross-type string collisions);
4. Assembled by join type (LEFT pads null on no match), merged output via `snakeMerge`.

## Metadata Subsystem (db + schema)

### Dual-Mode Architecture

`MetadataAccessFactory.java` (`sqlrec-core/src/main/java/com/sqlrec/db/MetadataAccessFactory.java`) selects based on whether `SQL_SCHEMA_DIR` is empty:

| | Local File Mode | Remote Mode |
| --- | --- | --- |
| SchemaAccess | `SqlFileSchemaAccess` (`SqlFileParser` parses table creation/UDF from SQL files in the directory) | `HmsSchemaAccess` (thrift access to HMS) |
| StoreAccess | `InMemoryStoreAccess` (function/API/model nodes in memory; service nodes replayed at startup) | `DbStoreAccess` (MyBatis + PostgreSQL) |
| HdfsAccess | `LocalHdfsAccess` (local directory) | `RemoteHdfsAccess` (Hadoop client) |

`MetadataAccess = SchemaAccess + StoreAccess + HdfsAccess` aggregated facade, all static singletons (DCL).

### HmsClient

`HmsClient.java` (`sqlrec-core/src/main/java/com/sqlrec/db/remote/HmsClient.java`): `HiveMetaStoreClient` static cache (DCL + volatile + JVM shutdown hook); `withRetry()` rebuilds the client after invalidation on `TTransportException` and retries once (read idempotent); **all public methods are `synchronized static`** — HMS access is globally serialized within the process.

### Schema Assembly and Caching

- `CalciteSchemaFactory.java` (`sqlrec-core/src/main/java/com/sqlrec/schema/CalciteSchemaFactory.java`): `createCalciteSchema()` builds the root schema: in remote mode, attaches an `HmsSchema` per database based on `databaseListCache` (`ObjCache`, TTL 60s, async refresh); when `globalSchema` (set at server startup) exists, directly reuses its subSchema mapping (fast path).
- `HmsSchema.java` (`sqlrec-core/src/main/java/com/sqlrec/schema/HmsSchema.java`): per-db two-level `ObjCache` — `tableMapCache` (incremental: only rebuilds `Table` objects whose `getTableUpdateTime` changed, reuses old instances otherwise) and `functionMapCache` (HMS functions + `FunctionConfigs.DEFAULT_SCALAR_FUNCTION_CONFIGS` built-in UDFs → `UdfManager.createScalarFunction` reflection).
- **`ObjCache`** (`sqlrec-core/src/main/java/com/sqlrec/utils/ObjCache.java`): minimal cache with TTL + optional async refresh. `getObj()` triggers `updateObj()` on expiry (synchronized): first load synchronous; afterwards submitted to a **global single-thread** executor for async refresh and immediately returns the old value; `invalidate()` clears.

### Table Abstraction System (common/schema)

```
AbstractTable
 └─ SqlRecTable (name, createSql)
     ├─ CacheTable            in-memory result table (ScannableTable)
     ├─ SqlRecKvTable         KV table abstraction: getByPrimaryKey(Set)/scan(filters) —
     │                          implemented by Redis/JDBC/Mongo
     │                          (point lookup vs full scan decided by connector config)
     └─ (implements VectorSearchable) vector retrieval table: Milvus
```

`HmsTableFactory` (SPI, registered via `META-INF/services`): maps an HMS table's storage handler / serde / parameters to the corresponding connector's Calcite Table + optional custom optimization rules + Flink DynamicTableFactory. Connectors are discovered via SPI (`TableFactoryUtils`).

## Storage Connector Layer (sqlrec-connectors)

The six connectors share the same structure: `config/` (Options parsing of table properties) + `calcite/` (CalciteTable + Factory) + `handler/` (storage access) + (redis/milvus) `flink/` (Flink DynamicTable Source/Sink).

| Connector | Read Path | Write Path | Key Points |
| --- | --- | --- | --- |
| **redis** | `RedisCalciteTable` (KV table): primary key `get/mget/lrange`; non-primary key full scan with filter | `RedisSinkTableFunction` (lpush/setex etc., codec: String/Json) | `RedisWrapper`/`RedisClusterWrapper` static connection pools (per URL); pipeline serialized with a global lock (autoFlush is connection-level global); released via shutdown hook; Flink side has an additional Lookup source |
| **jdbc** | `JdbcCalciteTable`: `JdbcHandler.scan(filters)` generates select with where | upsert/delete (including `upsertBatch`/`deleteBatch`) | HikariCP pool (key = url+user+driver+schema+password, credential rotation auto-evicts old pools); primary key point lookup uses IN batch |
| **mongodb** | `MongoCalciteTable` (KV table) | same | `MongoHandler` |
| **milvus** | `MilvusCalciteTable` implements `VectorSearchable`: ANN retrieval | `MilvusDynamicTableSink` (Flink write) | Right table for vector join; Flink DynamicTableFactory dual-stack |
| **kafka** | `KafkaCalciteTable` | — | Streaming table (with Flink Gateway DDL) |
| **filesystem** | `FileSystemCalciteTable` | truncate | Local/HDFS file table |

**Guava version alignment**: root pom unifies to 32.1.3 (hive-metastore brings 19.0 which conflicts with milvus/grpc).

## UDF Subsystem (sqlrec-udf + core/udf)

### Registration and Loading

- **Built-in UDFs**: `FunctionConfigs.DEFAULT_SCALAR_FUNCTION_CONFIGS` statically registers scalar function names → class names; table functions are looked up via `JavaFunctionUtils.getTableFunction()` (Java functions registered in HMS or built-in).
- **Dynamic Java UDFs**: `UdfManager`/`JavaFunctionUtils` load from the HMS function registry by className via reflection; `isJavaFunctionModifiedSince` supports hot-update (class name level).

### HTTP UDFs (Online Inference Core)

- `CallServiceFunction.java` (`sqlrec-udf/src/main/java/com/sqlrec/udf/table/CallServiceFunction.java`) (`call_service`): `ServiceManager.getServiceConfig()` gets the service URL (ObjCache cached) → input table serialized as JSON array → POST → parse prediction map → `mergePredictions` merges predictions back into input rows by column name. Static `OkHttpClient` (30s three-stage timeout, volatile for testability).
- `CallServiceWithQVFunction`, `BatchCallServiceUDTF` (Flink UDTF, JDK HttpURLConnection, batched by batchSize), `CallSqlRecApiFunction` (cross-instance sqlrec invocation via `SqlRecApiClient`).
- **Diversification family**: `WindowDiversify` (tag quota within sliding window), `DppDiversity` (determinantal point process), `RuleDiversity`, `WeightedMerge`, `DedupFunction`, `ShuffleFunction`.
- **Feature family**: `TagToVecFunction`, `RandomVecFunction`, `FeatureCoverageMetricsFunction`, `GetGrowthbookFeaturesFunction` (GrowthBook SDK), `Get/SetVariablesFunction` (variable propagation).
- Others: `JsonToTableFunction`, `AddColFunction`, `TruncateTableFunction`, `SleepFunction` (for testing).

## Model Subsystem (sqlrec-model + core/model + k8s)

### Lifecycle State Machine

```
CREATE MODEL ──► validation (ModelController.checkModel: fields/params/input-output duplicates)
                     │
TRAIN MODEL ──► (same checkpoint and CREATED → idempotent return of existing; otherwise
                delete old first)
                genModelTrainK8sYaml → injectPodConfig(params)
                → insertCheckpoint(status=CREATED) → K8sManager.applyYaml (training Job)
                     │  (client polls)
isCheckpointOperationCompleted:
   checkJobsStatusFromYaml → succeeded → status=SUCCEEDED, deleteYaml(Job)
                          → failed    → status=FAILED
                     │
EXPORT MODEL ─► genModelExportK8sYaml (export Job: training checkpoint → serving format)
                → export checkpoint(status=CREATED→…→SUCCEEDED)
                     │
CREATE SERVICE ► validate checkpoint=SUCCEEDED + legal type
                → getServiceK8sYaml (ConfigMap+Service+Deployment) → applyYaml
                → isDeploymentReadyFromYaml (Ready replica validation)
                     │
Online inference ─► call_service('service_name', input_table) UDF → HTTP POST
```

- **Checkpoint protection**: checkpoints/models referenced by a Service cannot be deleted (`deleteCheckpoint`/`deleteModel` pre-validation).
- **Path safety**: `PathUtils.validateModelPath` prevents out-of-bound deletion.

### ModelController SPI and Backends

`ModelController` (common, SPI: `META-INF/services`) is dispatched by `ModelControllerFactory` based on `engine`:

| Backend | Implementation | Training | Serving |
| --- | --- | --- | --- |
| xgboost / lightgbm / catboost | `XGBoostModel`/`LightGBMModel`/`CatBoostModel` (extends `GbdtModelBase`) | K8s Job runs Python (`train_*.py`) | `onnx_server.cpp` / `catboost_server.cpp` (cpp-httplib) |
| dssm / wide_and_deep | `DSSMModel`/`WideAndDeepModel` (extends `TzrecModelBase`) | K8s Job runs tzrec (`server.py`) | `tzrec/server.cpp` |
| external | `ExternalModel` | — | Direct pass-through to external URL |

`K8sYamlBuilder` (model/common) uniformly generates ConfigMap/Service/Deployment (container ports, resource request/limit, replicas, `REPLICAS`/`LABEL_COLUMNS` etc. env); `GbdtK8sYamlUtils`/`TzrecK8sYamlUtils` generate training/export Job YAML (including HDFS mounts, Python images).

### K8sManager

`K8sManager.java` (`sqlrec-core/src/main/java/com/sqlrec/k8s/K8sManager.java`): fabric8 client static cache (DCL + shutdown hook); `applyYaml` (serverSideApply), `deleteYaml` (check existence before delete, supports Deployment/Service/ConfigMap/Secret/Pod/Job), `checkJobsStatusFromYaml`, `isDeploymentReadyFromYaml`; transport-level/5xx failures automatically `resetClient()` rebuild (4xx business errors do not rebuild).

## Demo and Programming Model (sqlrec-demo)

`src/main/sql/quick_start/` provides a dependency-free experience using in-memory filesystem connector tables. Its global objects consistently use the `demo_` prefix (`demo_user_interest_category`, `demo_category_hot_item`, `demo_exposure_item`, and `demo_rec`). The complete MovieLens recommendation pipeline lives under `src/main/sql/movielens/`:

1. **Tables** (table/): `user_table`/`item_table`/`item_embedding`/`rec_log_kafka` (streaming exposure logs), etc.;
2. **Features** (function/): `recall_fun` (multi-way recall: `itemcf_i2i` + `genre_hot_item` + `user_interest_genre`...), `rank_fun` (join features → `call_service('rank_service')`), `diversify_fun` (`window_diversify`), `main_rec` (orchestration: `get_or_default` selects branch by variable → recall → ranking → diversification → async `save_rec_item` persistence);
3. **Models** (model/): `recall_model` (DSSM dual-tower), `rank_model` (GBDT);
4. **Services** (service/): `recall_service_user/item`, `rank_service` (deployed bound to exported checkpoint);
5. **APIs** (api/): `main_rec` published as a REST API.

Programming model essentials: `IF` expresses conditional execution; for a positive timeout, `IF TIMEIN` applies a millisecond timeout to THEN and falls back to ELSE on failure; `IF + RETURN` supports early function exit; `CALL ... ASYNC` expresses side-effect persistence; `ASSERT` provides data gating; variables (`get_or_default`/`SET`) provide request-level parameter passing.

## Observability

- **Metrics**: Micrometer composite registry + Prometheus `/metrics` (HTTP request timer/counter, session/operation gauges, operation switch counters, cache-table ignored-exception counters, function update timers, etc.); deploy/prometheus provides ServiceMonitor + Grafana JSON.
- **Trace**: OpenTelemetry (OTLP gRPC → Jaeger), `DEBUG_TRACE` switch + `TRACE_ENDPOINT/HEADERS/SERVICE_NAME`; `TraceUtils` instruments at node execution points.
- **Logging**: log4j2; the compilation pipeline logs the complete logical/physical plan and generated Java source at INFO level.

## Thread Model and Concurrency Design Summary

| Thread Resource | Type | Purpose |
| --- | --- | --- |
| `ExecutorServiceUtils` virtual thread pool | global, unbounded | CACHE timeout execution, ASYNC CALL, PARTITION parallelism |
| `ObjCache.executorService` | global, single-threaded | Async refresh for all ObjCaches (shared) |
| `FunctionUpdater.executor` | single-threaded scheduled | Function hot-update checks (300s) |
| `SessionTimeoutChecker.timeoutChecker` | single-threaded scheduled | Session timeout (5min check) |
| Thrift `TThreadPoolServer` | one platform thread per connection | RPC handling (including synchronous SQL execution) |
| Netty event loop | boss(1)+worker(default) | REST handling (**business executes synchronously on the event loop**) |
| Static clients | — | HmsClient (globally synchronized), K8sManager, RedisWrapper/JdbcHandler connection pools |

Static mutable state: `CompileManager.functionBindableMap/sqlApiCache`, `CalciteSchemaFactory.schemaMap/globalSchema`, `ServiceManager.serviceConfigCacheMap`, connector static connection pools, `HmsClient.client`, `K8sManager.kubernetesClient`.

## Deployment Architecture (deploy/ + bin/ + docker/)

- `deploy_minikube.sh` + `deploy_components.sh`: minikube one-stop deployment (HMS, PostgreSQL, Redis (standalone/cluster), Milvus, MongoDB, Kafka, HDFS, Flink SQL Gateway, Kyuubi, GrowthBook, Jaeger, Prometheus/Grafana, DolphinScheduler, ClickHouse, OpenSearch, Jupyter...), each component with independent `deploy.sh/uninstall.sh` + PVC.
- `deploy/sqlrec/sqlrec.yaml`: sqlrec engine Deployment (image built by `build_sqlrec_docker.sh`, Dockerfile based on the release package).
- `bin/`: `beeline.sh` (Thrift client), `kyuubi.sh`, `server.sh`, `cli.sh`, `sqlrec.cmd`.
- `benchmark/`: movielens (end-to-end recall pipeline + wrk load test request.lua) and criteo1m benchmarks.

## Key Configurations (SqlRecConfigs)

| Config | Default | Description |
| --- | --- | --- |
| `THRIFT_SERVER_PORT` / `REST_SERVER_PORT` | 30000 / 30001 | Service ports |
| `REST_BUSINESS_EXECUTOR_THREADS` | 16 | Number of REST business executor threads |
| `REST_BUSINESS_MAX_PENDING_TASKS` | 32 | Maximum pending tasks per REST business EventExecutor; minimum 16 |
| `ENABLE_REST_SERVER/SQL_API/UI_API/THRIFT_SERVER` | true | Entry switches |
| `PARALLELISM_EXEC` | true | Parallel execution |
| `NODE_EXEC_TIMEOUT` | 0 (unlimited) | Execution timeout for timeout-capable nodes (ms) |
| `FUNCTION_UPDATE_INTERVAL` | 300s | Function hot-update period |
| `SCHEMA_CACHE_EXPIRE` / `ASYNC_SCHEMA_UPDATE` | 60s / true | Schema cache |
| `SQL_SCHEMA_DIR` | empty (use HMS) | Local file metadata mode |
| `META_DB_URL/USER/PASSWORD/DRIVER` | PG | Metadata database |
| `HIVE_METASTORE_URI` | thrift://...:30008 | HMS |
| `FLINK_SQL_GATEWAY_ADDRESS/PORT` | .../30018 | Remote Flink Gateway |
| `SESSION_CHECK_INTERVAL` / `SESSION_IDLE_TIMEOUT` | 5min / 30min | Session management |
| `SQL_SYNC_EXECUTE_TIMEOUT` | 180s | Synchronous execution timeout |
| `IGNORE_UNION_EXCEPTION` / `IGNORE_JOIN_QUERY_EXCEPTION` | true | Degradation switches |
| `DEFAULT_VECTOR_SEARCH_LIMIT` | 100 | Default topK for vector retrieval |
