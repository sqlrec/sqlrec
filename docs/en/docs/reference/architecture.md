# SQLRec Architecture

SQLRec compiles and schedules SQL recommendation flows within the JVM, using UDFs for custom computation, Connectors for storage access, and HTTP for model inference. This page covers compilation, scheduling, state isolation, and external access.

## Core Components {#module-overview}

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

Arrows show module collaboration and runtime SPI discovery, rather than Maven dependencies. Connectors implement interfaces from `sqlrec-common`.

| Layer | Main implementation | Responsibility |
| --- | --- | --- |
| Service entry points | `sqlrec-frontend` | HTTP, Thrift/JDBC, CLI, UI, session management |
| SQL compilation | `sqlrec-sql-parser`, the `compiler` package in `sqlrec-core` | Parsing, query compilation, function compilation |
| Flow runtime | The `runtime` package in `sqlrec-core` | DAG scheduling, variables, returns, timeouts, cancellation |
| Data and function adapters | `sqlrec-connectors`, `sqlrec-udf` | Storage operators, scalar and table functions |
| Metadata and model management | The `db` and `model` packages in `sqlrec-core`, `sqlrec-model` | Definitions and caches, model tasks, inference services |
| Shared interfaces | `sqlrec-common` | Table schemas, contexts, extension contracts |

| Execution level | What it determines | Implementation |
| --- | --- | --- |
| SQL-function DAG | Dependencies and parallel execution across statements | SQLRec execution nodes and scheduler |
| Relational plan for one SQL statement | Scans, filters, joins, sorting, and other operations | Calcite + SQLRec storage operators |

## Request Entry Points and Execution Paths {#from-request-to-result}

### HTTP Recommendation API

```text
HTTP request
  → Netty I/O: decode
  → HttpServerHandler: handle on business threads
  → RestFunctionExecutor: resolve API and compiled function
  → Create root schema; bind input CacheTables and execution variables
  → SqlFunctionBindable: execute the function DAG
  → Materialize RETURN result → JSON response

Process-wide: API mappings, compiled function plans
Per request: input tables, intermediate tables, execution variables
```

| Step | Key implementation |
| --- | --- |
| Separate network and business threads | `RestServer` uses Netty I/O threads and a separate business executor group; node concurrency uses the engine executor |
| API → function | `SqlApiCache` holds name mappings; `CompileManager` / `SqlFunctionCache` retrieves compiled functions |
| Bind request data | `CalciteSchemaFactory` creates the root schema; JSON rows become input tables, and `params` populate `ExecuteContextImpl` |

### SQL Entry Points and Statement Dispatch

CLI, REST SQL, and Thrift sessions dispatch through `SqlExecutor`:

| Statement category | Execution path |
| --- | --- |
| SQL-function definition | Collect inputs and body, compile, and save the definition |
| Local queries, writes, and control statements | `CompileManager` → execution node → `bind` |
| Metadata-only DDL for databases, tables, and Java UDFs | Metadata adapters; remote mode writes HMS through HiveCatalog |
| SHOW, DESCRIBE | Read metadata or session cache tables |
| Model, checkpoint, and Service commands | Resource managers; submit external tasks when needed |
| Forwardable remote statements | Remote-mode Thrift session → Flink SQL Gateway |

Remote routing is considered only when local handling produces no result. Local compilation or execution errors return directly, without a Gateway retry.

## SQL Parsing and Compilation {#execution-model}

```text
One SQL statement · NormalSqlCompiler
  Parse and validate → Relational expressions → Optimize plan
    → Enumerable code generation → CalciteBindable

SQL function · FunctionCompiler
  Register schema placeholders → Compile each node → Check RETURN schema
    → Build dependency DAG → SqlFunctionBindable
```

### Parsing and Local Execution Checks

| Stage | Implementation and checks |
| --- | --- |
| Parse standard SQL | `SqlRecSqlParser` first tries Flink parsing |
| Parse extensions | Separate SQLRec grammar handles `CACHE TABLE`, function calls, model management, and other extensions |
| Check local capability | `SqlTypeChecker` checks statement types and table adapters; CTEs currently bypass the local query path |

Parsing, metadata persistence, and local execution are checked at separate stages.

### One SQL Statement: From Relational Plan to Executable Code

| Step | Key implementation and output |
| --- | --- |
| Name and type validation | Resolve table schemas and function signatures from the current schema |
| Plan optimization | `VolcanoPlanner` applies Calcite rules and SQLRec rules for KV joins, vector joins, writes, and UNION |
| Code generation | Convert to an Enumerable plan; `EnumerableInterpretable.toBindable()` compiles an executable object within the JVM |
| Execution and materialization | `CalciteBindable` binds the schema, variables, and parameters through `SqlRecDataContextImpl`, materializing results as row arrays |
| Diagnostics | Retain logical plans, physical plans, and generated code for the UI |

### SQL Functions: Build Schemas and Nodes at Compile Time

After compiling input declarations and cache-table statements, `FunctionCompiler` registers `CacheTable` placeholders with **schemas but no request data**, allowing later statements to validate. The top-level `RETURN` completes return-schema checks and DAG initialization.

| Execution node | Role |
| --- | --- |
| `CalciteBindable` | Local query or write |
| `CacheTableBindable` | Execute a child node and publish a named result table |
| `CallSqlFunctionBindable` / `JavaFunctionBindable` | SQL subfunction / Java `evaluate` call |
| `IfBindable` / `ReturnBindable` | Branch / return |
| `SqlFunctionBindable` | Nodes, schemas, and dependencies for the whole function |

Functions are compiled and validated when defined and may be recompiled when metadata loads. `SqlFunctionCache` reuses compiled results within the process; remote mode recompiles according to refresh policies.

| Call form | Target binding and schema handling |
| --- | --- |
| Static SQL subfunction | Bind at compile time and obtain its return schema; check compatibility when `LIKE` is present |
| Dynamic call | `FunctionProxyBindable` resolves the function name at runtime; `LIKE` declares the schema at compile time |

## UDF Registration and Execution

Java UDFs include **scalar functions** embedded in SQL expressions and **table functions** that process whole tables through `CALL`. SQL subfunctions are compiled by `FunctionCompiler` as described above.

### Registration and Execution Paths

```text
Function name → Java class name
  ├── Built-in: FunctionConfigs
  └── Custom: CREATE FUNCTION → Metadata
                       ↓ Load from the runtime classpath
Scalar function
  HmsSchema → UdfManager → SqlRecScalarFunctionImpl
    → Calcite type checking, overload resolution, and code generation
    → SQL expression result

Java table function
  CALL → FunctionBindableFactory → JavaFunctionUtils
    → JavaFunctionBindable → Inject arguments and invoke evaluate reflectively
    → CacheTable rows → Subsequent DAG nodes
```

| Type | Integration | Computation and use |
| --- | --- | --- |
| Scalar function | Adapt public `evaluate` methods as Calcite `ScalarFunction` implementations | Compute row values in expressions, such as `l2_norm` and `array_contains` |
| Java table function | `JavaFunctionBindable` wraps an execution node and selects an `evaluate` overload from static argument types | Batch processing of `CacheTable` inputs, literals, or variables, such as `dedup`, `weighted_merge`, and `call_service` |

Registration stores only the name-to-class mapping. Deploy the JAR and its dependencies on each SQLRec process's classpath beforehand; UDFs execute within the SQLRec JVM.

### Compile-Time Contracts and Runtime State

| Mechanism | Implementation and limits |
| --- | --- |
| Scalar-function context | `SqlRecCallNotNullImplementor` injects `DataContext` into generated calls without consuming an explicit SQL argument |
| Scalar-function determinism | Read the `deterministic` / `stateful` flags from `@UDFType` and propagate them to Calcite to prevent constant folding of nondeterministic calls |
| Table-function return schema | `LIKE` supplies the compile-time contract; without it, functions returning `CacheTable` run at compile time by default to infer fields. `ENABLE_JAVA_FUNCTION_SCHEMA_INFERENCE=false` disables this behavior |
| Table-function context | Automatically inject `ReadonlyContext` / `ExecuteContext` at runtime; a writable context makes the node nonparallel, as described in the next section |
| Table-function results | Materialize returned `CacheTable` rows and adapt them to the compiled fields; operations with no result may return `Void` and provide no result table |
| Instance state | `JavaFunctionBindable` holds the UDF instance, which may be reused across requests with the compiled plan; pass request state through arguments and contexts |

Use explicit `LIKE` for table functions that access the network or have side effects. See [Custom UDFs](../development/custom-udf.md) for implementation and deployment, and [CALL](./sql.md#call) for syntax.

## DAG Scheduling and Runtime Contexts

### Building Dependencies

`TopologicalSortUtils` reads each node's `getReadTables()`, `getWriteTables()`, and parallelism declaration in statement order:

| Constraint | Dependency |
| --- | --- |
| RAW: later read depends on earlier write | Wait for the table to be produced or updated |
| WAR: later write depends on earlier read | Finish reading before overwriting |
| WAW: later write depends on earlier write | Preserve write order for the same table |
| Execution barrier | Wait for earlier nodes; later nodes wait for the barrier |

`RETURN` and nonparallel nodes form barriers, including `SET`, `ASSERT`, and Java table functions declaring writable `ExecuteContext`. Using only `ReadonlyContext` does not impose this restriction. The graph undergoes topological sorting and redundant-edge removal; arbitrary external side effects inside Java UDFs are not inferred.

This function-body fragment assumes the subfunctions are defined, both recall paths return the same schema, have no additional shared writes, and allow parallel execution.

```sql
CACHE TABLE interest AS CALL interest_recall(user_info);
CACHE TABLE hot AS CALL hot_recall(user_info);
CACHE TABLE candidates AS
SELECT * FROM interest UNION ALL SELECT * FROM hot;
CACHE TABLE ranked AS CALL rank_items(candidates);
RETURN ranked;
```

```text
interest_recall → interest ─┐
                           ├→ UNION ALL → candidates → rank_items → ranked → RETURN
hot_recall ─────→ hot ──────┘

Recall paths run in parallel; merging waits for both, ranking waits for merging,
and RETURN forms an execution barrier.
```

SQL-subfunction read/write information maps back to the caller for outer dependency analysis. Rewriting a cache table also waits for earlier reads to finish.

### Parallelism, Partitioning, and Async Calls

| Mode | Implementation | Results and limits |
| --- | --- | --- |
| DAG parallelism | `SqlFunctionBindable` waits for dependencies with `CompletableFuture`, then submits nodes | `ExecutorServiceUtils` uses a virtual thread per task; disabling `PARALLELISM_EXEC` runs nodes sequentially |
| Partitioned calls | `PartitionExecutor` splits inputs and creates a schema per partition | Execute targets concurrently, then merge results |
| `ASYNC` | Submit a background call and return immediately | No synchronous result for the current `CACHE TABLE` / `RETURN` |

This concurrency runs within the SQLRec process; it does not submit a distributed job for each node.

### Shared Plans, Isolated Data, and Function Returns

```text
Process-wide resources
├── Compiled function plans
├── Database HmsSchema objects
└── Connector table objects
        ↑ Reuse
        ├── Request A (independent state)
        │   ├── Root schema: input and intermediate tables
        │   ├── Variable map ←→ Shared with SQL subfunctions
        │   └── SQL subfunction: own schema, local tables, return frame
        └── Request B (independent state, same structure)

Cancellation state: parent context → child context
```

| Implementation | State handling |
| --- | --- |
| `CallSqlFunctionBindable` | Check argument/parameter schemas and bind by position in a new schema; local tables remain hidden from the caller |
| `ExecuteContextImpl` | Manage variables, call stacks, Trace, cancellation, and return frames; subfunctions share the variable map but have separate return frames |
| `CacheTableBindable` | Fully materialize results → check cancellation → publish to the current schema; later nodes reuse results by table name |

Table-scope isolation does not copy variables. Query results and intermediate tables primarily reside in memory and are constrained by JVM memory.

### Failures, Timeouts, and Result Publication

| Level | Implementation and handling |
| --- | --- |
| Node | `ProxyAllBindable` handles timeouts, cancellation checks, Trace, duration, row counts, and merge-branch fallback |
| DAG | Unrecovered errors end waiting early and cancel the current function scope; child contexts observe cancellation through the parent chain, and tasks exit cooperatively |
| TIMEIN | `IfBindable` executes THEN in a child context; failure or timeout cancels it and runs ELSE, preventing late cache or return values from overwriting fallback results |
| Partition | Fail the whole call or discard failed partitions and merge the rest, depending on configuration |

Cancellation does not guarantee immediate termination of network requests. Completed external writes have no unified rollback across Connectors. See [Exception Recovery](../guides/exception-recovery.md) and [Observability](../operations/observability.md).

## Metadata, Table Objects, and Storage Execution {#data-sources-and-caches}

### Metadata Management

`MetadataAccess` separates schema definitions, resource records, and model-file access. `MetadataAccessFactory` selects the implementation based on `SQL_SCHEMA_DIR`.

| Mode | Schema definitions | Resources and files |
| --- | --- | --- |
| Remote | `HmsSchemaAccess` → HMS; DDL through HiveCatalog | `DbStoreAccess` → PostgreSQL; file access through Hadoop interfaces |
| Local files | `SqlFileParser` → `SqlFileSchemaAccess` | `InMemoryStoreAccess`; Services may point to existing endpoints |

| Object | Reuse and updates |
| --- | --- |
| Root schema | Created by `CalciteSchemaFactory` for each executor |
| Database `HmsSchema` | Reused within the process, loading tables and functions in both metadata modes |
| Connector Table | Created from table definitions through factories for compilation and execution |
| Local in-memory DDL | Neither writes back to files nor shares changes across instances; restarting restores file definitions |
| Remote persistent definitions | Shared across instances; each instance still maintains its own execution objects and caches |

See [Metadata DDL](./sql.md#metadata-ddl) for definition scope.

### How Connectors Enter Query Plans

```text
Table metadata → SPI / HmsTableFactory → Calcite Table
  → Select operators by table capabilities and query shape

KV primary-key join (INNER / LEFT)
  Materialize left input; deduplicate join keys, ignoring NULL
    → Convert key types → Check table cache → Batch-fetch missing keys
    → Merge by key and join rows; LEFT fills unmatched rows with NULL

Vector lookup join
  Recognize sorting, Top K, and filters
    → Build VectorSearchRequest per left row
    → Search through VectorSearchable / Milvus
    → Combine left row, right row, and score → Continue outer relational plan
```

| Path | Implementation and limits |
| --- | --- |
| Factory | `TableFactoryUtils` discovers `HmsTableFactory` through SPI and creates a Table using the `connector` option |
| KV primary-key join | `SqlrecEnumerableKvJoin` → `KvJoinUtils` → `SqlRecKvTable.getByPrimaryKey()`; support multiple rows per key, filling unmatched LEFT rows with NULL |
| KV non-key join | Build filtered scans for distinct join keys, with different costs from batched primary-key access |
| Vector join | `SqlRecVectorJoinRule` → `SqlrecEnumerableVectorLookupJoin` → `VectorJoinExecutor`; build vector, field, filter, and Top K requests per left row |
| Subsequent computation | Continue outer projections and sorting after combining search results; supported local plans handle predicates that were not pushed down |

These specialized paths depend on table capabilities and query shape. Unsupported queries are rejected; see [Built-in Connectors](./connectors/builtin-connectors.md).

### Caches and Definition Refresh {#connector-row-cache-and-query-paths}

| Cache or data | Location | Updates and invalidation |
| --- | --- | --- |
| `CACHE TABLE` | Execution schema | Published after the statement completes; scoped to the request, session, or subfunction |
| Table schemas and Connector objects | Database schema | Compare metadata; reuse unchanged objects and rebuild changed ones |
| Compiled functions | `SqlFunctionCache` | Reuse on cache hits; recompile after definition refresh or invalidation |
| Primary-key cache | Caffeine within a Table | TTL, size eviction, and affected-key invalidation after SQLRec writes |
| Filesystem data | File snapshot within a Table | Writes affect memory only; rebuilding the object reloads the file |

| Operation | What is retained or rebuilt |
| --- | --- |
| Normal metadata refresh | Retain unchanged Tables, primary-key caches, and Filesystem data |
| `FLUSH` | Invalidate definition caches and Tables; retain session cache tables and local in-memory DDL without rereading SQL files |
| External-system writes | Do not proactively clear primary-key caches; visibility depends on TTL and other policies |

See [Query Caching](./connectors/builtin-connectors.md#query-caching) and [FLUSH](./sql.md#flush) for details.

## Model Tasks and Online Inference {#models-and-online-services}

`ModelManager` and `ServiceManager` call backends through the `ModelController` SPI to generate training, export, and inference workload configurations.

### Task Management and Artifacts

```text
Preparation (training backends)
  Model / ModelController
    → Training and export Jobs (Kubernetes)
    → Checkpoint metadata + model files
    → Deploy inference workload (Deployment / Service)
      → HTTP endpoint for online calls

Online call
  Input CacheTable → CallServiceFunction feature encoding
    → PredictionClient HTTP inference → Append predictions → New CacheTable

An API publishes a recommendation flow; a Service provides model predictions.
```

| Object or stage | Stored or executed content |
| --- | --- |
| Model | Fields, model type, configuration |
| Checkpoint metadata | Model-definition snapshot, name, type, task DDL/YAML, and status; locate artifacts from the snapshot and name |
| Model storage | Actual weights and exported files; training backends export origin to export artifacts |
| Before export / service creation | Check the source checkpoint's completion status and type |
| Hosted-service creation | Validate configuration → submit to Kubernetes → save the definition and URL; submission success does not mean readiness |
| Load a Service in file mode | Build call configuration only; do not deploy to Kubernetes |

### How Inference Results Enter SQL Results

| Step | Implementation |
| --- | --- |
| Resolve inputs and outputs | `CallServiceFunction` reads Service configuration and model fields |
| Request | Select input columns according to the Model, encode row-based or User-Item JSON, and send HTTP through `PredictionClient` |
| Response | Append declared prediction fields by input position to form a new `CacheTable`; missing or short output columns are filled with NULL |

The service provider must still preserve response row count and order. Inference services run and scale independently. Hugging Face can deploy a downloaded origin directly; `external` connects to an existing endpoint. **An API publishes the flow; a Service provides predictions.** See the [Model Guide](../guides/model-lifecycle.md) for backend differences.

## Execution Boundaries {#runtime-boundaries}

### Gateway Forwarding Boundaries

| Area | Implementation and limits |
| --- | --- |
| Session and operation ownership | `SessionManager` manages each session's `SqlExecutor`, distinguishing local and remote operations |
| Remote execution | `GatewayClient` connects on demand, synchronizes the database and session settings, and executes RPCs |
| Forwarding entry points | Thrift/JDBC in remote metadata mode only; REST API, REST SQL, and CLI have no transparent forwarding |
| Routing limits | Forwardable statements such as CTEs go to Gateway; forwarding cannot bypass SQLRec extension restrictions |
| Metadata-only DDL | Local adapters write HMS without creating a Flink execution environment |

See the [SQL Reference](./sql.md#metadata-ddl) and [Client Connections](../guides/clients.md#gateway-errors).

### Memory and State Boundaries

| Entry point or resource | State scope |
| --- | --- |
| HTTP recommendation API | Independent inputs, variables, and root schema for each call |
| REST SQL | One shared executor within a request |
| Thrift/JDBC | Current session retains its executor, default database, and cache tables |
| Local CLI | Current process, with memory independent of the HTTP service |
| Connectors, model services, Flink jobs | Separate storage, compute, and resource lifecycles |

Local queries materialize in memory, without a general spill-to-disk or continuous streaming execution mechanism. Use the appropriate Flink path for continuous large-scale processing; training and hosted-model deployment depend on Kubernetes.
