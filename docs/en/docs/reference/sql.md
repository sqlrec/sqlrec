# SQLRec SQL Syntax Reference

This page covers SQLRec's extended SQL syntax. For ordinary `SELECT`, `INSERT`, and table creation, start with [Writing a Recommendation Flow](../guides/recommendation-flow.md) and [Connecting Data Sources](../guides/data-sources.md).

Table columns, SQL function input fields, and query result columns do not support the `ROW` type, including `ROW` nested inside `ARRAY` or `MAP`.

Find a statement by task:

| What you want to do | Start here |
| --- | --- |
| Define and manage SQL functions | [CREATE SQL FUNCTION](#create-sql-function), [SHOW SQL FUNCTIONS](#show-sql-functions) |
| Write a recommendation flow | [DEFINE INPUT TABLE](#define-input-table), [CACHE TABLE](#cache-table), [CALL](#call), [IF](#if), [ASSERT](#assert), [RETURN](#return) |
| Set and read execution variables | [SET](#set), [GET](#get) |
| Publish and manage APIs | [CREATE API](#create-api), [SHOW APIS](#show-apis) |
| Define, train, and export models | [CREATE MODEL](#create-model), [TRAIN MODEL](#train-model), [EXPORT MODEL](#export-model), [SHOW CHECKPOINTS](#show-checkpoints) |
| Deploy and manage model services | [CREATE SERVICE](#create-service), [SHOW SERVICES](#show-services) |
| Understand table, database, and UDF DDL support | [Metadata DDL](#metadata-ddl) |
| Inspect databases, table schemas, and UDFs | [Metadata Queries](#metadata-queries) |
| Refresh in-process definition caches | [FLUSH](#flush) |

## SQL Function Management

### CREATE SQL FUNCTION

Create a custom SQL function.

**Syntax:**

```sql
CREATE [OR REPLACE] SQL FUNCTION function_name
```

**Description:**

This statement starts a function definition block, followed by SQL statements for the function body until function compilation is complete.

**Parameters:**

| Parameter | Description |
|-----------|-------------|
| `OR REPLACE` | Optional. If the function already exists, replace the existing definition |
| `function_name` | Function name |

**Examples:**

```sql
CREATE SQL FUNCTION my_function;

CREATE OR REPLACE SQL FUNCTION my_function;
```

### DROP SQL FUNCTION

Drop an existing SQL function. `IF EXISTS` suppresses the error when the object is absent.

```sql
DROP SQL FUNCTION [IF EXISTS] function_name;
```

**Note:** If the function is referenced by an API, it cannot be dropped.

### SHOW SQL FUNCTIONS

Show list of all SQL functions.

```sql
SHOW SQL FUNCTIONS;
```

### DESCRIBE SQL FUNCTION

Show SQL function creation statement.

**Syntax:**

```sql
{DESCRIBE | DESC} SQL FUNCTION function_name
```

**Parameters:**

| Parameter | Description |
|-----------|-------------|
| `function_name` | Function name |

**Examples:**

```sql
DESCRIBE SQL FUNCTION my_function;

DESC SQL FUNCTION my_function;
```

## Recommendation Flow Statements

### DEFINE INPUT TABLE

Define the structure of an input table, used to declare input parameter table structure in SQL functions.

**Syntax:**

```sql
DEFINE INPUT TABLE table_name (
    column_name1 column_type1,
    column_name2 column_type2,
    ...
)

-- Or use LIKE clause to copy an existing table's structure
DEFINE INPUT TABLE table_name LIKE existing_table
```

**Parameters:**

| Parameter | Description |
|-----------|-------------|
| `table_name` | Input table name |
| `column_name` | Column name |
| `column_type` | Column data type |
| `existing_table` | Name of an existing table to copy its structure from |

**Description:**

`DEFINE INPUT TABLE` supports two ways to define input table structure:
1. **Explicit Definition**: Directly specify column names and types
2. **LIKE Clause**: Copy the structure of an existing table, including all column names and types

**Example:**

```sql
-- Explicitly define columns
DEFINE INPUT TABLE input_data (
    id INT,
    name VARCHAR(100),
    score DOUBLE,
    created_at TIMESTAMP
);

-- Use LIKE clause to copy table structure
DEFINE INPUT TABLE input_data LIKE source_table;
```

### CACHE TABLE

Cache query results or function call results to a specified table.

**Syntax:**

```sql
CACHE TABLE table_name AS
    {CALL function_name([arg1, arg2, ...]) [LIKE {like_table | FUNCTION 'function_name'}] [PARTITION BY table_name SIZE partition_size]
     | select_statement}
```

**Parameters:**

| Parameter | Description |
|-----------|-------------|
| `table_name` | Cache table name |
| `function_name` | Function name to call; an identifier or a `GET()` expression |
| `arg1, arg2, ...` | Function arguments; identifiers, `GET()` expressions, or string, numeric and Boolean literals |
| `like_table` | Optional. Specify template table for result table |
| `FUNCTION 'function_name'` | Optional. Specify that the result table schema matches the output schema of a function |
| `PARTITION BY table_name SIZE partition_size` | Optional. Partition the specified input table for concurrent execution. `table_name` must be one of the function's input tables; `partition_size` can be an integer literal or `get()` and is the maximum number of rows per partition |
| `select_statement` | SELECT query statement |

Cache table names are case-insensitive and stored in lowercase. SQL function input table names follow the same rule and must be unique under that rule. Cache writes whose names differ only in case replace the same table. Spaces inside quoted table names remain part of the name.

**Examples:**

```sql
CACHE TABLE cached_result AS
SELECT * FROM source_table WHERE status = 'active';

CACHE TABLE cached_result AS
CALL my_function('param1', 'param2');

CACHE TABLE cached_result AS
CALL my_function(GET('var1'), 'param2') LIKE template_table;

CACHE TABLE cached_result AS
CALL my_function(GET('var1'), 'param2') LIKE FUNCTION 'template_function';

CACHE TABLE cached_result AS
CALL my_function(t1) PARTITION BY t1 SIZE 100;

CACHE TABLE cached_result AS
CALL my_function(t1) LIKE t1 PARTITION BY t1 SIZE 100;
```

::: warning Note
`CACHE TABLE ... AS CALL` supports synchronous calls only. Use a standalone `CALL ... ASYNC` for asynchronous execution.
:::

### CALL

Call an SQL function or a registered Java UDF. Explicit table arguments for table functions must be `CacheTable` objects in the current executor; scalar arguments may be string, numeric or Boolean literals, or values from `GET()`. If the result schema cannot be inferred at compile time, use `LIKE table` or `LIKE FUNCTION`.

Java scalar parameters support `String`, `Integer`, `Long`, `Double`, `Boolean` and their primitive types. Exact type matches take priority; integers may widen to BIGINT or DOUBLE. For numeric or Boolean `GET` arguments, provide a typed default. Arbitrary expressions are not supported in these arguments.

When no return schema is specified with `LIKE`, Java methods returning `CacheTable` are invoked at compile time by default to infer the schema. Set the server environment variable `ENABLE_JAVA_FUNCTION_SCHEMA_INFERENCE=false` before startup to disable this invocation; SQL `SET` and request `params` cannot override it. With inference disabled, calls to Java methods returning `CacheTable` require an explicit schema through `LIKE table` or `LIKE FUNCTION`; otherwise compilation fails without invoking the method. Methods returning `void` do not require a schema.

**Syntax:**

```sql
CALL function_name([arg1, arg2, ...]) [LIKE {like_table | FUNCTION 'function_name'}] [PARTITION BY table_name SIZE partition_size] [ASYNC]
```

**Parameters:**

| Parameter | Description |
|-----------|-------------|
| `function_name` | Function name; an identifier or a `GET()` expression |
| `arg1, arg2, ...` | Function arguments; identifiers, `GET()` expressions, or string, numeric and Boolean literals |
| `like_table` | Optional. Specify template table for result table |
| `FUNCTION 'function_name'` | Optional. Specify that the result table schema matches the output schema of a function |
| `PARTITION BY table_name SIZE partition_size` | Optional. Partition the specified input table for concurrent execution. `table_name` must be one of the function's input tables; `partition_size` can be an integer literal or `get()` and is the maximum number of rows per partition |
| `ASYNC` | Optional. Execute asynchronously |

`ASYNC` only submits background work and returns immediately; it has no synchronously consumable result, so it cannot be used in `CACHE TABLE ... AS CALL` or `RETURN CALL`. `PARTITION BY` splits an input cache table for concurrent calls and normally should be combined with `LIKE` to declare the merged result schema; `ASYNC` must be last.

For a SQL function call with `LIKE`, the actual output must satisfy the declared column count, names, order, complete field types, and nullability constraints. Static calls are checked during compilation; dynamic calls are checked after resolving the target and before executing it. A mismatch raises an error.

**Examples:**

```sql
CALL my_function('param1', 'param2');

CALL my_function(GET('var1'), 'param2') LIKE template_table;

CALL my_function(GET('var1'), 'param2') LIKE FUNCTION 'template_function';

CALL my_function('param1') ASYNC;

CALL GET('fun1')(GET('id'), t1, '10') LIKE t1;

CALL my_function(t1) PARTITION BY t1 SIZE 100;

CALL my_function(t1) LIKE t1 PARTITION BY t1 SIZE 100 ASYNC;
```

### IF

Conditionally execute statements.

**Syntax:**

```sql
IF [TIMEIN] (condition) THEN (statement) [ELSE (statement)]
```

**Parameters:**

| Parameter | Description |
|-----------|-------------|
| `TIMEIN` | Optional. Specifies timeout mode, where the condition returns a timeout value in milliseconds |
| `condition` | Condition expression. Returns a boolean in normal mode, or a numeric value (milliseconds) in timeout mode |
| `statement` | An executable statement: `CACHE TABLE`, `SELECT`/`INSERT`/`UPDATE`/`DELETE`, `ASSERT`, `CALL`, `SET`, or `RETURN` |
| `ELSE` | Optional. Optional in normal mode, required in timeout mode |

**Description:**

The IF statement supports two execution modes:

1. **Normal Mode**: Evaluates the condition expression. If it returns true, executes the THEN clause; otherwise executes the ELSE clause (if present)
2. **Timeout Mode** (TIMEIN): The condition expression must return a numeric THEN execution time budget in milliseconds
   - If timeout > 0, executes the THEN clause with the specified timeout; falls back to the ELSE clause if timeout occurs
   - If the budget <= 0, skips THEN and executes ELSE directly
   - If THEN times out or fails, ELSE provides the result; partial results from the failed branch are not returned

**Notes:**
- The condition query must return exactly one row and one column. In normal mode the value must be BOOLEAN (NULL is treated as false); in TIMEIN mode it must be numeric.
- THEN and ELSE clauses must be both CACHE statements or both non-CACHE statements; mixing is not allowed
- When both branches are CACHE statements: they must write to the same table name with compatible table schemas
- The IF result schema merges nullability and type bounds from every possible branch, including an existing cache that may be retained when ELSE is omitted. Multiple RETURN statements in a function use the same merging rule.
- When both branches are non-CACHE statements: the return field structures of the two branches must be compatible
- An IF containing `RETURN` has only two valid shapes: a returning THEN with no ELSE, or both THEN and ELSE returning. An ELSE-only return and a returning THEN paired with a non-returning ELSE are rejected
- When both branches return, their result schemas must be compatible. The IF must be followed immediately by one empty `RETURN;` that ends the definition, with no intervening statement. The function result schema comes from the two branches and is unaffected by this empty terminator
- When THEN returns and ELSE is omitted, a false condition continues with the statements following the IF
- TIMEIN requires ELSE; both branches must be CACHE statements or both must be RETURN statements
- Late results from a timed-out or failed THEN cannot overwrite ELSE.
- Request cancellation terminates execution without falling back to ELSE. While the request remains active, THEN exceptions, Errors, and interruptions may all trigger ELSE.
- A non-positive TIMEIN budget selects ELSE directly; `NODE_EXEC_TIMEOUT <= 0` still disables the node timeout
- Directly nesting another IF in THEN or ELSE is currently unsupported
- With no ELSE clause and a false condition: if the THEN clause is a CACHE statement, the corresponding cache table is registered as an empty table (if it does not already exist), so subsequent statements can reference it normally

**Examples:**

```sql
IF (SELECT COUNT(*) > 100 FROM source_table) THEN (
    CACHE TABLE result_table AS SELECT * FROM source_table
) ELSE (
    CACHE TABLE result_table AS SELECT * FROM backup_table
);

IF TIMEIN (SELECT timeout_ms FROM config_table) THEN (
    CACHE TABLE result_table AS CALL slow_function('param')
) ELSE (
    CACHE TABLE result_table AS SELECT * FROM default_table
);

-- Branches as arbitrary executable statements
IF (SELECT COUNT(*) > 0 FROM source_table) THEN (
    SELECT * FROM source_table
) ELSE (
    SELECT * FROM backup_table
);

IF (SELECT COUNT(*) > 0 FROM source_table) THEN (
    ASSERT SELECT COUNT(*) > 0 FROM source_table
);

-- Early return in a SQL function. With no ELSE, false continues execution.
IF (SELECT COUNT(*) = 0 FROM source_table) THEN (
    RETURN SELECT CAST(NULL AS BIGINT) AS id LIMIT 0
);
RETURN SELECT id FROM source_table;

-- Both branches return the same schema
IF (SELECT use_primary FROM config_table) THEN (
    RETURN SELECT id, score FROM primary_result
) ELSE (
    RETURN SELECT id, score FROM fallback_result
);
-- Both branches cover every path; the following empty RETURN only ends the definition
RETURN;

-- TIMEIN also supports RETURN; timeout or failure selects ELSE
IF TIMEIN (SELECT 1000) THEN (
    RETURN SELECT id, score FROM slow_result
) ELSE (
    RETURN SELECT id, score FROM fallback_result
);
RETURN;
```

### ASSERT

Executes a SELECT query and asserts that the result is true. If any field of the query result is not `true`, an exception is thrown and execution is aborted.

**Syntax:**

```sql
ASSERT select_statement
```

**Parameters:**

| Parameter | Description |
|-----------|-------------|
| `select_statement` | SELECT query statement. All returned fields must be of boolean type |

**Description:**

The ASSERT statement executes the specified SELECT query and validates the result:
1. All returned fields must be of boolean (BOOLEAN) type, otherwise an exception is thrown at compile time
2. The query must return at least one row, otherwise an exception is thrown
3. Every value in every row and column must be `true`; if any `false` or `null` value exists, an exception is thrown

**Notes:**
- The ASSERT operator does not support parallel execution
- On assertion failure, a `RuntimeException` is thrown containing the row index, column name and column index of the failure

**Examples:**

```sql
ASSERT SELECT COUNT(*) > 0 FROM source_table;

ASSERT SELECT COUNT(*) > 100 FROM source_table WHERE status = 'active';

ASSERT SELECT COUNT(*) > 0, COUNT(*) >= 10 FROM source_table;
```

Use aliases to name assertions. Failure messages include the column alias and its index:

```sql
ASSERT SELECT COUNT(*) > 0 AS has_candidates FROM items;
```

### RETURN

Return a result from a SQL function and finish the current invocation early. `RETURN` can return a cache table directly, or execute a `SELECT` or synchronous `CALL` and return its result.

**Syntax:**

```sql
RETURN
RETURN table_name
RETURN select_statement
RETURN CALL function_name([arg1, arg2, ...]) [LIKE {like_table | FUNCTION 'function_name'}] [PARTITION BY table_name SIZE partition_size]
```

**Parameters:**

| Parameter | Description |
|-----------|-------------|
| `table_name` | Name of a cache table (`CacheTable`) to return; ordinary external tables are not accepted |
| `select_statement` | SELECT query whose result becomes the function result |
| `CALL ...` | Invoke a SQL/Java function and return its result; dynamic calls still require `LIKE` to declare the result schema |

**Rules:**

- `RETURN;` completes the function normally without returning data.
- A top-level `RETURN` terminates the SQL function definition. Even when an `IF` in the body can return early, the definition must still end with a top-level `RETURN`. No function-body statement may follow it.
- A `RETURN` inside an `IF` only exits the invocation at runtime; it does not terminate the function definition at compile time.
- If only the THEN branch returns, a false condition continues execution. The final top-level `RETURN` is an actual return point for that path and must be compatible with the THEN result schema.
- If both THEN and ELSE are `RETURN` statements, the IF covers every runtime path. It must be followed immediately by exactly one bare `RETURN;` that terminates the function definition. No statement may appear between them, and the terminator cannot be `RETURN table_name`, `RETURN SELECT ...`, or `RETURN CALL ...`. This empty terminator does not participate in result-schema inference.
- Every return point that can execute must use one consistent result schema: either all are empty, or all return the same column count, names, and types. The final empty terminator in the exhaustive-IF case above is compile-time syntax, not a runtime return point.
- `RETURN CALL ... ASYNC` is not supported because an asynchronous invocation cannot provide the synchronous result of the current function.

**Examples:**

```sql
-- Empty return
RETURN;

-- Return an existing cache table
RETURN result_table;

-- Return a query directly without creating an anonymous cache table
RETURN SELECT id, score FROM candidates ORDER BY score DESC;

-- Return a function invocation
RETURN CALL rerank(candidates);

-- Return early from IF. The final top-level RETURN still terminates the
-- function definition and is the fallback when the condition is false.
IF (SELECT COUNT(*) = 0 FROM candidates) THEN (
    RETURN SELECT CAST(NULL AS BIGINT) AS id LIMIT 0
);
RETURN SELECT id FROM candidates;
```

## Execution Variables

### SET

Set a string variable in the current execution context. A SQL function can read it with `` `get` ``.

In a Thrift session, top-level `SET 'key' = 'value'` also saves session settings without connecting to Gateway. Settings synchronize before the next remote SQL statement; successful local saving does not mean remote validation succeeded. Non-assignment `SET` (such as `SET;`) reads Flink configuration and still requires Gateway. `RESET` or `RESET 'key'` clears all or the specified saved local session settings only after Gateway confirms success; disabled forwarding or remote execution failure leaves the settings unchanged. REST and CLI do not provide transparent Gateway forwarding. See [Execution Boundaries](./architecture.md#gateway-forwarding-boundaries).

**Syntax:**

```sql
SET 'variable_name' = 'value'
```

**Examples:**

```sql
SET 'rank_fun' = 'rank_fun_simple';
SET 'limit_count' = '10';

SELECT `get`('rank_fun');
SELECT CAST(`get`('limit_count', '50') AS INT);
```

Keys and values in the API request's `params` object are also execution variables. Values are strings; use a typed default in the scalar function `get`, or cast them explicitly, when using them as numbers or Booleans.

Reading an absent execution variable falls back to the same-named environment variable by default. Explicit empty strings do not trigger fallback. Set the server environment variable `ENABLE_VARIABLE_ENV_FALLBACK=false` to disable this behavior. Environment values are never written into the execution variable map.

### GET

Read an execution variable as a dynamic function name in `CALL`, a scalar argument to a Java table function, or a `PARTITION BY ... SIZE` value. `GET` is available in these positions; it is not a standalone SQL statement. When calling an SQL function, pass cache-table names directly as its table arguments.

**Syntax:**

```sql
GET('variable_name')
GET('variable_name', 'default_value')
```

**Parameters:**

| Parameter | Description |
|-----------|-------------|
| `variable_name` | Variable name, must be a string literal |
| `default_value` | Optional fallback; a string, numeric or Boolean literal. Its type determines the Java argument type; omitted defaults imply VARCHAR. |

**Examples:**

```sql
SET 'rank_fun' = 'default_rank';

CALL GET('rank_fun')(input_table) LIKE FUNCTION 'default_rank';

CALL GET('rank_fun', 'default_rank')(input_table)
LIKE FUNCTION 'default_rank';
```

In ordinary SQL expressions such as `SELECT`, use the scalar function `` `get`('variable_name') `` or `` `get`('variable_name', 'default_value') ``. See [Scalar Functions](./udf/scalar-functions.md#get) for the syntax and supported positions.

## API Management

### CREATE API

Create an API interface, associated with a specified SQL function.

**Syntax:**

```sql
CREATE [OR REPLACE] API api_name WITH function_name
```

**Parameters:**

| Parameter | Description |
|-----------|-------------|
| `OR REPLACE` | Optional. If the API already exists, replace the existing definition |
| `api_name` | API name |
| `function_name` | Associated SQL function name |

**Examples:**

```sql
CREATE API my_api WITH my_function;

CREATE OR REPLACE API my_api WITH my_function;
```

### DROP API

Drop an existing API. `IF EXISTS` suppresses the error when the object is absent.

```sql
DROP API [IF EXISTS] api_name;
```

### SHOW APIS

Show list of all APIs.

```sql
SHOW APIS;
```

### DESCRIBE API

Show API creation statement.

**Syntax:**

```sql
{DESCRIBE | DESC} API api_name
```

**Parameters:**

| Parameter | Description |
|-----------|-------------|
| `api_name` | API name |

**Examples:**

```sql
DESCRIBE API my_api;

DESC API my_api;
```

## Models and Checkpoints

### CREATE MODEL

Create a new machine learning model definition.

**Syntax:**

```sql
CREATE MODEL [IF NOT EXISTS] model_name
    [(column_name column_type [, ...])]
    [WITH ('property_name' = 'property_value' [, ...])]
```

**Parameters:**

| Parameter | Description |
|-----------|-------------|
| `IF NOT EXISTS` | Optional. If the model already exists, no new model is created and no error is raised |
| `model_name` | Model name, must be a valid identifier |
| `column_name` | Column name |
| `column_type` | Column data type |
| `property_name` | Property name |
| `property_value` | Property value |

**Examples:**

```sql
CREATE MODEL rank_model (
    user_id BIGINT,
    item_id BIGINT,
    price DOUBLE,
    is_click INT
) WITH (
    'model' = 'tzrec.wide_and_deep',
    'label_columns' = 'is_click'
);

CREATE MODEL IF NOT EXISTS external_rank_model WITH (
    'model' = 'external',
    'output_columns' = 'score:FLOAT'
);
```

### DROP MODEL

Drop an existing model. `IF EXISTS` suppresses the error when the object is absent.

```sql
DROP MODEL [IF EXISTS] model_name;
```

### TRAIN MODEL

Train or download a model and create a checkpoint.

Training backends require an `ON` table. For Hugging Face, `TRAIN MODEL` downloads a model without `ON`. See [the model guide](../guides/model-lifecycle.md) for backend differences.

Retrying the same checkpoint is idempotent: running or succeeded tasks return the existing checkpoint. Failed tasks stop and remove the old Kubernetes resources, then resubmit while preserving the model directory and checkpoint record. TZRec resumes from saved progress in that directory; progress recovery for other backends depends on their training entry point. Keep training data and configuration unchanged when retrying. To retrain, change parameters, or add training epochs, use a new checkpoint name and optionally select an existing model with `FROM`.

**Syntax:**

```sql
TRAIN MODEL model_name CHECKPOINT = 'checkpoint_name'
    [ON data_source]
    [WHERE condition]
    [FROM 'existing_checkpoint']
    [WITH ('property_name' = 'property_value' [, ...])]
```

**Parameters:**

| Parameter | Description |
|-----------|-------------|
| `model_name` | Name of the model to train |
| `checkpoint_name` | Checkpoint name to identify training results |
| `data_source` | Training table; required by training backends, omitted for Hugging Face downloads |
| `condition` | Optional. WHERE condition to filter training data |
| `existing_checkpoint` | Optional. Continue training from existing checkpoint |
| `property_name` | Optional. Training property name |
| `property_value` | Optional. Training property value |

**Examples:**

```sql
TRAIN MODEL my_model CHECKPOINT = 'v1.0'
    ON training_data
    WHERE status = 'active';

TRAIN MODEL my_model CHECKPOINT = 'v2.0'
    ON training_data
    FROM 'v1.0'
    WITH ('num_epochs' = '10', 'sparse_lr' = '0.01');
```

### EXPORT MODEL

Export model training results.

`ON` names a data table read during export, rather than the artifact destination. Artifacts are saved in the configured model storage; their format is determined by the backend.

**Syntax:**

```sql
EXPORT MODEL model_name CHECKPOINT = 'checkpoint_name'
    [ON data_source]
    [WHERE condition]
    [WITH ('property_name' = 'property_value' [, ...])]
```

**Parameters:**

| Parameter | Description |
|-----------|-------------|
| `model_name` | Name of the model to export |
| `checkpoint_name` | Checkpoint name |
| `data_source` | Optional. Data table read during export; requirements depend on the backend |
| `condition` | Optional. WHERE condition |
| `property_name` | Optional. Export property name |
| `property_value` | Optional. Export property value |

**Examples:**

```sql
EXPORT MODEL my_model CHECKPOINT = 'v1.0'
    ON export_table;

EXPORT MODEL my_model CHECKPOINT = 'v1.0'
    ON export_table
    WHERE status = 'valid';
```

### SHOW MODELS

Show list of all models.

```sql
SHOW MODELS;
```

### DESCRIBE MODEL

Show model creation statement or checkpoint information.

**Syntax:**

```sql
{DESCRIBE | DESC} [FORMATTED] MODEL model_name [CHECKPOINT = 'checkpoint_name']
```

**Parameters:**

| Parameter | Description |
|-----------|-------------|
| `FORMATTED` | Optional. Display detailed information in a formatted table, including model information, input fields, output fields, and model parameters |
| `model_name` | Model name |
| `checkpoint_name` | Optional. Checkpoint name, if specified shows detailed information for that checkpoint |

**Examples:**

```sql
DESCRIBE MODEL my_model;

DESC MODEL my_model CHECKPOINT = 'v1.0';

DESCRIBE FORMATTED MODEL my_model;

DESCRIBE FORMATTED MODEL my_model CHECKPOINT = 'v1.0';
```

### SHOW CHECKPOINTS

Show list of all checkpoints for a specified model.

```sql
SHOW CHECKPOINTS model_name;
```

### ALTER MODEL DROP CHECKPOINT

Drop a specified checkpoint of a model.

**Syntax:**

```sql
ALTER MODEL model_name DROP [IF EXISTS] CHECKPOINT = 'checkpoint_name'
```

**Parameters:**

| Parameter | Description |
|-----------|-------------|
| `model_name` | Model name |
| `IF EXISTS` | Optional. If the checkpoint doesn't exist, no error is raised |
| `checkpoint_name` | Name of the checkpoint to drop |

**Examples:**

```sql
ALTER MODEL my_model DROP CHECKPOINT = 'v1.0';

ALTER MODEL my_model DROP IF EXISTS CHECKPOINT = 'v1.0';
```

## Model Service Management

### CREATE SERVICE

Create a model service.

**Syntax:**

```sql
CREATE SERVICE [IF NOT EXISTS] service_name
    ON MODEL model_name
    [CHECKPOINT = 'checkpoint_name']
    [WITH ('property_name' = 'property_value' [, ...])]
```

**Parameters:**

| Parameter | Description |
|-----------|-------------|
| `IF NOT EXISTS` | Optional. The name must pass validation first; if the service already exists, no new service is created and no error is raised |
| `service_name` | Follows Kubernetes Service DNS (RFC 1035) naming rules: only lowercase letters, digits and hyphens `-`; 1–63 characters, starting with a lowercase letter and ending with a lowercase letter or digit |
| `model_name` | Associated model name |
| `checkpoint_name` | Optional. Checkpoint name to use |
| `property_name` | Optional. Service property name |
| `property_value` | Optional. Service property value |

**Examples:**

```sql
CREATE SERVICE `rank-service`
    ON MODEL rank_model
    CHECKPOINT = 'v1.0_export';

CREATE SERVICE IF NOT EXISTS `external-rank-service`
    ON MODEL external_rank_model
    WITH ('url' = 'http://rank-service:8080/predict');
```

Names follow [Kubernetes Service DNS naming rules](https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#rfc-1035-label-names). Uppercase letters, underscores, dots and spaces are rejected without case conversion. Name validation also applies to `IF NOT EXISTS`. Quote names containing hyphens with backticks. Hosted services use the service name directly for Kubernetes Services and Deployments, without a hash suffix. Their URL is `http://<service-name>.<namespace>.svc.cluster.local:80/predict`. Invalid names are rejected without substitution or truncation. The same name validation applies to external services.

Service metadata is saved after Kubernetes submission succeeds. Submission failure does not overwrite an existing definition. Successful submission does not mean the workload is ready; check its status before calling it. To update or retry an existing definition, omit `IF NOT EXISTS`, which skips existing records.

### DROP SERVICE

Drop an existing service. `IF EXISTS` suppresses the error when the object is absent.

```sql
DROP SERVICE [IF EXISTS] service_name;
```

### SHOW SERVICES

Show list of all services.

```sql
SHOW SERVICES;
```

### DESCRIBE SERVICE

Show service creation statement.

**Syntax:**

```sql
{DESCRIBE | DESC} [FORMATTED] SERVICE service_name
```

**Parameters:**

| Parameter | Description |
|-----------|-------------|
| `FORMATTED` | Optional. Display detailed information in a formatted table, including service information, associated model information, and model fields |
| `service_name` | Service name |

**Examples:**

```sql
DESCRIBE SERVICE `my-service`;

DESC SERVICE `my-service`;

DESCRIBE FORMATTED SERVICE `my-service`;
```

## Metadata DDL

| Mode | Where definitions live | Runtime DDL |
| --- | --- | --- |
| Remote metadata | HMS for databases/tables/Java UDFs; PostgreSQL for SQLRec resources | Persistent DDL through Beeline/JDBC |
| SQL files | Files under `SQL_SCHEMA_DIR`, loaded at startup | Disabled by default; `SQL_SCHEMA_DDL_ENABLED=true` enables supported in-memory DDL |

SQL-file DDL changes are shared within one process, are not written back to files, and disappear on restart. Separate instances do not share changes. The Demo image enables this option. Configure it before startup; SQL `SET` cannot change it.

Supported SQL-file DDL includes database CREATE/ALTER SET/DROP, table CREATE/CREATE LIKE/ALTER/DROP, persistent Java UDFs, SQL functions, and APIs. Replacement is available where the syntax supports it. Model, Service, Checkpoint, training, and export commands remain disabled at runtime.

Database/table/UDF definitions use the Flink 1.19 default dialect. Tables support ordinary and metadata columns, primary keys, and partition declarations. Computed columns, WATERMARK, temporary tables, CTAS/RTAS, views, partition-instance changes, and cross-database renaming are not supported in file mode. Database DROP supports RESTRICT/CASCADE; current and default databases cannot be dropped. After a database DDL change, other existing sessions may need to reconnect before querying the new database.

Remote-mode persistent metadata DDL does not require Gateway. Temporary functions, views, and persistent CTAS/RTAS use remote Thrift forwarding. `USE CATALOG` and temporary tables are unsupported. `USE database` does not validate existence immediately; later operations use actual metadata. See [execution boundaries](./architecture.md#gateway-forwarding-boundaries).

Saving a table or UDF definition does not guarantee local execution support. Deploy required Connector/UDF JARs separately; SQLRec does not download UDF resources automatically.

## Metadata Queries

Both modes support `SHOW DATABASES`, `SHOW TABLES`, `SHOW CREATE TABLE`, `DESCRIBE`, `SHOW FUNCTIONS`, and `SHOW USER FUNCTIONS` without Gateway. SQL-file queries do not need HMS or the Connector/UDF classes just to inspect definitions.

- Table names accept `table`, `database.table`, or `hive.database.table`. Unqualified names prefer a session `CACHE TABLE`.
- `SHOW TABLES` includes cached tables for the current database; `LIKE` and `NOT LIKE` filter names.
- `SHOW USER FUNCTIONS` lists declared Java UDFs. `SHOW FUNCTIONS` also lists Flink built-ins and supports LIKE/ILIKE and their NOT forms. A listed function may not execute locally; SQL functions use `SHOW SQL FUNCTIONS`.
- `SHOW CREATE TABLE` returns normalized DDL, including primary-key NOT NULL constraints. Column comments may not survive round-trip conversion.
- Persistent table `DESCRIBE` returns `name`, `type`, `null`, `key`, `extras`, `watermark`, and `comment` when present. Cached tables return `name` and `type`.

SQL files load in path order; the source of `CREATE TABLE ... LIKE` must be defined first. Editing files requires a restart; `FLUSH` does not re-read them.

## Metadata Refresh

DDL preserves unchanged tables and their in-memory data. ALTER or RENAME may reset the affected table's runtime data; DROP followed by recreation creates a new table.

### FLUSH

```sql
FLUSH;
```

Invalidate this process's definition caches, including table, Java/SQL function, API, and service configuration caches. Subsequent access rebuilds Connector table objects; Filesystem runtime writes are reset. Session `CACHE TABLE` results and SQL-file in-memory DDL changes remain.

Use this after external metadata changes. It does not re-read SQL files or add databases to existing sessions; reconnect if a newly created database is not visible to local queries. See [cache scope](./architecture.md#connector-row-cache-and-query-paths).
