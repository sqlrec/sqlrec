# SQLRec SQL Syntax Reference

This document introduces the extended SQL syntax supported by SQLRec.

## Model Management

### CREATE MODEL

Create a new machine learning model definition.

**Syntax:**

```sql
CREATE MODEL [IF NOT EXISTS] model_name 
    [(column_name column_type [, ...])]
    [WITH (property_name = property_value [, ...])]
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
CREATE MODEL my_model (
    id INT,
    name VARCHAR(100),
    score DOUBLE
) WITH (
    model_type = 'classification',
    version = '1.0'
);

CREATE MODEL IF NOT EXISTS my_model;
```


### DROP MODEL

Drop an existing model.

**Syntax:**

```sql
DROP MODEL [IF EXISTS] model_name
```

**Parameters:**

| Parameter | Description |
|-----------|-------------|
| `IF EXISTS` | Optional. If the model doesn't exist, no error is raised |
| `model_name` | Name of the model to drop |

**Examples:**

```sql
DROP MODEL my_model;

DROP MODEL IF EXISTS my_model;
```


### TRAIN MODEL

Train a model and create a checkpoint.

**Syntax:**

```sql
TRAIN MODEL model_name CHECKPOINT = 'checkpoint_name'
    ON data_source
    [WHERE condition]
    [FROM 'existing_checkpoint']
    [WITH (property_name = property_value [, ...])]
```

**Parameters:**

| Parameter | Description |
|-----------|-------------|
| `model_name` | Name of the model to train |
| `checkpoint_name` | Checkpoint name to identify training results |
| `data_source` | Training data source table name |
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
    WITH (epochs = 100, learning_rate = 0.01);
```


### EXPORT MODEL

Export model training results.

**Syntax:**

```sql
EXPORT MODEL model_name CHECKPOINT = 'checkpoint_name'
    [ON data_source]
    [WHERE condition]
    [WITH (property_name = property_value [, ...])]
```

**Parameters:**

| Parameter | Description |
|-----------|-------------|
| `model_name` | Name of the model to export |
| `checkpoint_name` | Checkpoint name |
| `data_source` | Optional. Export target data source |
| `condition` | Optional. WHERE condition |
| `property_name` | Optional. Export property name |
| `property_value` | Optional. Export property value |

**Examples:**

```sql
EXPORT MODEL my_model CHECKPOINT = 'v1.0'
    ON export_table;

EXPORT MODEL my_model CHECKPOINT = 'v1.0'
    ON export_table
    WHERE status = 'valid'
    WITH (format = 'parquet');
```


### SHOW MODELS

Show list of all models.

**Syntax:**

```sql
SHOW MODELS
```

**Example:**

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

**Syntax:**

```sql
SHOW CHECKPOINTS model_name
```

**Parameters:**

| Parameter | Description |
|-----------|-------------|
| `model_name` | Model name |

**Example:**

```sql
SHOW CHECKPOINTS my_model;
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


## Service Management

### CREATE SERVICE

Create a model service.

**Syntax:**

```sql
CREATE SERVICE [IF NOT EXISTS] service_name
    ON MODEL model_name
    [CHECKPOINT = 'checkpoint_name']
    [WITH (property_name = property_value [, ...])]
```

**Parameters:**

| Parameter | Description |
|-----------|-------------|
| `IF NOT EXISTS` | Optional. If the service already exists, no new service is created and no error is raised |
| `service_name` | Service name |
| `model_name` | Associated model name |
| `checkpoint_name` | Optional. Checkpoint name to use |
| `property_name` | Optional. Service property name |
| `property_value` | Optional. Service property value |

**Examples:**

```sql
CREATE SERVICE my_service
    ON MODEL my_model
    CHECKPOINT = 'v1.0';

CREATE SERVICE IF NOT EXISTS my_service
    ON MODEL my_model
    CHECKPOINT = 'v1.0'
    WITH (port = 8080, replicas = 3);
```


### DROP SERVICE

Drop an existing service.

**Syntax:**

```sql
DROP SERVICE [IF EXISTS] service_name
```

**Parameters:**

| Parameter | Description |
|-----------|-------------|
| `IF EXISTS` | Optional. If the service doesn't exist, no error is raised |
| `service_name` | Name of the service to drop |

**Examples:**

```sql
DROP SERVICE my_service;

DROP SERVICE IF EXISTS my_service;
```


### SHOW SERVICES

Show list of all services.

**Syntax:**

```sql
SHOW SERVICES
```

**Example:**

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
DESCRIBE SERVICE my_service;

DESC SERVICE my_service;

DESCRIBE FORMATTED SERVICE my_service;
```


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

Drop an existing API.

**Syntax:**

```sql
DROP API [IF EXISTS] api_name
```

**Parameters:**

| Parameter | Description |
|-----------|-------------|
| `IF EXISTS` | Optional. If the API doesn't exist, no error is raised |
| `api_name` | Name of the API to drop |

**Examples:**

```sql
DROP API my_api;

DROP API IF EXISTS my_api;
```


### SHOW APIS

Show list of all APIs.

**Syntax:**

```sql
SHOW APIS
```

**Example:**

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
- Every possible `RETURN` in a function must use one consistent result schema: either all returns are empty, or all return the same column count, names, and types.
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
    RETURN SELECT CAST(NULL AS BIGINT) AS id WHERE FALSE
);
RETURN SELECT id FROM candidates;
```


### DROP SQL FUNCTION

Drop an existing SQL function.

**Syntax:**

```sql
DROP SQL FUNCTION [IF EXISTS] function_name
```

**Parameters:**

| Parameter | Description |
|-----------|-------------|
| `IF EXISTS` | Optional. If the function doesn't exist, no error is raised |
| `function_name` | Name of the function to drop |

**Note:** If the function is referenced by an API, it cannot be dropped.

**Examples:**

```sql
DROP SQL FUNCTION my_function;

DROP SQL FUNCTION IF EXISTS my_function;
```


### SHOW SQL FUNCTIONS

Show list of all SQL functions.

**Syntax:**

```sql
SHOW SQL FUNCTIONS
```

**Example:**

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


## Cache Management

### CACHE TABLE

Cache query results or function call results to a specified table.

**Syntax:**

```sql
CACHE TABLE table_name AS 
    {CALL function_name([arg1, arg2, ...]) [LIKE {like_table | FUNCTION 'function_name'}] [PARTITION BY table_name SIZE partition_size] [ASYNC]
     | select_statement}
```

**Parameters:**

| Parameter | Description |
|-----------|-------------|
| `table_name` | Cache table name |
| `function_name` | Function name to call, can be an identifier or `GET()` expression |
| `arg1, arg2, ...` | Function parameters, can be identifiers, `GET()` expressions, or string literals |
| `like_table` | Optional. Specify template table for result table |
| `FUNCTION 'function_name'` | Optional. Specify that the result table schema matches the output schema of a function |
| `PARTITION BY table_name SIZE partition_size` | Optional. Partition the specified input table for concurrent execution. `table_name` must be one of the function's input tables, `partition_size` is the maximum number of rows per partition |
| `ASYNC` | Optional. Execute asynchronously (only supported in standalone CALL statements, not in CACHE TABLE) |
| `select_statement` | SELECT query statement |

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
The `ASYNC` keyword is parsed in `CACHE TABLE` syntax but throws an exception at runtime (`async function not support in cache`). To execute asynchronously, use a standalone `CALL` statement.
:::


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
2. **Timeout Mode** (TIMEIN): The condition expression must return a numeric timeout value in milliseconds
   - If timeout > 0, executes the THEN clause with the specified timeout; falls back to the ELSE clause if timeout occurs
   - If timeout <= 0, executes the THEN clause immediately
   - When timeout > 0, if THEN times out or throws, its temporary `RETURN` state is discarded before ELSE runs, so a failed branch cannot publish a function result

**Notes:**
- The condition query must return exactly one row and one column. In normal mode the value must be BOOLEAN (NULL is treated as false); in TIMEIN mode it must be numeric.
- THEN and ELSE clauses must be both CACHE statements or both non-CACHE statements; mixing is not allowed
- When both branches are CACHE statements: they must write to the same table name with compatible table schemas
- When both branches are non-CACHE statements: the return field structures of the two branches must be compatible
- An IF containing `RETURN` has only two valid shapes: a returning THEN with no ELSE, or both THEN and ELSE returning. An ELSE-only return and a returning THEN paired with a non-returning ELSE are rejected
- When both branches return, their result schemas must be compatible; every other return point in the function must use the same schema
- When THEN returns and ELSE is omitted, a false condition continues with the statements following the IF
- TIMEIN requires ELSE; both branches must be CACHE statements or both must be RETURN statements
- Directly nesting another IF in THEN or ELSE is currently unsupported
- With no ELSE clause and a false condition: if the THEN clause is a CACHE statement, the corresponding cache table is registered as an empty table (if it does not already exist), so subsequent statements can reference it normally

**Examples:**

```sql
IF (SELECT COUNT(*) > 100 FROM source_table) THEN (
    CACHE TABLE result AS SELECT * FROM source_table
) ELSE (
    CACHE TABLE result AS SELECT * FROM backup_table
);

IF TIMEIN (SELECT timeout_ms FROM config_table) THEN (
    CACHE TABLE result AS CALL slow_function('param')
) ELSE (
    CACHE TABLE result AS SELECT * FROM default_table
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
    RETURN SELECT CAST(NULL AS BIGINT) AS id WHERE FALSE
);
RETURN SELECT id FROM source_table;

-- Both branches return the same schema
IF (SELECT use_primary FROM config_table) THEN (
    RETURN SELECT id, score FROM primary_result
) ELSE (
    RETURN SELECT id, score FROM fallback_result
);
-- A top-level RETURN is still required to terminate the function definition
RETURN SELECT id, score FROM fallback_result;

-- TIMEIN also supports RETURN; timeout or failure selects ELSE
IF TIMEIN (SELECT 1000) THEN (
    RETURN SELECT id, score FROM slow_result
) ELSE (
    RETURN SELECT id, score FROM fallback_result
);
RETURN SELECT id, score FROM fallback_result;
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
- On assertion failure, a `RuntimeException` is thrown containing the specific row and column index of the failure

**Examples:**

```sql
ASSERT SELECT COUNT(*) > 0 FROM source_table;

ASSERT SELECT COUNT(*) > 100 FROM source_table WHERE status = 'active';

ASSERT SELECT COUNT(*) > 0, COUNT(*) >= 10 FROM source_table;
```


### FLUSH

Forcefully invalidate all caches in the system.

**Syntax:**

```sql
FLUSH
```

**Description:**

The `FLUSH` statement immediately invalidates all in-process caches of SQLRec, forcing subsequent queries to reload the latest data from the metastore. The following caches are invalidated:

- `CalciteSchemaFactory`: database list and table schema caches
- `JavaFunctionUtils`: Java function non-existence cache
- `CompileManager`: compiled SQL function bindings (`SqlFunctionBindable`) and API caches
- `ServiceManager`: service configuration cache

**Example:**

```sql
FLUSH;
```

::: warning Note
`FLUSH` invalidates all caches, which may cause short-term overhead from reloading metadata. It is typically executed manually after metadata changes (e.g., external modifications to table schemas, functions, or service definitions) to ensure subsequent queries see the latest state.
:::


## Function Calls

### CALL

Call a SQL function.

**Syntax:**

```sql
CALL function_name([arg1, arg2, ...]) [LIKE {like_table | FUNCTION 'function_name'}] [PARTITION BY table_name SIZE partition_size] [ASYNC]
```

**Parameters:**

| Parameter | Description |
|-----------|-------------|
| `function_name` | Function name, can be an identifier or `GET()` expression |
| `arg1, arg2, ...` | Function parameters, can be identifiers, `GET()` expressions, or string literals |
| `like_table` | Optional. Specify template table for result table |
| `FUNCTION 'function_name'` | Optional. Specify that the result table schema matches the output schema of a function |
| `PARTITION BY table_name SIZE partition_size` | Optional. Partition the specified input table for concurrent execution. `table_name` must be one of the function's input tables, `partition_size` is the maximum number of rows per partition |
| `ASYNC` | Optional. Execute asynchronously |

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


### GET

Get the value of a runtime variable.

**Syntax:**

```sql
GET('variable_name')
```

**Parameters:**

| Parameter | Description |
|-----------|-------------|
| `variable_name` | Variable name, must be a string literal |

**Examples:**

```sql
GET('my_variable');

CALL my_function(GET('input_table'));
```
