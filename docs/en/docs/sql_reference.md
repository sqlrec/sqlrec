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
{DESCRIBE | DESC} MODEL model_name [CHECKPOINT = 'checkpoint_name']
```

**Parameters:**

| Parameter | Description |
|-----------|-------------|
| `model_name` | Model name |
| `checkpoint_name` | Optional. Checkpoint name, if specified shows detailed information for that checkpoint |

**Examples:**

```sql
DESCRIBE MODEL my_model;

DESC MODEL my_model CHECKPOINT = 'v1.0';
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
{DESCRIBE | DESC} SERVICE service_name
```

**Parameters:**

| Parameter | Description |
|-----------|-------------|
| `service_name` | Service name |

**Examples:**

```sql
DESCRIBE SERVICE my_service;

DESC SERVICE my_service;
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
```

**Parameters:**

| Parameter | Description |
|-----------|-------------|
| `table_name` | Input table name |
| `column_name` | Column name |
| `column_type` | Column data type |

**Example:**

```sql
DEFINE INPUT TABLE input_data (
    id INT,
    name VARCHAR(100),
    score DOUBLE,
    created_at TIMESTAMP
);
```


### RETURN

Return result from a function.

**Syntax:**

```sql
RETURN [table_name]
```

**Parameters:**

| Parameter | Description |
|-----------|-------------|
| `table_name` | Optional. Table name to return |

**Examples:**

```sql
RETURN;

RETURN result_table;
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
    {CALL function_name([arg1, arg2, ...]) [LIKE {like_table | FUNCTION 'function_name'}] [ASYNC]
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
| `ASYNC` | Optional. Execute asynchronously |
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
CALL my_function('param1') ASYNC;
```


## Function Calls

### CALL

Call a SQL function.

**Syntax:**

```sql
CALL function_name([arg1, arg2, ...]) [LIKE {like_table | FUNCTION 'function_name'}] [ASYNC]
```

**Parameters:**

| Parameter | Description |
|-----------|-------------|
| `function_name` | Function name, can be an identifier or `GET()` expression |
| `arg1, arg2, ...` | Function parameters, can be identifiers, `GET()` expressions, or string literals |
| `like_table` | Optional. Specify template table for result table |
| `FUNCTION 'function_name'` | Optional. Specify that the result table schema matches the output schema of a function |
| `ASYNC` | Optional. Execute asynchronously |

**Examples:**

```sql
CALL my_function('param1', 'param2');

CALL my_function(GET('var1'), 'param2') LIKE template_table;

CALL my_function(GET('var1'), 'param2') LIKE FUNCTION 'template_function';

CALL my_function('param1') ASYNC;

CALL GET('fun1')(GET('id'), t1, '10') LIKE t1;
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
