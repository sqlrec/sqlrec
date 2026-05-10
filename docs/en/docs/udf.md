# Built-in UDF

This document introduces SQLRec's built-in user-defined functions (UDF), including table functions and scalar functions.

## Overview

SQLRec provides rich built-in UDFs for common operations in recommendation system development, such as deduplication, diversification, vector computation, etc.

**UDF Categories**:

| Type | Description | Return Value |
|------|-------------|--------------|
| Table Function | Table functions that take tables as input and return tables | `CacheTable` |
| Scalar Function | Scalar functions that take scalar values and return scalar values | Single value |

## Table Functions

### dedup

Deduplication function that excludes records from the input table that already exist in the deduplication table based on specified columns.

**Function Signature**:

```java
public CacheTable evaluate(CacheTable input, CacheTable dedupTable, String col1, String col2)
```

**Parameter Description**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `input` | CacheTable | Input table |
| `dedupTable` | CacheTable | Deduplication table, containing values to exclude |
| `col1` | String | Column name in input table for deduplication |
| `col2` | String | Column name in deduplication table for matching |

**Return Value**: Returns deduplicated `CacheTable` with same structure as input table.

**Usage Example**:

```sql
-- Get items already exposed to user
CACHE TABLE exposured_item AS
SELECT item_id
FROM user_info JOIN user_exposure_item ON user_id = user_info.id;

-- Exclude exposed items from recall results
CACHE TABLE dedup_recall AS
CALL dedup(recall_item, exposured_item, 'item_id', 'item_id');
```

**Working Principle**:
1. Collect all values from `col2` column of `dedupTable`
2. Traverse `input` table, exclude records where `col1` column value exists in deduplication set
3. Return deduplicated result table

---

### shuffle

Random shuffle function that randomly sorts records in the input table.

**Function Signature**:

```java
public CacheTable evaluate(CacheTable input)
```

**Parameter Description**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `input` | CacheTable | Input table |

**Return Value**: Returns randomly sorted `CacheTable` with same structure and data as input table.

**Usage Example**:

```sql
-- Randomly shuffle recommendation results
CACHE TABLE shuffled_result AS
CALL shuffle(recall_item);

-- Take first N from shuffled results
CACHE TABLE random_top_n AS
SELECT * FROM shuffled_result LIMIT 10;
```

---

### window_diversify

Window diversification function that ensures adjacent records don't concentrate too much on a single category, achieving diversity in recommendation results.

**Function Signature**:

```java
public CacheTable evaluate(
    CacheTable input,
    String categoryColumnName,
    String windowSize,
    String maxCategoryNumInWindow,
    String maxReturnRecord
)
```

**Parameter Description**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `input` | CacheTable | Input table |
| `categoryColumnName` | String | Category column name, basis for diversification |
| `windowSize` | String | Sliding window size |
| `maxCategoryNumInWindow` | String | Maximum occurrences per category in window |
| `maxReturnRecord` | String | Maximum return record count |

**Return Value**: Returns diversified `CacheTable` with same structure as input table.

**Usage Example**:

```sql
-- Category diversification: window size 3, each category appears at most once in window, return 10 records
CACHE TABLE diversify_result AS
CALL window_diversify(rec_item, 'category1', '3', '1', '10');
```

**Working Principle**:
1. Maintain a sliding window, counting occurrences of each category in the window
2. Traverse input records, prioritize categories not exceeding limits in window
3. When window slides, remove category count of oldest record
4. Ensure recommendation result diversity, avoid same category items appearing consecutively

---

### add_col

Add column function that adds a new column to the input table with the same value for all rows.

**Function Signature**:

```java
public CacheTable evaluate(CacheTable input, String colName, String value)
```

**Parameter Description**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `input` | CacheTable | Input table |
| `colName` | String | New column name |
| `value` | String | New column value (same for all rows) |

**Return Value**: Returns `CacheTable` with new column added.

**Usage Example**:

```sql
-- Add a source identifier column
CACHE TABLE result_with_source AS
CALL add_col(recall_item, 'source', 'daily_rec');

-- Add timestamp column
CACHE TABLE result_with_time AS
CALL add_col(recall_item, 'rec_time', '2024-01-01');
```

**Notes**:
- New column name cannot duplicate existing column names
- New column type is `VARCHAR`

---

### call_service

Model service call function used to call deployed model services for inference. See [Models documentation](./model/basic_concepts.md#call_service) for details.

---

### call_service_with_qv

Model service call function with Query-Value mode. See [Models documentation](./model/basic_concepts.md#call_service_with_qv) for details.

---

### truncate_table

Table truncation function that extracts rows within a specified range from the input table.

**Function Signature**:

```java
public CacheTable evaluate(CacheTable input, String start, String end)
```

**Parameter Description**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `input` | CacheTable | Input table |
| `start` | String | Starting row index (0-based, inclusive) |
| `end` | String | Ending row index (exclusive) |

**Return Value**: Returns truncated `CacheTable` with same structure as input table.

**Usage Example**:

```sql
-- Get records from row 10 to 20
CACHE TABLE partial_result AS
CALL truncate_table(recall_item, '10', '20');

-- Get first 100 records
CACHE TABLE top_100 AS
CALL truncate_table(recall_item, '0', '100');
```

**Notes**:
- `start` and `end` must be valid integer strings
- `start` and `end` must be non-negative
- `start` must be less than or equal to `end`
- Truncation range is left-closed, right-open interval `[start, end)`

---

### get_variables

Get variables function that retrieves all variables from the execution context and returns a table containing key-value pairs.

**Function Signature**:

```java
public CacheTable evaluate(ExecuteContext context)
```

**Parameter Description**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `context` | ExecuteContext | Execution context |

**Return Value**: Returns a 2-column `CacheTable` with column names `key` and `value`, both of type `VARCHAR`.

**Usage Example**:

```sql
-- Set some variables
SET 'user_id' = '12345';
SET 'limit' = '100';

-- Get all variables
CACHE TABLE all_vars AS
CALL get_variables();

-- View variables
SELECT * FROM all_vars;
```

**Working Principle**:
1. Get all variables from execution context
2. Convert each variable's key-value pair to a row
3. Return table containing all variables

---

### set_variables

Set variables function that reads key-value pairs from a table and sets them in the execution context.

**Function Signature**:

```java
public CacheTable evaluate(ExecuteContext context, CacheTable input)
```

**Parameter Description**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `context` | ExecuteContext | Execution context |
| `input` | CacheTable | Input table, must have exactly 2 columns, both of string type |

**Return Value**: Returns the input table itself.

**Usage Example**:

```sql
-- Create variable table
CACHE TABLE var_table AS
SELECT 'user_id' AS key, '12345' AS value
UNION ALL
SELECT 'limit', '100';

-- Set variables
CALL set_variables(var_table);

-- Use the set variables
SELECT `get`('user_id') AS user_id;
```

**Notes**:
- Input table must have exactly 2 columns
- Both columns must be string type (VARCHAR or CHAR)
- First column is variable name, second column is variable value
- If variable value is NULL, the variable will be deleted

---

### feature_coverage_metrics

Feature coverage metrics function that calculates feature coverage for each field in tables and reports metrics.

**Function Signature**:

```java
public Void evaluate(ExecuteContext context, String metricsName, CacheTable... tables)
```

**Parameter Description**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `context` | ExecuteContext | Execution context |
| `metricsName` | String | Metrics name |
| `tables` | CacheTable... | One or more input tables |

**Return Value**: No return value.

**Usage Example**:

```sql
-- Calculate and report feature coverage
CALL feature_coverage_metrics('feature.coverage', user_features, item_features);

-- Calculate coverage for a single table
CALL feature_coverage_metrics('user.feature.coverage', user_info);
```

**Working Principle**:
1. Traverse each field of each table
2. Count non-null values for each field (`null`, empty `Collection`, empty `Map` are considered missing)
3. Calculate coverage = non-null count / total row count
4. Report metrics using summary type, tags include `table` (table name) and `field` (field name)

**Notes**:
- If table is empty, it will be skipped
- Metrics name cannot be empty

## Scalar Functions

### array_contains

Array contains function that checks if an array contains a specified element.

**Function Signature**:

```java
public static Boolean evaluate(List<?> list, Object element)
```

**Parameter Description**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `list` | List<?> | Input array |
| `element` | Object | Element to check |

**Return Value**: Returns `true` if the array contains the element, `false` otherwise; returns `null` if either parameter is `null`.

**Usage Example**:

```sql
-- Check if user tags contain 'vip'
SELECT
    user_id,
    array_contains(tags, 'vip') AS is_vip
FROM user_info;

-- Filter users with specific tag
SELECT *
FROM user_info
WHERE array_contains(tags, 'active') = true;
```

---

### array_contains_all

Array contains all function that checks if an array contains all specified elements.

**Function Signature**:

```java
public static Boolean evaluate(List<?> list, List<?> elements)
```

**Parameter Description**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `list` | List<?> | Input array |
| `elements` | List<?> | List of elements to check |

**Return Value**: Returns `true` if the array contains all specified elements, `false` otherwise; returns `null` if either parameter is `null`.

**Usage Example**:

```sql
-- Check if user has multiple tags
SELECT
    user_id,
    array_contains_all(tags, ARRAY['vip', 'active']) AS is_vip_active
FROM user_info;

-- Filter users with all specified tags
SELECT *
FROM user_info
WHERE array_contains_all(tags, ARRAY['premium', 'verified']) = true;
```

---

### array_contains_any

Array contains any function that checks if an array contains any of the specified elements.

**Function Signature**:

```java
public static Boolean evaluate(List<?> list, List<?> elements)
```

**Parameter Description**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `list` | List<?> | Input array |
| `elements` | List<?> | List of elements to check |

**Return Value**: Returns `true` if the array contains any of the specified elements, `false` otherwise; returns `null` if either parameter is `null`.

**Usage Example**:

```sql
-- Check if user has any VIP level
SELECT
    user_id,
    array_contains_any(levels, ARRAY['gold', 'platinum', 'diamond']) AS is_high_level
FROM user_info;

-- Filter users with any of the specified tags
SELECT *
FROM user_info
WHERE array_contains_any(tags, ARRAY['new_user', 'trial']) = true;
```

---

### random_vec

Random vector generation function that generates a normalized random vector of specified dimension.

**Function Signature**:

```java
public List<Double> evaluate(String dimensionStr)
```

**Parameter Description**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `dimensionStr` | String | Vector dimension, must be a positive integer string |

**Return Value**: Returns a normalized random vector (`List<Double>`), with L2 norm equal to 1.

**Usage Example**:

```sql
-- Generate a 64-dimensional random vector
SELECT
    user_id,
    random_vec('64') AS random_embedding
FROM user_info;

-- Generate random vectors for cold-start users
CACHE TABLE cold_start_users AS
SELECT
    user_id,
    random_vec('128') AS user_embedding
FROM new_users;
```

**Working Principle**:
1. Parse dimension parameter as integer
2. Generate random vector of specified dimension
3. Perform L2 normalization on the vector, making norm equal to 1

**Notes**:
- Dimension must be a positive integer
- Generated vector is already normalized and can be used directly for similarity calculation

---

### uuid

UUID generation function that generates a random UUID string.

**Function Signature**:

```java
public String evaluate()
```

**Return Value**: Returns a random UUID string, format like `ee073e63-b74a-4c7e-8fea-60459729099c`.

**Usage Example**:

```sql
-- Generate request ID
CACHE TABLE request_meta AS
SELECT
    user_id,
    CAST(CURRENT_TIMESTAMP AS BIGINT) AS req_time,
    uuid() AS req_id
FROM user_info;
```

---

### l2_norm

L2 normalization function that performs L2 normalization on vectors.

**Function Signature**:

```java
public Object evaluate(Object vector)
```

**Parameter Description**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `vector` | Object | Input vector, must be a list of numbers |

**Return Value**: Returns normalized vector (`List<Double>`), making the vector's L2 norm equal to 1.

**Usage Example**:

```sql
-- Normalize user vectors
CACHE TABLE normalized_user AS
SELECT
    user_id,
    l2_norm(user_embedding) AS normalized_embedding
FROM user_features;
```

**Working Principle**:
1. Calculate vector's L2 norm: `norm = sqrt(sum(x_i^2))`
2. Divide each element by the norm: `x_i' = x_i / norm`
3. Normalized vectors are commonly used for cosine similarity calculation

---

### ip

Inner product calculation function that calculates the inner product (dot product) of two vectors.

**Function Signature**:

```java
public Object evaluate(Object emb1, Object emb2)
```

**Parameter Description**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `emb1` | Object | First vector, must be a list of numbers |
| `emb2` | Object | Second vector, must be a list of numbers |

**Return Value**: Returns inner product of two vectors (`Float`).

**Usage Example**:

```sql
-- Calculate inner product of user vector and item vector
SELECT
    user_id,
    item_id,
    ip(user_embedding, item_embedding) AS similarity
FROM user_item_pairs;

-- Vector recall: sort by inner product
CACHE TABLE vector_recall AS
SELECT item_embedding.id AS item_id
FROM user_embedding JOIN item_embedding ON 1=1
ORDER BY ip(user_embedding.embedding, item_embedding.embedding) DESC
LIMIT 300;
```

**Working Principle**:
- Inner product calculation: `ip = sum(emb1[i] * emb2[i])`
- If vectors are already normalized, inner product equals cosine similarity
- Commonly used for vector retrieval and similarity calculation

---

### get

Variable retrieval function that gets the value of a variable from the execution context. Commonly used to reference variables set via the `SET` statement in SQL.

**Function Signature**:

```java
public static String evaluate(DataContext context, String key)
```

**Parameter Description**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `key` | String | Variable name |

**Return Value**: Returns the variable value (`String`), or `NULL` if the variable doesn't exist.

::: warning Note
Since `get` is a SQL keyword, you need to wrap the function name with backticks when using it, written as `` `get` ``.
:::

**Usage Example**:

```sql
-- Set variable
SET 'user_id' = '12345';

-- Get variable value
SELECT `get`('user_id') AS user_id;

-- Use in expressions
SELECT `get`('user_id') || '_suffix' AS user_id_with_suffix;

-- Type conversion
SELECT CAST(`get`('limit_count') AS INT) AS limit_count;

-- Get variable name from table and use it
CACHE TABLE var_names AS SELECT 'user_id' AS var_name;
SELECT `get`(var_name) AS var_value FROM var_names;
```

**Working Principle**:
1. Function receives a variable name as parameter
2. Looks up the corresponding variable value from the execution context (`ExecuteContext`)
3. Returns the variable value, or `NULL` if the variable doesn't exist

**Typical Use Cases**:
- Parameterized SQL queries
- Dynamic configuration passing
- Cross-statement variable sharing

## Custom UDF

Refer to [Programming Model](program_model.md#udf) documentation for how to develop custom UDFs.
