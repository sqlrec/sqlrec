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

### batch_call_service

Batch model service call function used in Flink SQL to batch call deployed model services for inference. This function sends multiple rows of data in batches to a remote service and merges the returned results with the original data.

::: warning Note
This function can only be used in Flink SQL and does not support SQLRec's CACHE TABLE syntax.
:::

**Function Signature**:

```java
@FunctionHint(output = @DataTypeHint("ROW&lt;" +
        "long_map MAP&lt;STRING, BIGINT&gt;, " +
        "double_map MAP&lt;STRING, DOUBLE&gt;, " +
        "string_map MAP&lt;STRING, STRING&gt;, " +
        "long_array_map MAP&lt;STRING, ARRAY&lt;BIGINT&gt;&gt;, " +
        "double_array_map MAP&lt;STRING, ARRAY&lt;DOUBLE&gt;&gt;, " +
        "string_array_map MAP&lt;STRING, ARRAY&lt;STRING&gt;&gt;" +
        "&gt;"))
public void eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object... args)
```

**Parameter Description**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `serviceUrl` | String | URL address of the model service |
| `batchSize` | Integer | Batch size, number of rows sent per request |
| `fieldName-value pairs` | Object... | Field name-value pairs specifying fields to send to the service, must appear in pairs |

**Return Value**: Returns a ROW type containing the following fields:

| Field Name | Type | Description |
|------------|------|-------------|
| `long_map` | MAP&lt;STRING, BIGINT&gt; | Map of long integer fields |
| `double_map` | MAP&lt;STRING, DOUBLE&gt; | Map of double precision floating-point fields |
| `string_map` | MAP&lt;STRING, STRING&gt; | Map of string fields |
| `long_array_map` | MAP&lt;STRING, ARRAY&lt;BIGINT&gt;&gt; | Map of long integer array fields |
| `double_array_map` | MAP&lt;STRING, ARRAY&lt;DOUBLE&gt;&gt; | Map of double precision floating-point array fields |
| `string_array_map` | MAP&lt;STRING, ARRAY&lt;STRING&gt;&gt; | Map of string array fields |

**Usage Example**:

```sql
-- Create temporary function
CREATE TEMPORARY FUNCTION batch_call_service AS 'com.sqlrec.udf.udtf.BatchCallServiceUDTF';

-- Call model service to generate item embeddings
INSERT INTO item_embedding
SELECT 
    r.long_map['movie_id'] AS id,
    r.string_map['title'] AS title,
    r.string_array_map['genres'] AS genres,
    r.double_array_map['item_tower_emb'] AS embedding
FROM ml_movies, LATERAL TABLE(batch_call_service(
    'http://test-recall-service-item.sqlrec.svc.cluster.local:80/predict', 
    128, 
    'movie_id', movie_id, 
    'title', title, 
    'genres', genres
)) AS r
WHERE dt = '2024-01-01';
```

**Working Principle**:
1. Function receives multiple rows of data and caches field name-value pairs in a buffer
2. When buffer size reaches `batchSize`, data is sent in batch to the model service
3. Model service receives JSON array format request and returns a JSON object containing prediction results
4. Function merges prediction results with original data, classifying them by type into different Maps
5. Each row of data outputs a ROW, accessible via Map for original fields and prediction results

**Request Format**:

JSON format sent to the model service is an array of objects:

```json
[
  {"movie_id": 1, "title": "Toy Story", "genres": ["Animation", "Comedy"]},
  {"movie_id": 2, "title": "Jumanji", "genres": ["Adventure", "Children"]}
]
```

**Response Format**:

Model service should return a JSON object where each field's value is an array with the same length as the request data rows:

```json
{
  "item_tower_emb": [[0.1, 0.2, ...], [0.3, 0.4, ...]],
  "score": [0.95, 0.87]
}
```

**Notes**:
- This function can only be used in Flink SQL and requires the `LATERAL TABLE` syntax
- `batchSize` should be adjusted based on model service performance and network latency, typically set to 64-256
- Model service needs to support POST requests, receiving JSON arrays and returning JSON objects
- Array fields in the results are automatically matched to input data by row index
- Remaining data in the buffer is processed in the `close()` method

---

### dpp_diversity

DPP (Determinantal Point Process) diversity function that implements diversity in recommendation results based on determinantal point processes. Uses the fast greedy MAP inference algorithm (referencing the Hulu NIPS 2018 paper) to improve recommendation diversity while maintaining relevance.

**Function Signature**:

```java
public CacheTable evaluate(
    CacheTable input,
    String embeddingColumnName,
    String scoreColumnName,
    String theta,
    String maxLength
)
```

**Parameter Description**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `input` | CacheTable | Input table |
| `embeddingColumnName` | String | Embedding column name, used to compute item similarity |
| `scoreColumnName` | String | Relevance score column name, used to measure item quality |
| `theta` | String | Relevance-diversity trade-off parameter, range [0, 1). Closer to 1 favors relevance, closer to 0 favors diversity |
| `maxLength` | String | Maximum number of records to return |

**Return Value**: Returns diversified `CacheTable` with same structure as input table.

**Usage Example**:

```sql
-- DPP diversity: theta=0.5 balances relevance and diversity, return 20 records
CACHE TABLE dpp_result AS
CALL dpp_diversity(rec_item, 'item_embedding', 'score', '0.5', '20');
```

**Working Principle**:
1. Extract relevance scores and embedding vectors from the input table
2. Clip negative scores to a small positive value, then apply exponential transform: `score = exp(alpha * r)`, where `alpha = theta / (2 * (1 - theta))`
3. L2 normalize embedding vectors
4. Build kernel matrix `L = Diag(scores) * S * Diag(scores)`, where `S[i][j] = (1 + dot(emb[i], emb[j])) / 2`
5. Run greedy DPP MAP inference algorithm to select a diverse subset

**Notes**:
- `theta` must be in the range [0, 1)
- `maxLength` must be a positive integer
- All vectors in the embedding column must have the same dimension
- Rows with NULL scores or embeddings are automatically skipped

---

### rule_diversity

Rule-based diversity function that reorders recommendation results using a greedy algorithm based on user-defined rules, supporting flexible diversity constraint configurations.

**Function Signature**:

```java
public CacheTable evaluate(
    CacheTable targetTable,
    CacheTable ruleTable,
    String maxReturn
)
```

**Parameter Description**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `targetTable` | CacheTable | Target table to be diversified |
| `ruleTable` | CacheTable | Rule table defining diversity constraints |
| `maxReturn` | String | Maximum number of records to return |

**Rule Table Fields**:

| Field Name | Type | Description |
|------------|------|-------------|
| `window_size` | Integer | Window size |
| `window_start` | Integer | Window start position (1-based) |
| `window_num` | Integer | Number of sliding windows (1 = no sliding) |
| `diversity_column` | String | Column name in target table for diversification |
| `diversity_value` | String | Value to match (empty/null = constraint applies to each distinct value) |
| `op` | String | Comparison operator (`>`, `=`, `<`) |
| `diversity_num` | Integer | Constraint threshold |
| `weight` | Double | Rule weight, higher weight = higher priority |

**Return Value**: Returns diversified `CacheTable` with same structure as target table.

**Usage Example**:

```sql
-- Create rule table
CACHE TABLE diversity_rules AS
SELECT
    5 AS window_size,
    1 AS window_start,
    1 AS window_num,
    'category' AS diversity_column,
    '' AS diversity_value,
    '<' AS op,
    2 AS diversity_num,
    1.0 AS weight
UNION ALL
SELECT
    3, 1, 1, 'brand', 'Nike', '=', 1, 2.0;

-- Rule-based diversification, return 20 records
CACHE TABLE rule_diversify_result AS
CALL rule_diversity(rec_item, diversity_rules, '20');
```

**Working Principle**:
1. Parse rule table and build sliding windows for each rule
2. For each output position, greedily select the unassigned item with the minimum constraint violation penalty
3. If no violation, select by original rank order
4. Higher weight rules incur larger penalties when violated

**Notes**:
- Rule table must contain all required fields
- `diversity_column` must exist in the target table
- When `diversity_value` is empty, the constraint applies to each distinct attribute value in the window
- The diversity column in the target table can be a single value or a list

---

### json_to_table

JSON to table function that converts a JSON string into a CacheTable.

**Function Signature**:

```java
public CacheTable evaluate(String jsonString)
```

**Parameter Description**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `jsonString` | String | JSON string, supports JSON object or JSON array |

**Return Value**: Returns converted `CacheTable` with column names and types automatically inferred from JSON content.

**Usage Example**:

```sql
-- Convert JSON array to table
CACHE TABLE json_result AS
CALL json_to_table('[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]');

-- Convert single JSON object to table
CACHE TABLE single_obj AS
CALL json_to_table('{"id": 1, "name": "Alice", "score": 95.5}');
```

**Working Principle**:
1. Parse JSON string, supporting both JSON objects and JSON arrays
2. Collect all keys as column names, preserving insertion order
3. Automatically infer column types from the first non-null value (`BOOLEAN`, `DOUBLE`, `VARCHAR`, `ARRAY<...>`)
4. Convert each JSON object to a row

**Notes**:
- JSON string cannot be empty
- Must be in JSON object or JSON array format
- Nested objects in arrays are stored as JSON strings in `VARCHAR` type
- Array types automatically infer element types (`ARRAY<DOUBLE>`, `ARRAY<BOOLEAN>`, `ARRAY<VARCHAR>`)

---

### tag_to_vec

Tag to vector function that converts a tag column to a Multi-Hot vector representation.

**Function Signature**:

```java
public CacheTable evaluate(CacheTable input, String tagColName, String outputColName)
```

**Parameter Description**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `input` | CacheTable | Input table |
| `tagColName` | String | Tag column name, can be a single value or a list |
| `outputColName` | String | Output vector column name |

**Return Value**: Returns `CacheTable` with the vector column added, new column type is `ARRAY<FLOAT>`.

**Usage Example**:

```sql
-- Convert user tags to Multi-Hot vectors
CACHE TABLE user_with_vec AS
CALL tag_to_vec(user_info, 'tags', 'tag_vector');

-- Convert item categories to vectors
CACHE TABLE item_with_vec AS
CALL tag_to_vec(item_info, 'categories', 'category_vector');
```

**Working Principle**:
1. Traverse all rows, collect all unique tags from the tag column, and build a tag-to-index mapping
2. Generate a Multi-Hot vector for each row, with dimension equal to the number of unique tags
3. If a row contains a tag, the corresponding position is 1.0, otherwise 0.0
4. Append the vector column to the original table

**Notes**:
- Tag column can be a single value (string) or a list (`ARRAY<STRING>`)
- Output column name cannot duplicate existing column names
- Vector dimension depends on the total number of unique tags across all rows

---

### weighted_merge

Weighted merge function that merges multiple tables into one based on specified weights, with deduplication by primary key.

**Function Signature**:

```java
public CacheTable evaluate(String primaryKey, String weights, String limit, CacheTable... tables)
```

**Parameter Description**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `primaryKey` | String | Primary key column name for deduplication |
| `weights` | String | Weights for each table, comma-separated, e.g. `"2,1,1"` |
| `limit` | String | Maximum number of records to return |
| `tables` | CacheTable... | Two or more input tables, all must have the same schema |

**Return Value**: Returns merged `CacheTable` with same structure as input tables.

**Usage Example**:

```sql
-- Merge three recall channels with weights 2:1:1, return 100 records
CACHE TABLE merged_recall AS
CALL weighted_merge('item_id', '2,1,1', '100', recall_channel_a, recall_channel_b, recall_channel_c);

-- Merge two recall channels with weights 3:2, return 50 records
CACHE TABLE merged_result AS
CALL weighted_merge('item_id', '3,2', '50', recall_a, recall_b);
```

**Working Principle**:
1. In each round, take the weight number of records from each table in order
2. Deduplicate by primary key, already seen records are not added again
3. Repeat rounds until `limit` is reached or all tables are exhausted
4. Tables with higher weights contribute more records per round

**Notes**:
- All input tables must have identical schema (column names and types)
- Number of weights must match the number of tables
- Weights and limit must be positive integers
- Primary key column must exist in all tables

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

---

### get_or_default

Variable retrieval function with default value that gets the value of a variable from the execution context, returning the specified default value if the variable doesn't exist.

**Function Signature**:

```java
public static String evaluate(DataContext context, String key, String defaultValue)
```

**Parameter Description**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `key` | String | Variable name |
| `defaultValue` | String | Default value to return when variable doesn't exist |

**Return Value**: Returns the variable value (`String`), or `defaultValue` if the variable doesn't exist.

**Usage Example**:

```sql
-- Set variable
SET 'func_name' = 'add_col';

-- Get variable value, use default if not exists
SELECT `get_or_default`('user_id', 'default_user') AS user_id;

-- Dynamic function call: use variable value when exists
CALL `get_or_default`('func_name', 'shuffle')(my_table);

-- Dynamic function call: use default value when variable doesn't exist
CALL `get_or_default`('unknown_func', 'shuffle')(my_table);
```

**Working Principle**:
1. Function receives variable name and default value as parameters
2. Looks up the corresponding variable value from the execution context (`ExecuteContext`)
3. Returns the variable value if it exists, otherwise returns the default value

**Typical Use Cases**:
- Dynamic function calls with fallback functions
- Configuration retrieval with default settings
- Parameterized SQL with default parameters

## Custom UDF

Refer to [Programming Model](program_model.md#udf) documentation for how to develop custom UDFs.
