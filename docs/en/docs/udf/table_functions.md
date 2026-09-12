# Table Functions

SQLRec table functions are called with `CALL`. They accept one or more cached tables and return a new cached table, except for side-effect-only functions. Pass arguments in the documented order and wrap string arguments in single quotes.

## Function List

| Scenario | Functions |
|----------|-----------|
| Result processing | [`dedup`](#dedup), [`shuffle`](#shuffle), [`add_col`](#add_col), [`truncate_table`](#truncate_table) |
| Diversity | [`window_diversify`](#window_diversify), [`dpp_diversity`](#dpp_diversity), [`rule_diversity`](#rule_diversity) |
| Conversion and merging | [`json_to_table`](#json_to_table), [`tag_to_vec`](#tag_to_vec), [`weighted_merge`](#weighted_merge) |
| External services | [`call_service`](#call_service), [`batch_call_service`](#batch_call_service), [`call_sqlrec_api`](#call_sqlrec_api), [`get_growthbook_features`](#get_growthbook_features) |
| Variables and observability | [`get_variables`](#get_variables), [`set_variables`](#set_variables), [`feature_coverage_metrics`](#feature_coverage_metrics) |
| Testing utility | [`sleep`](#sleep) |

### dedup

Deduplication function that excludes records from the input table that already exist in the deduplication table based on specified columns.

**Parameters**:

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
FROM user_info JOIN exposure_item ON user_id = user_info.id;

-- Exclude exposed items from recall results
CACHE TABLE dedup_recall AS
CALL dedup(recall_item, exposured_item, 'item_id', 'item_id');
```

---

### shuffle

Random shuffle function that randomly sorts records in the input table.

**Parameters**:

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

**Parameters**:

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

---

### add_col

Add column function that adds a new column to the input table with the same value for all rows.

**Parameters**:

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

Model service call function used to call deployed model services for inference. See [Models documentation](../model/basic_concepts.md#call_service) for details.

SQL supports these two call forms:

- `CALL call_service(serviceName, input)`: calls the service with row-wise JSON. The result contains the input columns followed by model output columns.
- `CALL call_service(serviceName, user, item)`: calls the service in User-Item mode with a column-oriented JSON request body. The result keeps the Item columns and appends model output columns.

User-Item request rules:

1. The User table must contain exactly one row; the Item table may contain multiple rows.
2. Only model input fields are considered. A field belongs to User when its name matches a User table column (case-insensitive); otherwise it belongs to Item. If both tables contain the same field, the User field takes precedence. A model field missing from its selected table is omitted from the request, as are extra table columns not declared as model inputs.
3. Every field is serialized as a JSON array. User fields are single-element arrays, so one set of user data is sent only once per request. Item fields become arrays in Item row order; user data is not repeated for every item.

For example, one User row and three Item rows produce:

```json
{
  "user_id": [1001],
  "user_age": [25],
  "item_id": [1, 2, 3],
  "category": ["phone", "tablet", "laptop"]
}
```

When the Item table is empty, no HTTP request is sent. The function returns an empty table whose schema consists of the Item columns followed by the model output columns.

HTTP connect/read/write timeouts default to 30 seconds. See the [model documentation](../model/basic_concepts.md#call_service) for the protocol and examples.

---

### batch_call_service

Batch model service call function used in Flink SQL to batch call deployed model services for inference. This function sends multiple rows of data in batches to a remote service and merges the returned results with the original data.

::: warning Note
This function can only be used in Flink SQL and does not support SQLRec's CACHE TABLE syntax.
:::

Invoke this function through `LATERAL TABLE`. It sends a JSON array whenever it collects `batchSize` rows and also sends the final partial batch. The service must return a JSON object whose array values map to input rows by position.

**Parameters**:

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

---

### dpp_diversity

DPP (Determinantal Point Process) diversification combines item embeddings and relevance scores to diversify recommendation results.

**Parameters**:

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

**Notes**:
- `theta` must be in the range [0, 1)
- `maxLength` must be a positive integer
- All vectors in the embedding column must have the same dimension
- Rows with NULL scores or embeddings are automatically skipped

---

### rule_diversity

Reorders recommendation results with user-defined window rules. Use it to constrain several attributes, such as category and brand, at the same time.

**Parameters**:

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

**Notes**:
- Rule table must contain all required fields
- `diversity_column` must exist in the target table
- When `diversity_value` is empty, the constraint applies to each distinct attribute value in the window
- The diversity column in the target table can be a single value or a list

---

### json_to_table

JSON to table function that converts a JSON string into a CacheTable.

**Parameters**:

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

**Notes**:
- JSON string cannot be empty
- Must be in JSON object or JSON array format
- Nested objects in arrays are stored as JSON strings in `VARCHAR` type
- Array types automatically infer element types (`ARRAY<DOUBLE>`, `ARRAY<BOOLEAN>`, `ARRAY<VARCHAR>`)

---

### tag_to_vec

Tag to vector function that converts a tag column to a Multi-Hot vector representation.

**Parameters**:

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

**Notes**:
- Tag column can be a single value (string) or a list (`ARRAY<STRING>`)
- Output column name cannot duplicate existing column names
- Vector dimension depends on the total number of unique tags across all rows

---

### weighted_merge

Weighted merge function that merges multiple tables into one based on specified weights, with deduplication by primary key.

**Parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `primaryKey` | String | Primary key column name for deduplication |
| `weights` | String | Weights for each table, comma-separated, e.g. `"2,1,1"` |
| `limit` | String | Maximum number of records to return |
| `tables` | CacheTable... | One or more input tables; all must have the same schema |

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

**Notes**:
- All input tables must have identical schema (column names and types)
- An empty `primaryKey` disables deduplication; when specified, rows are deduplicated by that column's string value
- Number of weights must match the number of tables
- Weights and limit must be positive integers
- Primary key column must exist in all tables
- In a SQL function with `IGNORE_UNION_EXCEPTION=true`, a failed input can be treated as an empty table when it is consumed only by UNION or `weighted_merge`; the remaining inputs continue merging. This fallback does not apply when `GET()` selects `weighted_merge` dynamically

---

### call_sqlrec_api

Remote SQLRec API call function. Invokes an API published on a remote SQLRec instance (i.e. a SQL function exposed via `CREATE API`) and converts the returned result into a `CacheTable`. Useful for cross-cluster / cross-instance calls to other SQLRec services.

**Parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `url` | String | Remote SQLRec API URL, e.g. `http://host:port/api/v1/function_name` |
| `tables` | CacheTable... | One or more input tables; the table name is used as the input placeholder for the remote function |

**Return Value**: Returns a `CacheTable` containing the remote function's result; column names and types are inferred from the response data.

**Usage Example**:

```sql
-- Prepare input data
CACHE TABLE user_input AS
SELECT 1001 AS user_id, 'Alice' AS user_name;

-- Call a recommendation API published on a remote SQLRec instance
CACHE TABLE remote_rec AS
CALL call_sqlrec_api(
    'http://remote-sqlrec:30001/api/v1/recommend',
    user_input
);

SELECT * FROM remote_rec;
```

**Notes**:
- `url` must not be empty and must point to a valid SQLRec API endpoint
- At least one input table is required, and each input table must have a name
- An exception is thrown when the remote API call fails (empty data or error message returned)
- The input table name must match the input table placeholder in the remote function definition

---

### truncate_table

Table truncation function that extracts rows within a specified range from the input table.

**Parameters**:

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

---

### set_variables

Set variables function that reads key-value pairs from a table and sets them in the execution context.

**Parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
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

**Parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
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

**Notes**:
- If table is empty, it will be skipped
- Metrics name cannot be empty

---

### get_growthbook_features

GrowthBook feature retrieval function that fetches A/B experiment feature values from the GrowthBook platform, sets experiment parameters as execution context variables, and returns experiment tracking data for metrics calculation.

**Parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `apiHost` | String | GrowthBook API host URL |
| `clientKey` | String | GrowthBook client key |
| `usertable` | CacheTable | User table, columns will be passed as user attributes to GrowthBook |
| `featureKeys` | String... | One or more feature key names |

**Return Value**: Returns a `CacheTable` containing experiment tracking data with the following fields:

| Field Name | Type | Description |
|------------|------|-------------|
| `experiment_id` | VARCHAR | Experiment identifier |
| `variation_id` | VARCHAR | Experiment variation identifier |
| `user_id` | VARCHAR | User identifier |

**Usage Example**:

```sql
-- Get GrowthBook features and set variables
CACHE TABLE gb_tracking AS
CALL get_growthbook_features(
    'https://cdn.growthbook.io',
    'sdk-abc123',
    user_info,
    'new_recommendation_algo',
    'ui_theme'
);

-- Use the set experiment variables
SELECT `get`('new_recommendation_algo') AS algo;
```

**Notes**:
- `apiHost` and `clientKey` cannot be empty
- `usertable` cannot be null
- At least one `featureKey` must be specified
- An exception will be thrown if GrowthBookClient initialization fails
- The same client instance is reused for the same `apiHost` and `clientKey` combination

---

### sleep

Sleep function that causes the current thread to sleep for a specified number of milliseconds. Mainly used for testing, rate limiting, or simulating latency scenarios.

**Parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `millisStr` | String | Sleep duration in milliseconds, must be a non-negative integer string |

**Return Value**: No return value.

**Usage Example**:

```sql
-- Sleep for 1000 milliseconds (1 second)
CALL sleep('1000');

-- Sleep for 500 milliseconds
CALL sleep('500');
```

**Notes**:
- `millisStr` must be a valid long integer string
- Sleep duration must be non-negative
- This function only has side effects and no return value

---
