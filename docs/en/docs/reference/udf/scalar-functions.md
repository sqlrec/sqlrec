# Scalar Functions

Scalar functions are used in expressions such as `SELECT`, `WHERE`, and `ORDER BY`, and return one value for each input row. SQLRec provides both Calcite standard functions and built-in scalar functions for recommendation workloads.

## Calcite Built-in Functions

In addition to SQLRec's own UDFs, regular `SELECT` queries can directly use the Apache Calcite 1.32.0 standard functions bundled with SQLRec. They do not need to be registered with `CREATE FUNCTION`.

```sql
SELECT ABS(-10), POWER(2, 3), UPPER('sqlrec');

SELECT category,
       COUNT(*) AS item_count,
       ROW_NUMBER() OVER (PARTITION BY category ORDER BY score DESC) AS row_num
FROM candidates
GROUP BY category, score;

SELECT JSON_VALUE('{"item_id": 1001}', 'strict $.item_id');
```

The following table lists the main functions currently verified by SQLRec. The function names are representative; refer to the Calcite SQL documentation for accepted argument types and exact syntax.

| Category | Functions |
|----------|-----------|
| Numeric | `ABS`, `ACOS`, `ASIN`, `ATAN`, `ATAN2`, `CBRT`, `CEIL`, `COS`, `COT`, `DEGREES`, `EXP`, `FLOOR`, `LN`, `LOG10`, `MOD`, `PI`, `POWER`, `RADIANS`, `RAND`, `RAND_INTEGER`, `ROUND`, `SIGN`, `SIN`, `SQRT`, `TAN`, `TRUNCATE` |
| VARCHAR | `ASCII`, `CHAR_LENGTH`, `CHARACTER_LENGTH`, `UPPER`, `LOWER`, `INITCAP`, `POSITION`, `OVERLAY`, `REPLACE`, `SUBSTRING`, `TRIM`, and the `\|\|` concatenation operator |
| Null handling and conversion | `COALESCE`, `NULLIF`, `CAST`, `CASE` |
| Date and time | `CURRENT_DATE`, `CURRENT_TIME`, `CURRENT_TIMESTAMP`, `EXTRACT`, `YEAR`, `QUARTER`, `MONTH`, `WEEK`, `DAYOFYEAR`, `DAYOFMONTH`, `DAYOFWEEK`, `HOUR`, `MINUTE`, `SECOND`, `LAST_DAY`, `TIMESTAMPADD`, `TIMESTAMPDIFF` |
| Collections | `ARRAY`, `MAP`, and `MULTISET` constructors, collection item access, `CARDINALITY`, and `ELEMENT` |
| Aggregate | `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `EVERY`, `SOME`, `ANY_VALUE`, `SINGLE_VALUE`, `MODE`, `APPROX_COUNT_DISTINCT`, `BIT_AND`, `BIT_OR`, `BIT_XOR`, `COLLECT`, `LISTAGG`, `FUSION`, `INTERSECTION` |
| Statistical aggregate | `STDDEV`, `STDDEV_POP`, `STDDEV_SAMP`, `VARIANCE`, `VAR_POP`, `VAR_SAMP`, `COVAR_POP`, `COVAR_SAMP`, `REGR_COUNT`, `REGR_SXX`, `REGR_SYY` |
| Grouping and window | `GROUPING`, `GROUPING_ID`, `GROUP_ID`, `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `NTILE`, `FIRST_VALUE`, `LAST_VALUE`, `NTH_VALUE`, `LEAD`, `LAG` |
| JSON | `JSON_EXISTS`, `JSON_VALUE`, `JSON_QUERY`, `JSON_OBJECT`, `JSON_ARRAY`, `JSON_OBJECTAGG`, `JSON_ARRAYAGG`, `JSON_TYPE`, `JSON_DEPTH`, `JSON_LENGTH`, `JSON_KEYS`, `JSON_PRETTY`, `JSON_REMOVE`, `JSON_STORAGE_SIZE` |

Be aware of the following limitations:

- `PI` is a niladic function. Use `SELECT PI`, not `PI()`.
- SQLRec currently registers only Calcite's standard operator table. It does not register the MySQL, PostgreSQL, Oracle, Spark, or BigQuery `SqlLibrary` extensions. MySQL-style lexical parsing does not automatically enable dialect-specific functions such as `IFNULL`, `DATE_FORMAT`, or `NVL`.
- `CUME_DIST`, `PERCENT_RANK`, `PERCENTILE_CONT`, and `PERCENTILE_DISC` currently cannot be converted to an executable Enumerable plan.
- The runtime context required by `LOCALTIME`, `LOCALTIMESTAMP`, `USER`, `CURRENT_USER`, `SESSION_USER`, `SYSTEM_USER`, and `CURRENT_SCHEMA` is not fully provided yet.
- `OCTET_LENGTH` does not work with a regular `VARCHAR`; the current Calcite implementation expects a binary string.
- `TUMBLE`, `HOP`, `SESSION`, and functions specific to `MATCH_RECOGNIZE` are not ordinary scalar UDFs and do not currently have an end-to-end SQLRec support guarantee.

References:

- [Apache Calcite SQL language and built-in function reference](https://calcite.apache.org/docs/reference.html)

The Calcite reference may describe a newer version than the one used by SQLRec and also includes dialect extensions. Use the verified list above and the project's actual Calcite dependency version when determining whether a function is available in SQLRec.

## SQLRec Built-in Scalar Functions

| Purpose | Functions |
|---------|-----------|
| Array predicates | [`array_contains`](#array-contains), [`array_contains_all`](#array-contains-all), [`array_contains_any`](#array-contains-any) |
| Vector operations | [`random_vec`](#random-vec), [`l2_norm`](#l2-norm), [`ip`](#ip) |
| Request identifiers | [`uuid`](#uuid) |
| Execution variables | [`get`](#get), [`get_or_default`](#get-or-default) |
| Remote models | [`llm_embed_text`, `llm_complete`, `ark_embed_image`](#remote-model-functions) |

### array_contains

Array contains function that checks if an array contains a specified element.

**Parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `list` | ARRAY | Input array |
| `element` | ANY | Element to check |

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

**Parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `list` | ARRAY | Input array |
| `elements` | ARRAY | List of elements to check |

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

**Parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `list` | ARRAY | Input array |
| `elements` | ARRAY | List of elements to check |

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

**Parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `dimensionStr` | VARCHAR | Vector dimension, must be a positive integer string |

**Return Value**: Returns a normalized random vector (`ARRAY<DOUBLE>`), with L2 norm equal to 1.

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

**Notes**:
- Dimension must be a positive integer
- Generated vector is already normalized and can be used directly for similarity calculation

---

### uuid

UUID generation function that generates a random UUID string.

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

**Parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `vector` | ARRAY | Input vector, must be a list of numbers |

**Return Value**: Returns normalized vector (`ARRAY<DOUBLE>`), making the vector's L2 norm equal to 1.

**Usage Example**:

```sql
-- Normalize user vectors
CACHE TABLE normalized_user AS
SELECT
    user_id,
    l2_norm(user_embedding) AS normalized_embedding
FROM user_features;
```

---

### ip

Inner product calculation function that calculates the inner product (dot product) of two vectors.

**Parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `emb1` | ARRAY | First vector, must be a list of numbers |
| `emb2` | ARRAY | Second vector, must be a list of numbers |

**Return Value**: Returns inner product of two vectors (`DOUBLE`).

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

---

### get

Variable retrieval function that gets the value of a variable from the execution context. Commonly used to reference variables set via the `SET` statement in SQL.

**Parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `key` | VARCHAR | Variable name |

**Return Value**: Returns the variable value (`VARCHAR`). An absent execution variable falls back to the same-named environment variable by default; returns `NULL` if neither exists. Set the server environment variable `ENABLE_VARIABLE_ENV_FALLBACK=false` to disable fallback. Explicit empty strings do not trigger fallback.

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

**Typical Use Cases**:
- Parameterized SQL queries
- Dynamic configuration passing
- Cross-statement variable sharing

---

### get_or_default

Variable retrieval function with default value that gets the value of a variable from the execution context, returning the specified default value if the variable doesn't exist.

**Parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `key` | VARCHAR | Variable name |
| `defaultValue` | VARCHAR | Default value to return when variable doesn't exist |

**Return Value**: Returns the variable value (`VARCHAR`), resolving the execution variable, same-named environment variable, then `defaultValue` by default. Set the server environment variable `ENABLE_VARIABLE_ENV_FALLBACK=false` to disable environment fallback. Explicit empty strings still take precedence.

**Usage Example**:

```sql
SET 'limit_count' = '10';

SELECT CAST(`get_or_default`('limit_count', '50') AS INT) AS limit_count;
```

To select a function dynamically, use `GET_OR_DEFAULT(...)` as shown in [Writing a Recommendation Flow](../../guides/recommendation-flow.md#select-a-function-dynamically).

**Typical Use Cases**:
- Dynamic function calls with fallback functions
- Configuration retrieval with default settings
- Parameterized SQL with default parameters


### Remote model functions

All three functions take five `VARCHAR` arguments:

| Function | Signature | Result |
|----------|-----------|--------|
| `llm_embed_text` | `(endpoint, api_key, model, text, options_json)` | `ARRAY<DOUBLE>` |
| `llm_complete` | `(endpoint, api_key, model, prompt, options_json)` | `VARCHAR` |
| `ark_embed_image` | `(endpoint, api_key, model, image, options_json)` | `ARRAY<DOUBLE>` |

Pass the complete HTTP(S) endpoint; paths are never appended. Model names are passed through. A nonempty key is sent as `Authorization: Bearer ...`; an empty key supports unauthenticated compatible services. Inject secrets through the execution context and use `` `get`('llm_api_key') `` rather than SQL literals, which can appear in SQL logs, plans and metadata. UDF exceptions omit credentials, input and response bodies.

```sql
SELECT llm_embed_text(
    'https://api.openai.com/v1/embeddings',
    `get`('llm_api_key'), 'text-embedding-3-small', title,
    '{"request":{"dimensions":1024},"runtime":{"timeout_ms":10000}}'
) AS embedding FROM items;

SELECT llm_complete(
    'https://api.openai.com/v1/chat/completions',
    `get`('llm_api_key'), `get`('llm_model'), content,
    '{"request":{"max_completion_tokens":128}}'
) AS summary FROM articles;

SELECT ark_embed_image(
    'https://ark.cn-beijing.volces.com/api/v3/embeddings/multimodal',
    `get`('ark_api_key'), 'doubao-embedding-vision-251215', image_url,
    '{"request":{"dimensions":1024},"runtime":{"timeout_ms":10000}}'
) AS embedding FROM items;
```

Options must be a strict JSON object. Use `'{}'` or SQL `NULL` for defaults. Unknown fields are rejected.

| Section | Allowed fields |
|---------|----------------|
| `request`, text embedding | `dimensions` (positive integer), `user` |
| `request`, completion | `temperature` (0–2), `top_p` (0–1), `max_completion_tokens` (positive integer), `reasoning_effort`, `user` |
| `request`, Ark image embedding | `dimensions` (1024 or 2048), `instructions` |
| `runtime` | `timeout_ms` (positive integer, default 30000, maximum 300000) |

Only explicitly supplied model parameters are sent; they must be supported by the remote model. Runtime options are never sent to the API. The timeout covers waiting for a concurrency slot and receiving the complete HTTP response. Local validation errors, HTTP failures, timeouts, invalid remote responses and interruptions all throw exceptions. There is no option to return `NULL` on failure. A SQL `NULL` input returns `NULL` without validating other arguments or making a request. Empty text embedding inputs are rejected.

Embedding requests force `encoding_format="float"`. Results are nonempty finite vectors returned as `List<Double>`, with an explicit dimension checked when supplied. They are not normalized locally. OpenAI results use `data[0].embedding`; Ark results use `data.embedding`. Images can be accessible HTTP(S) URLs or `data:image/...;base64,...` URIs. The Ark function exposes single-image dense vectors only; video, mixed content, sparse and multi-vector outputs are outside this interface. Text vectors from a different model cannot be assumed to align with these image vectors.

Completions wrap the prompt in one user message and force `stream=false`, `n=1`, `store=false`. The result is `choices[0].message.content`. Refusals, content filtering and missing text are remote errors; a length-limited completion returns its partial text. Responses API, tools and conversation history are not supported.

A shared JDK HTTP client reuses connections, limits in-flight requests to 32 per classloader, uses a 2-second connection timeout and does not follow redirects. There are no application retries or result caching. Functions are declared non-deterministic: SQLRec compilation and Flink planning do not call the API. Runtime calls are synchronous per row. Re-execution or task recovery can repeat calls; remote requests are not exactly-once.

SQLRec registers these functions by default. In Flink, deploy the UDF JAR with Hive support (HiveCatalog/HiveModule) and register:

```sql
CREATE FUNCTION llm_embed_text AS 'com.sqlrec.udf.scalar.LlmEmbedTextFunction';
CREATE FUNCTION llm_complete AS 'com.sqlrec.udf.scalar.LlmCompleteFunction';
CREATE FUNCTION ark_embed_image AS 'com.sqlrec.udf.scalar.ArkEmbedImageFunction';
```

Protocol references: [OpenAI Embeddings](https://developers.openai.com/api/reference/resources/embeddings/methods/create), [OpenAI Chat Completions](https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create), [Ark multimodal embeddings](https://console.volcengine.com/ark/region:cn-beijing/docs/multimodal-vectorization-api).
