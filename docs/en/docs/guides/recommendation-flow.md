# Writing a Recommendation Flow

SQLRec represents a multi-step business flow as a **SQL function**. A typical recommendation function receives user data, performs recall, deduplication, ranking, and diversification, then returns the final items.

This guide is for people writing business SQL. See the [SQL Syntax Reference](../reference/sql.md) for exact grammar and [Architecture](../reference/architecture.md) for execution-engine details.

## A Minimal Function

```sql
CREATE OR REPLACE SQL FUNCTION recommend;

DEFINE INPUT TABLE user_info (
  user_id BIGINT
);

CACHE TABLE result AS
SELECT item_id, score
FROM hot_item
ORDER BY score DESC
LIMIT 10;

RETURN result;
```

The function has four essential parts:

1. `CREATE SQL FUNCTION` starts the definition.
2. `DEFINE INPUT TABLE` declares the table and fields the caller must provide.
3. `CACHE TABLE` stores an intermediate result.
4. A top-level `RETURN` returns the result and ends the definition.

Terminate every top-level statement in a multi-statement function with a semicolon.

## Input Tables

An input table is a table-valued function argument. At call time, pass compatible cached tables in declaration order.

Declare fields explicitly:

```sql
DEFINE INPUT TABLE user_info (
  user_id BIGINT,
  country VARCHAR,
  tags ARRAY<VARCHAR>
);
```

Or reuse an existing table's schema:

```sql
DEFINE INPUT TABLE user_info LIKE user_profile;
```

`LIKE` copies only the schema, not rows from `user_profile`.

## Cached Tables

`CACHE TABLE` stores an intermediate result for the current request:

```sql
CACHE TABLE interested_category AS
SELECT category
FROM user_info
JOIN user_interest_category
  ON user_interest_category.user_id = user_info.user_id
ORDER BY score DESC
LIMIT 10;
```

Later statements can query the table or pass it to another SQL or Java table function:

```sql
CACHE TABLE dedup_result AS
CALL dedup(recall_result, exposed_item, 'item_id', 'item_id');
```

A cached table belongs to one execution. Do not use it to retain data between API calls. For persistence, `INSERT` into a Redis, Kafka, JDBC, or other connector table.

## Calling Functions

Call a SQL function directly:

```sql
CALL recall(user_info);
```

Cache its result when later steps need it:

```sql
CACHE TABLE recall_result AS
CALL recall(user_info);
```

Built-in Java table functions such as `dedup`, `weighted_merge`, and `window_diversify` also use `CALL`:

```sql
CACHE TABLE final_result AS
CALL window_diversify(ranked_item, 'category', '3', '1', '10');
```

Pass a cached table name directly and quote string arguments. See [Table Functions](../reference/udf/table-functions.md) for the complete list.

To select a function at runtime, use an execution variable:

```sql
CALL `get_or_default`('rank_fun', 'default_rank')(user_info, recall_result)
LIKE FUNCTION 'default_rank';
```

Because a dynamic name does not reveal the return fields during compilation, declare the result schema with `LIKE FUNCTION` or `LIKE table_name`.

## Returning a Result

A SQL function must end with a top-level `RETURN`. It may return a cached table, a query, or a synchronous call:

```sql
RETURN result_table;

RETURN SELECT item_id, score FROM result_table;

RETURN CALL post_process(result_table);

RETURN;
```

`RETURN;` completes successfully with no data. `RETURN CALL ... ASYNC` is unsupported because an asynchronous call cannot provide a synchronous function result.

## Conditional Execution

Use a normal `IF` for a business condition:

```sql
IF (SELECT COUNT(*) > 0 FROM recall_result) THEN (
  CACHE TABLE result AS SELECT * FROM recall_result
) ELSE (
  CACHE TABLE result AS SELECT * FROM fallback_result
);
```

The condition query must return one Boolean value; `NULL` is treated as `false`. If both branches cache a table, they must write the same name with compatible schemas.

Use `IF TIMEIN` when the primary path should fall back after a timeout or exception:

```sql
IF TIMEIN (SELECT 100) THEN (
  CACHE TABLE result AS CALL online_rank(recall_result)
) ELSE (
  CACHE TABLE result AS SELECT * FROM recall_result
);
```

The `TIMEIN` query returns milliseconds:

- greater than zero: set a timeout for THEN and run ELSE after a timeout or ordinary exception;
- zero or less: do not set a timeout, but still run ELSE after an ordinary exception.

`IF TIMEIN` requires ELSE. Both branches must use `CACHE TABLE` or both must use `RETURN`. See [Timeouts, Fallbacks, and Error Recovery](./exception-recovery.md) for other recovery options.

An IF branch may return early:

```sql
IF (SELECT COUNT(*) = 0 FROM candidates) THEN (
  RETURN SELECT CAST(NULL AS BIGINT) AS item_id WHERE FALSE
);

RETURN SELECT item_id FROM candidates;
```

A branch-level `RETURN` ends that call, but it does not terminate the function definition at compile time, so the final top-level `RETURN` is still required. If both THEN and ELSE return, their schemas must be compatible and the IF must be followed immediately by an empty `RETURN;`.

## Execution Variables

Variables carry small request-specific settings such as a recall function, result limit, or experiment group:

```sql
SET 'limit_count' = '100';

SELECT CAST(`get`('limit_count') AS INT);

SELECT CAST(`get_or_default`('limit_count', '50') AS INT);
```

API `params` are added to the same execution context:

```json
{
  "data": {
    "user_info": [{"user_id": 1001}]
  },
  "params": {
    "rank_fun": "rank_fun_simple",
    "limit_count": "10"
  }
}
```

`get` is a SQL keyword, so quote it as a scalar function: `` `get`('name') ``.

## Partitioned Calls

For independent batch processing, split one input table and invoke the same function concurrently:

```sql
CACHE TABLE result AS
CALL process_batch(input_table)
LIKE input_table
PARTITION BY input_table SIZE 100;
```

- `PARTITION BY` selects the table to split.
- `SIZE` is the maximum rows per partition and may also come from `get()` or `get_or_default()`.
- The partitioned table must be an argument of the called function.
- Prefer `LIKE` to declare the merged result schema.

By default, one failed partition fails the complete call. Use `IGNORE_PARTITION_EXCEPTION` only when partial results are acceptable.

## Asynchronous Calls

Use `ASYNC` for a side task whose result is not needed, such as writing a recommendation log:

```sql
CALL save_rec_log(final_result) ASYNC;
```

The call returns immediately after submission. Later SQL must not assume it has completed. `ASYNC` cannot be used in `CACHE TABLE ... AS CALL` or `RETURN CALL`.

## Join Guidelines

SQLRec selects execution behavior from the table capabilities. For online queries:

- drive Redis, JDBC, MongoDB, and similar external lookups from a small cached table;
- include an equality condition on the external table's primary key where possible;
- do not assume every connector supports a full scan or every filter expression;
- trigger Milvus vector recall by joining a user-vector table to the Milvus table, ordering by a distance function, and applying `LIMIT`.

```sql
CACHE TABLE vector_recall AS
SELECT item_embedding.id AS item_id
FROM user_embedding
JOIN item_embedding ON 1 = 1
ORDER BY ip(user_embedding.embedding, item_embedding.embedding) DESC
LIMIT 300;
```

See [Connecting Data Sources](./data-sources.md) for connector capabilities.

After validating the function, follow [Publishing and Calling an API](./api.md) to expose it to an application.

## Complete Example

```sql
CREATE OR REPLACE SQL FUNCTION demo_rec;

DEFINE INPUT TABLE user_info(user_id BIGINT);

CACHE TABLE exposed_item AS
SELECT item_id
FROM user_info
JOIN demo_exposure_item
  ON demo_exposure_item.user_id = user_info.user_id;

CACHE TABLE interested_category AS
SELECT category
FROM user_info
JOIN demo_user_interest_category
  ON demo_user_interest_category.user_id = user_info.user_id
LIMIT 10;

CACHE TABLE recall_result AS
SELECT item_id,
       'category_recall:' || interested_category.category AS rec_reason
FROM interested_category
JOIN demo_category_hot_item
  ON demo_category_hot_item.category = interested_category.category
LIMIT 300;

CACHE TABLE dedup_result AS
CALL dedup(recall_result, exposed_item, 'item_id', 'item_id');

RETURN SELECT item_id, rec_reason FROM dedup_result LIMIT 10;
```

Runnable demo definitions are under `sqlrec-demo/src/main/sql/quick_start/`.

## Troubleshooting

### A function keeps waiting for more input

Check that its body ends with a top-level `RETURN` and every top-level statement ends with a semicolon.

### A `CALL` result does not compile

For a dynamic function or a UDF whose result depends on its input, use `LIKE table_name` or `LIKE FUNCTION 'function_name'` to declare the schema.

### A table is not found inside a function call

SQL function arguments must be cached tables in the current execution context. An external table name cannot be passed directly; prepare it with `CACHE TABLE ... AS SELECT ...` first.
