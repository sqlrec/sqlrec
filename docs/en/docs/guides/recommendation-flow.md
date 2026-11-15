# Writing a Recommendation Flow

A SQL function receives input tables, runs recall, filtering, ranking, and diversification, then returns a result. This guide uses one complete example, followed by optional control-flow features. See [the SQL reference](../reference/sql.md) for full syntax.

Start the [Docker demo](../getting-started/docker.md), then open [a CLI](./clients.md#local-cli). The example reuses its built-in tables. For your own data sources, see [Connect Data Sources](./data-sources.md).

## Complete Example

This shortened example reuses the demo tables for recall and exposure deduplication. It does not write exposure records or add request metadata. The name `example_rec` keeps it separate from the built-in `demo_rec`.

```sql
CREATE OR REPLACE SQL FUNCTION example_rec;

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

## Read and Call the Function

| Statement | Purpose |
| --- | --- |
| `CREATE SQL FUNCTION` | Begin a function definition |
| `DEFINE INPUT TABLE` | Declare required input fields; `LIKE existing_table` reuses a schema, not its data |
| `CACHE TABLE` | Name an intermediate result for the current execution |
| `RETURN` | Return a table, SELECT, or synchronous CALL; a top-level RETURN ends the definition |

Run the complete definition in the CLI, then prepare input and call it in the same session:

```sql
CACHE TABLE quick_start_user AS
SELECT CAST(1000001 AS BIGINT) AS user_id;

CALL example_rec(quick_start_user);
```

The result contains `item_id` and `rec_reason`. Input tables are passed in declaration order with compatible fields. Parameters must be cached tables; first query an external table with `CACHE TABLE ... AS SELECT ...`.

To manage this definition as a file, save it under `function/example_rec.sql` in the complete SQL directory and restart; see [Load SQL Files](../getting-started/docker.md#managing-local-sql-definitions). The Demo also permits in-memory DDL, which disappears on restart. Remote metadata uses Beeline/JDBC.

Cached results are not shared across independent API requests. Persist exposure records and other shared data through an external Connector. Publish the function using [the API guide](./api.md).

## Calling Functions

SQL functions and built-in table functions use `CALL`. Capture the result when later statements need it:

```sql
CACHE TABLE recall_result AS CALL recall(user_info);

CACHE TABLE final_result AS
CALL window_diversify(ranked_item, 'category', 3, 1, 10);
```

Pass cached tables by name and strings in single quotes. Use `LIKE table_name` or `LIKE FUNCTION 'function_name'` when the result schema is dynamic. See [Table Functions](../reference/udf/table-functions.md) for signatures and output schemas.

## Execution Variables

Variables carry small request-specific settings such as a recall function, result limit, or experiment group:

```sql
SET 'limit_count' = '100';

SELECT CAST(`get`('limit_count') AS INT);

SELECT CAST(`get`('limit_count', '50') AS INT);
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

## Select a Function Dynamically

To select a function at runtime, use an execution variable:

```sql
CALL GET('rank_fun', 'default_rank')(user_info, recall_result)
LIKE FUNCTION 'default_rank';
```

Because a dynamic name does not reveal the return fields during compilation, declare the result schema with `LIKE FUNCTION` or `LIKE table_name`.

## Conditional Execution

Use a normal `IF` for a business condition:

```sql
IF (SELECT COUNT(*) > 0 FROM recall_result) THEN (
  CACHE TABLE result_table AS SELECT * FROM recall_result
) ELSE (
  CACHE TABLE result_table AS SELECT * FROM fallback_result
);
```

The condition query must return one Boolean value; `NULL` is treated as `false`. If both branches cache a table, they must write the same name with compatible schemas.

Use `IF TIMEIN` when the primary path should fall back after a timeout or exception:

```sql
IF TIMEIN (SELECT 100) THEN (
  CACHE TABLE result_table AS CALL online_rank(recall_result)
) ELSE (
  CACHE TABLE result_table AS SELECT * FROM recall_result
);
```

The `TIMEIN` query returns milliseconds:

- greater than zero: set a timeout for THEN and run ELSE after a timeout or ordinary exception;
- zero or less: skip THEN and execute ELSE directly. Set the configured budget to `0` to disable the primary path and select the fallback.

`IF TIMEIN` requires ELSE. Both branches must use `CACHE TABLE` or both must use `RETURN`. See [Timeouts, Fallbacks, and Error Recovery](./exception-recovery.md) for other recovery options.

An IF branch may return early:

```sql
IF (SELECT COUNT(*) = 0 FROM candidates) THEN (
  RETURN SELECT CAST(NULL AS BIGINT) AS item_id LIMIT 0
);

RETURN SELECT item_id FROM candidates;
```

A branch-level `RETURN` ends that call, but it does not terminate the function definition at compile time, so the final top-level `RETURN` is still required. If both THEN and ELSE return, their schemas must be compatible and the IF must be followed immediately by an empty `RETURN;`.

## Partitioned Calls

For independent batch processing, split one input table and invoke the same function concurrently:

```sql
CACHE TABLE result_table AS
CALL process_batch(input_table)
LIKE input_table
PARTITION BY input_table SIZE 100;
```

- `PARTITION BY` selects the table to split.
- `SIZE` is the maximum rows per partition and may also come from `get()`.
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

## Troubleshooting

### A function keeps waiting for more input

Check that its body ends with a top-level `RETURN` and every top-level statement ends with a semicolon.

### A `CALL` result does not compile

For a dynamic function or a UDF whose result depends on its input, use `LIKE table_name` or `LIKE FUNCTION 'function_name'` to declare the schema.

### A table is not found inside a function call

SQL function arguments must be cached tables in the current execution context. An external table name cannot be passed directly; prepare it with `CACHE TABLE ... AS SELECT ...` first.
