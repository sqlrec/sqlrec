# UDF Basics

A UDF (user-defined function) extends SQL expressions and SQLRec's table-processing capabilities. Built-in functions are ready to use. When they do not cover a business requirement, you can implement a custom UDF in Java.

## Function Types

| Type | Best for | Invocation | Result |
|------|----------|------------|--------|
| Scalar function | Testing, transforming, or calculating individual values | Use in expressions such as `SELECT`, `WHERE`, and `ORDER BY` | One value per row |
| Table function | Deduplicating, diversifying, merging, or sending a whole table to an external service | Use `CALL` | A `CacheTable`, or a side effect only |

For example, `array_contains` is a scalar function and `dedup` is a table function:

```sql
SELECT user_id
FROM user_info
WHERE array_contains(tags, 'vip') = true;

CACHE TABLE dedup_recall AS
CALL dedup(recall_item, exposed_item, 'item_id', 'item_id');
```

Regular SQL expressions can also use Calcite built-in functions such as `ABS`, `UPPER`, `COUNT`, and `ROW_NUMBER`. They require no registration. See [Scalar Functions](./scalar_functions.md#calcite-built-in-functions) for the supported list and compatibility notes.

## Arguments and Results

- Pass a cached table name directly as a table-function argument. Wrap string arguments in single quotes, for example `'item_id'`.
- Scalar functions follow SQL expression type rules. See each function's return-value description for its `NULL` behavior.
- A returned table usually preserves the input fields or appends new fields. See each function's return-value description for the exact schema.
- SQLRec supplies runtime context parameters such as `ExecuteContext`, `ReadonlyContext`, and `DataContext`; do not pass them in SQL.

## Where to Go Next

- For row-by-row calculations in expressions, see [Scalar Functions](./scalar_functions.md).
- For processing or generating a cached table, see [Table Functions](./table_functions.md).
- When built-in functions are not enough, see [Custom UDFs](./custom_udf.md).

::: tip UDFs and SQL functions
Use a Java UDF to reuse Java code or integrate an external capability. A business flow composed of several SQL statements is usually better expressed as a [SQL function](../program_model.md#function-system).
:::
