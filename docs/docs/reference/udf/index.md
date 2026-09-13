# UDF 基础概念

UDF（用户定义函数）用于补充 SQL 表达式和 SQLRec 表处理能力。内置函数开箱即用；当内置能力不能满足业务需求时，也可以用 Java 扩展自定义 UDF。

## 函数类型

| 类型 | 适合解决的问题 | 调用方式 | 结果 |
|------|----------------|----------|------|
| 标量函数 | 对单个值做判断、转换或计算 | 在 `SELECT`、`WHERE`、`ORDER BY` 等表达式中调用 | 每行返回一个值 |
| 表函数 | 对整张表去重、打散、合并或调用外部服务 | 使用 `CALL` 调用 | 返回一张 `CacheTable`，或只产生副作用 |

例如，`array_contains` 是标量函数，`dedup` 是表函数：

```sql
SELECT user_id
FROM user_info
WHERE array_contains(tags, 'vip') = true;

CACHE TABLE dedup_recall AS
CALL dedup(recall_item, exposed_item, 'item_id', 'item_id');
```

普通 SQL 表达式还可以直接使用 Calcite 内置函数，例如 `ABS`、`UPPER`、`COUNT` 和 `ROW_NUMBER`。这些函数不需要注册，完整清单及兼容性说明见[标量函数](./scalar-functions.md#calcite-内置函数)。

## 参数与返回结果

- 表函数中的表参数直接传缓存表名，字符串参数使用单引号，例如 `'item_id'`。
- 标量函数遵循 SQL 表达式的类型规则；遇到 `NULL` 时，以各函数的返回值说明为准。
- 返回表的字段通常继承输入表，或在输入字段后追加新字段。具体结构以各函数的“返回值”说明为准。
- `ExecuteContext`、`ReadonlyContext` 和 `DataContext` 等运行上下文由 SQLRec 自动提供，不需要在 SQL 中传入。

## 如何选择

- 需要在表达式里逐行计算：查看[标量函数](./scalar-functions.md)。
- 需要处理或生成整张缓存表：查看[表函数](./table-functions.md)。
- 内置函数无法满足需求：查看[自定义 UDF](../../development/custom-udf.md)。

::: tip UDF 与 SQL 函数
Java UDF 适合复用 Java 代码或接入外部能力；由多条 SQL 组合而成的业务流程，通常更适合使用 [SQL 函数](../../guides/recommendation-flow.md#一个最小函数)。
:::
