# 编写 SQL 推荐流程

SQLRec 把一段由多条 SQL 组成的业务流程定义为 **SQL 函数**。一个典型推荐函数会接收用户信息，依次完成召回、去重、排序和打散，最后返回推荐结果。

本文面向编写业务 SQL 的用户。语法的完整定义请查看 [SQL 语法参考](../reference/sql.md)，执行引擎和调度机制请查看[架构设计](../reference/architecture.md)。

## 一个最小函数

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

这段 SQL 包含四个要点：

1. `CREATE SQL FUNCTION` 开始函数定义。
2. `DEFINE INPUT TABLE` 声明调用方需要传入的表及其字段。
3. `CACHE TABLE` 保存中间结果。
4. 顶层 `RETURN` 返回结果并结束函数定义。

多语句函数中的每条顶层语句都应以分号结尾。

## 输入表

输入表是函数的表类型参数。调用函数时，按声明顺序传入结构兼容的缓存表。

### 显式声明字段

```sql
DEFINE INPUT TABLE user_info (
  user_id BIGINT,
  country VARCHAR,
  tags ARRAY<VARCHAR>
);
```

### 复用现有表结构

```sql
DEFINE INPUT TABLE user_info LIKE user_profile;
```

`LIKE` 只复用字段结构，不会把 `user_profile` 的数据带入函数。

## 缓存表

`CACHE TABLE` 用于保存当前请求中的中间结果：

```sql
CACHE TABLE interested_category AS
SELECT category
FROM user_info
JOIN user_interest_category
  ON user_interest_category.user_id = user_info.user_id
ORDER BY score DESC
LIMIT 10;
```

缓存表可以被后续 SQL 查询，也可以作为参数传给 SQL 函数或 Java 表函数：

```sql
CACHE TABLE dedup_result AS
CALL dedup(recall_result, exposed_item, 'item_id', 'item_id');
```

缓存表属于当前执行过程。不要用它在不同 API 请求之间保存数据；需要持久化时，应将结果 `INSERT` 到 Redis、Kafka、JDBC 等 Connector 表。

## 调用函数

### 调用 SQL 函数

```sql
CALL recall(user_info);
```

如果需要继续处理返回数据，先把结果保存为缓存表：

```sql
CACHE TABLE recall_result AS
CALL recall(user_info);
```

### 调用 Java 表函数

SQLRec 内置的 `dedup`、`weighted_merge`、`window_diversify` 等表函数也使用 `CALL`：

```sql
CACHE TABLE final_result AS
CALL window_diversify(ranked_item, 'category', '3', '1', '10');
```

表参数直接写缓存表名，字符串参数使用单引号。完整的内置函数清单见 [表函数](../reference/udf/table-functions.md)。

### 动态选择函数

执行变量可用于在运行时选择函数：

```sql
CALL `get_or_default`('rank_fun', 'default_rank')(user_info, recall_result)
LIKE FUNCTION 'default_rank';
```

动态调用时，编译器无法仅根据函数名确定返回字段，因此应通过 `LIKE FUNCTION` 或 `LIKE table_name` 声明结果结构。

## 返回结果

SQL 函数必须以一条顶层 `RETURN` 结束定义。可以返回缓存表、查询结果或同步函数调用：

```sql
RETURN result_table;

RETURN SELECT item_id, score FROM result_table;

RETURN CALL post_process(result_table);

RETURN;
```

`RETURN;` 表示正常结束但不返回数据。`RETURN CALL ... ASYNC` 不受支持，因为异步调用无法同步提供函数结果。

## 条件执行

### 普通 IF

```sql
IF (SELECT COUNT(*) > 0 FROM recall_result) THEN (
  CACHE TABLE result AS SELECT * FROM recall_result
) ELSE (
  CACHE TABLE result AS SELECT * FROM fallback_result
);
```

条件查询必须返回一行一列的布尔值，`NULL` 按 `false` 处理。两个分支同时写缓存表时，必须写入同名且结构兼容的表。

### 超时或异常时回退

```sql
IF TIMEIN (SELECT 100) THEN (
  CACHE TABLE result AS CALL online_rank(recall_result)
) ELSE (
  CACHE TABLE result AS SELECT * FROM recall_result
);
```

`TIMEIN` 的条件返回毫秒数：

- 大于 0：为 THEN 分支设置超时；THEN 超时或抛出异常时执行 ELSE。
- 小于等于 0：不设置超时，但 THEN 抛出普通异常时仍会执行 ELSE。

`IF TIMEIN` 必须有 ELSE。两个分支必须都是 `CACHE TABLE` 或都是 `RETURN`。更多降级方式见[超时、降级与异常恢复](./exception-recovery.md)。

### 在 IF 中提前返回

```sql
IF (SELECT COUNT(*) = 0 FROM candidates) THEN (
  RETURN SELECT CAST(NULL AS BIGINT) AS item_id WHERE FALSE
);

RETURN SELECT item_id FROM candidates;
```

IF 分支中的 `RETURN` 会提前结束当前调用，但不会在编译时结束函数定义，因此仍需要最后的顶层 `RETURN`。

如果 THEN 和 ELSE 都返回，两个结果的列数、列名和类型必须兼容，并且 IF 后必须紧跟一条空 `RETURN;` 结束定义。

## 执行变量

变量适合传递每次请求的小型配置，例如召回函数名、返回数量或实验分组。

```sql
SET 'limit_count' = '100';

SELECT CAST(`get`('limit_count') AS INT);

SELECT CAST(`get_or_default`('limit_count', '50') AS INT);
```

API 请求体中的 `params` 也会进入执行上下文：

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

`get` 是 SQL 关键字，作为标量函数调用时应写成 `` `get`('name') ``。

## 分区并发调用

对大表执行独立的批量处理时，可以将一个输入缓存表分区后并发调用：

```sql
CACHE TABLE result AS
CALL process_batch(input_table)
LIKE input_table
PARTITION BY input_table SIZE 100;
```

- `PARTITION BY` 指定被拆分的输入表。
- `SIZE` 是每个分区的最大行数，也可以使用 `get()` 或 `get_or_default()` 取值。
- 被分区的表必须是被调函数的输入参数之一。
- 建议使用 `LIKE` 明确合并后的结果结构。

默认情况下，任何分区失败都会使整次调用失败。是否允许保留部分成功结果，请查看 `IGNORE_PARTITION_EXCEPTION` 配置。

## 异步调用

不需要等待完成的旁路任务可以使用 `ASYNC`，例如写入推荐日志：

```sql
CALL save_rec_log(final_result) ASYNC;
```

异步调用提交后立即返回，后续 SQL 不应依赖它已完成。`ASYNC` 不能用在 `CACHE TABLE ... AS CALL` 或 `RETURN CALL` 中。

## Join 使用建议

SQLRec 会根据表类型选择合适的执行方式。编写在线查询时，建议遵循以下原则：

- 使用小型缓存表驱动 Redis、JDBC、MongoDB 等外部表查询。
- Join 条件尽量包含外部表的主键等值条件。
- 不要假设所有 Connector 都支持全表扫描或所有过滤表达式。
- 向量召回通过用户向量表与 Milvus 表 Join，再按距离函数排序和 `LIMIT` 触发。

```sql
CACHE TABLE vector_recall AS
SELECT item_embedding.id AS item_id
FROM user_embedding
JOIN item_embedding ON 1 = 1
ORDER BY ip(user_embedding.embedding, item_embedding.embedding) DESC
LIMIT 300;
```

Connector 支持的查询和写入能力见[接入数据源](./data-sources.md)。

函数验证完成后，可继续阅读[发布和调用 API](./api.md)，将 SQL 函数提供给业务方调用。

## 完整示例

下面的精简示例串联了召回、曝光去重和返回结果：

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

可直接运行的 Demo 定义位于 `sqlrec-demo/src/main/sql/quick_start/`。

## 常见问题

### 函数一直等待后续输入

检查函数体最后是否有顶层 `RETURN`，以及每条顶层语句是否以分号结尾。

### CALL 结果无法编译

对动态函数、返回结构取决于输入的 UDF，使用 `LIKE table_name` 或 `LIKE FUNCTION 'function_name'` 明确返回结构。

### 函数调用后找不到表

SQL 函数参数必须是当前执行上下文中的缓存表，不能把普通外部表名直接当作表参数传入。先用 `CACHE TABLE ... AS SELECT ...` 准备输入。
