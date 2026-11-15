# 编写 SQL 推荐流程

SQL 函数接收输入表，完成召回、过滤、排序和打散，再返回结果。本文先用一个完整示例说明写法，后面介绍按需使用的控制流能力；完整语法见 [SQL 参考](../reference/sql.md)。

先启动 [Docker Demo](../getting-started/docker.md)，进入 [CLI](./clients.md#本地-cli)。下面复用内置表；接入自己的数据时参考[接入数据源](./data-sources.md)。

## 完整示例

下面的精简示例复用 Demo 表，串联召回、曝光去重和返回结果。它不写入曝光、不添加请求信息，函数名使用 `example_rec`，以便与内置 `demo_rec` 分开：

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

## 理解并调用函数

| 语句 | 作用 |
| --- | --- |
| `CREATE SQL FUNCTION` | 开始函数定义 |
| `DEFINE INPUT TABLE` | 声明必需输入字段；`LIKE existing_table` 只复用结构，不读取数据 |
| `CACHE TABLE` | 为当前执行过程的中间结果命名 |
| `RETURN` | 返回表、SELECT 或同步 CALL；顶层 RETURN 结束定义 |

在 CLI 执行完整定义，再在同一会话中准备输入并调用：

```sql
CACHE TABLE quick_start_user AS
SELECT CAST(1000001 AS BIGINT) AS user_id;

CALL example_rec(quick_start_user);
```

结果包含 `item_id`、`rec_reason`。输入表按声明顺序传入，字段结构须兼容。表参数须为缓存表；外部表先通过 `CACHE TABLE ... AS SELECT ...` 读取。

使用文件管理定义时，将完整函数保存为 SQL 目录中的 `function/example_rec.sql` 并重启，见[加载 SQL 文件](../getting-started/docker.md#管理本地-sql-定义)。Demo 也允许执行内存 DDL，但重启后丢失；远程元数据通过 Beeline/JDBC 管理。

缓存结果不在独立 API 请求间共享。曝光记录等共享数据应写入外部 Connector；函数验证后按[发布 API](./api.md)供业务调用。

## 调用函数

SQL 函数和内置表函数均使用 `CALL`，后续需要处理结果时先保存：

```sql
CACHE TABLE recall_result AS CALL recall(user_info);

CACHE TABLE final_result AS
CALL window_diversify(ranked_item, 'category', 3, 1, 10);
```

缓存表参数直接写表名，字符串使用单引号。动态结果结构使用 `LIKE table_name` 或 `LIKE FUNCTION 'function_name'` 声明，签名及输出见[表函数](../reference/udf/table-functions.md)。

## 执行变量

变量适合传递每次请求的小型配置，例如召回函数名、返回数量或实验分组。

```sql
SET 'limit_count' = '100';

SELECT CAST(`get`('limit_count') AS INT);

SELECT CAST(`get`('limit_count', '50') AS INT);
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

## 动态选择函数

执行变量可用于在运行时选择函数：

```sql
CALL GET('rank_fun', 'default_rank')(user_info, recall_result)
LIKE FUNCTION 'default_rank';
```

动态调用时，编译器无法仅根据函数名确定返回字段，因此应通过 `LIKE FUNCTION` 或 `LIKE table_name` 声明结果结构。

## 条件执行

### 普通 IF

```sql
IF (SELECT COUNT(*) > 0 FROM recall_result) THEN (
  CACHE TABLE result_table AS SELECT * FROM recall_result
) ELSE (
  CACHE TABLE result_table AS SELECT * FROM fallback_result
);
```

条件查询必须返回一行一列的布尔值，`NULL` 按 `false` 处理。两个分支同时写缓存表时，必须写入同名且结构兼容的表。

### 超时或异常时回退

```sql
IF TIMEIN (SELECT 100) THEN (
  CACHE TABLE result_table AS CALL online_rank(recall_result)
) ELSE (
  CACHE TABLE result_table AS SELECT * FROM recall_result
);
```

`TIMEIN` 的条件返回毫秒数：

- 大于 0：为 THEN 分支设置超时；THEN 超时或抛出异常时执行 ELSE。
- 小于等于 0：跳过 THEN，直接执行 ELSE。可以将配置中的预算设为 `0` 来关闭主路径、启用降级路径。

`IF TIMEIN` 必须有 ELSE。两个分支必须都是 `CACHE TABLE` 或都是 `RETURN`。更多降级方式见[超时、降级与异常恢复](./exception-recovery.md)。

### 在 IF 中提前返回

```sql
IF (SELECT COUNT(*) = 0 FROM candidates) THEN (
  RETURN SELECT CAST(NULL AS BIGINT) AS item_id LIMIT 0
);

RETURN SELECT item_id FROM candidates;
```

IF 分支中的 `RETURN` 会提前结束当前调用，但不会在编译时结束函数定义，因此仍需要最后的顶层 `RETURN`。

如果 THEN 和 ELSE 都返回，两个结果的列数、列名和类型必须兼容，并且 IF 后必须紧跟一条空 `RETURN;` 结束定义。

## 分区并发调用

对大表执行独立的批量处理时，可以将一个输入缓存表分区后并发调用：

```sql
CACHE TABLE result_table AS
CALL process_batch(input_table)
LIKE input_table
PARTITION BY input_table SIZE 100;
```

- `PARTITION BY` 指定被拆分的输入表。
- `SIZE` 是每个分区的最大行数，也可以使用 `get()` 取值。
- 被分区的表必须是被调函数的输入参数之一。
- 建议使用 `LIKE` 明确合并后的结果结构。

默认情况下，任何分区失败都会使整次调用失败。是否允许保留部分成功结果，请查看[异常恢复中的分区配置](./exception-recovery.md)。

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

## 常见问题

### 函数一直等待后续输入

检查函数体最后是否有顶层 `RETURN`，以及每条顶层语句是否以分号结尾。

### CALL 结果无法编译

对动态函数、返回结构取决于输入的 UDF，使用 `LIKE table_name` 或 `LIKE FUNCTION 'function_name'` 明确返回结构。

### 函数调用后找不到表

SQL 函数参数必须是当前执行上下文中的缓存表，不能把普通外部表名直接当作表参数传入。先用 `CACHE TABLE ... AS SELECT ...` 准备输入。
