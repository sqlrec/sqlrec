# 异常处理与恢复

推荐系统链路中，某个远程服务、召回源或分区偶尔失败并不一定意味着整条请求都应该失败。SQLRec 提供几种不同的恢复方式，选择取决于你的业务意图：

| 目标 | 推荐写法 |
| --- | --- |
| 主方案失败时使用备用方案 | `IF TIMEIN ... ELSE ...` |
| 多路召回中某一路失败仍继续合并 | `IGNORE_UNION_EXCEPTION` |
| Join 的某个外部查询失败时继续处理其他数据 | `IGNORE_JOIN_QUERY_EXCEPTION` |
| 分区调用中丢弃失败分区，保留成功分区 | `IGNORE_PARTITION_EXCEPTION` |
| 失败必须阻断请求 | 不开启对应的忽略开关，直接让异常抛出 |

## 主方案失败时回退：`IF TIMEIN`

`IF TIMEIN` 不只是超时回退，也能处理 THEN 分支中的普通异常。

```sql
IF TIMEIN (SELECT 100 FROM config_table) THEN (
    CACHE TABLE result AS CALL remote_rank_service(candidate_items)
) ELSE (
    CACHE TABLE result AS SELECT * FROM local_rank_fallback
);
```

上面的 `100` 是毫秒数：

- THEN 在 100ms 内成功：使用 THEN 的 `result`；
- THEN 超时：取消 THEN，执行 ELSE；
- THEN 抛出普通异常：执行 ELSE；
- 父请求已经取消：不再执行 ELSE，直接结束请求。

条件值也可以来自配置表或变量：

```sql
IF TIMEIN (
    SELECT CAST(get_or_default('rank_timeout_ms', '80') AS BIGINT)
) THEN (
    CACHE TABLE result AS CALL rank_service(items)
) ELSE (
    CACHE TABLE result AS SELECT * FROM items
);
```

当超时时间小于等于 0 时，不设置超时，THEN 可以一直执行；如果 THEN 抛出普通异常，仍会执行 ELSE：

```sql
IF TIMEIN (SELECT 0) THEN (
    CACHE TABLE result AS CALL rank_service(items)
) ELSE (
    CACHE TABLE result AS SELECT * FROM items
);
```

上例不会因为 `0` 触发超时，但 `rank_service` 抛出异常时仍会回退到 ELSE；只有 THEN 成功完成后才提交结果。

如果两个分支都返回结果，schema 必须兼容：

```sql
IF TIMEIN (SELECT 50) THEN (
    RETURN SELECT * FROM service_result
) ELSE (
    RETURN SELECT * FROM cached_result
);
```

只有成功完成的 THEN 才会提交返回值；超时或异常不会提交 THEN 的半成品结果。

> 普通 `IF` 只负责条件选择，不会因为 THEN 抛异常而自动执行 ELSE。需要异常/超时回退时使用 `IF TIMEIN`。

## 多路召回继续合并：`IGNORE_UNION_EXCEPTION`

多路召回通常适合“缺一路仍可返回其他路”的策略：

```sql
SET 'IGNORE_UNION_EXCEPTION' = 'true';

CACHE TABLE recall_a AS CALL itemcf_recall(user_profile);
CACHE TABLE recall_b AS CALL hot_recall(user_profile);
CACHE TABLE all_recall AS
    SELECT * FROM recall_a
    UNION ALL
    SELECT * FROM recall_b;
```

当某个缓存分支最终只用于 UNION 合并时，普通异常或节点超时会把这一支视为空结果，其他分支继续合并。适合召回、特征补充等“部分数据优于完全失败”的场景。

关闭该行为：

```sql
SET 'IGNORE_UNION_EXCEPTION' = 'false';
```

以下情况不会降级：

- 失败结果被直接返回，而不是先进入 UNION；
- 同一输入还被其他非 UNION 路径使用；
- 请求已经被取消；
- 线程中断或严重错误；
- 函数名通过 `GET()` 动态选择，系统无法判断它是否是可合并函数。

开启该开关并不意味着所有 `CALL` 都会自动变成“失败返回空表”，只有最终用于合并的分支才适用。

## Join 查询失败继续处理：`IGNORE_JOIN_QUERY_EXCEPTION`

外部 KV 或向量查询可能只对某一个 key/候选行失败。可以让其他数据继续处理：

```sql
SET 'IGNORE_JOIN_QUERY_EXCEPTION' = 'true';

CACHE TABLE enriched AS
SELECT u.user_id, i.item_id, i.category
FROM users u
LEFT JOIN item_features i
  ON u.item_id = i.item_id;
```

行为是：

- KV Join：失败的 key 被跳过；LEFT JOIN 保留左行并补右侧 NULL；
- Vector Join：失败的左侧行没有匹配结果，其他左侧行继续执行。

需要严格失败时：

```sql
SET 'IGNORE_JOIN_QUERY_EXCEPTION' = 'false';
```

该开关只处理单次外部查询失败，不会把整个 SQL 查询、输入表或 schema 错误转换成空结果。

## 分区调用保留成功结果：`IGNORE_PARTITION_EXCEPTION`

默认情况下，任意一个分区失败都会使整个分区调用失败：

```sql
CACHE TABLE ranked AS
CALL rank_partition(items)
PARTITION BY items SIZE 100;
```

如果业务允许丢弃失败分区，可以开启：

```sql
SET 'IGNORE_PARTITION_EXCEPTION' = 'true';

CACHE TABLE ranked AS
CALL rank_partition(items)
PARTITION BY items SIZE get_or_default('partition_size', '100');
```

开启后：

- 成功分区的结果继续合并；
- 失败分区的结果被丢弃；
- 至少一个分区成功时返回部分结果；
- 全部分区失败时仍然报错；
- 请求取消、超时或严重错误不会被当成普通分区失败忽略。

关闭或恢复默认行为：

```sql
SET 'IGNORE_PARTITION_EXCEPTION' = 'false';
```

`SIZE` 支持字面量和运行时变量：

```sql
-- 固定大小，旧语法继续支持
PARTITION BY items SIZE 100

-- 变量不存在时使用 100
PARTITION BY items SIZE get_or_default('partition_size', '100')

-- 变量不存在时抛异常
PARTITION BY items SIZE get('partition_size')
```

如果所有输入行都在空表中，正数 SIZE 不会创建分区，结果为空。`ASYNC` 调用是后台提交，调用方不会同步收到分区失败；需要判断部分成功或全部失败时，应使用同步 `CALL`。

## 配置优先级

通过执行上下文读取的开关遵循：

```text
本次执行参数 > 环境变量 > 代码默认值
```

例如：

```sql
SET 'IGNORE_PARTITION_EXCEPTION' = 'true';
```

只影响当前执行上下文。若本次执行没有设置该参数，系统会读取同名环境变量；环境变量也不存在时，使用默认值。

| 参数 | 默认值 | 适用场景 |
| --- | --- | --- |
| `IGNORE_UNION_EXCEPTION` | `true` | UNION 合并前的缓存分支降级 |
| `IGNORE_JOIN_QUERY_EXCEPTION` | `true` | KV/Vector Join 单次外部查询失败 |
| `IGNORE_PARTITION_EXCEPTION` | `false` | 分区调用保留成功分区 |
| `NODE_EXEC_TIMEOUT` | `0` | 可超时节点的执行时间（毫秒） |

## 哪些错误应该继续抛出

以下错误通常表示 SQL 或请求本身不可继续，不适合降级：

- `ASSERT` 失败；
- 表、函数或字段不存在；
- 输入 schema 不兼容；
- SQL 函数没有执行 `RETURN`；
- 请求上下文已取消；
- 线程中断或严重错误。

恢复策略会改变“失败范围”，不会保证结果顺序。多路合并或分区合并后，如果业务需要稳定顺序，请显式添加 `ORDER BY`。
