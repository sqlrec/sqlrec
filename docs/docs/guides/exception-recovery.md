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

`IF TIMEIN` 不只是超时回退，也能处理 THEN 分支中的异常，包括 `Error` 和 `InterruptedException`。

```sql
IF TIMEIN (SELECT 100 FROM config_table) THEN (
    CACHE TABLE result_table AS CALL remote_rank_service(candidate_items)
) ELSE (
    CACHE TABLE result_table AS SELECT * FROM local_rank_fallback
);
```

上面的 `100` 是毫秒数：

- THEN 在 100ms 内成功：使用 THEN 的 `result_table`；
- THEN 超时：取消 THEN，执行 ELSE；
- THEN 抛出异常或错误（包括中断）：执行 ELSE；
- 父请求已经取消：不再执行 ELSE，直接结束请求。

缓存数据完整计算后才会发布。缓存发布与上下文取消使用同一个 schema 锁：取消完成后，迟到的 THEN 不能再覆盖 ELSE 已写入的缓存。SQL 子函数和分区使用各自独立的临时 schema，不共享发布锁。

普通 `IF` 和 `IF TIMEIN` 整体受外层 `NODE_EXEC_TIMEOUT` 控制，条件、THEN、ELSE 的执行不会分别重置节点预算。节点超时取消当前 IF 后，不再执行 ELSE。

条件值也可以来自配置表或变量：

```sql
IF TIMEIN (
    SELECT CAST(`get_or_default`('rank_timeout_ms', '80') AS BIGINT)
) THEN (
    CACHE TABLE result_table AS CALL rank_service(items)
) ELSE (
    CACHE TABLE result_table AS SELECT * FROM items
);
```

`TIMEIN` 表示 THEN 分支可用的时间预算。当预算小于等于 0 时，跳过 THEN，直接执行 ELSE；可以将预算配置为 `0` 来主动降级：

```sql
IF TIMEIN (SELECT 0) THEN (
    CACHE TABLE result_table AS CALL rank_service(items)
) ELSE (
    CACHE TABLE result_table AS SELECT * FROM items
);
```

上例不会调用 `rank_service`，直接使用 ELSE 中的 `items` 作为结果。负数预算也采用相同规则，因此动态计算的剩余时间耗尽后会直接降级。

迁移已有配置时，需要将原来表示“不限时”的 `0` 或负数改为业务可接受的较大正数预算。较大预算仍有时间限制，并受外层节点超时和请求取消约束。`NODE_EXEC_TIMEOUT <= 0` 的“不设置节点超时”含义保持不变。

直接降级记录 `sqlrec_if_cache_direct_fallback_total`，实际等待超时记录 `sqlrec_if_cache_timeout_total`，THEN 普通异常回退记录 `sqlrec_if_cache_exception_fallback_total`。这些计数记录分支选择次数；ELSE 自身失败仍向上传播。请求已取消时，不选择 ELSE，也不增加降级计数。

如果两个分支都返回结果，schema 必须兼容：

```sql
IF TIMEIN (SELECT 50) THEN (
    RETURN SELECT * FROM service_result
) ELSE (
    RETURN SELECT * FROM cached_result
);
-- 结束函数定义
RETURN;
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

当某个缓存分支最终只用于 UNION 合并时，异常、错误或节点超时会把这一支视为空结果，其他分支继续合并。`Error`、`InterruptedException` 及其包装异常也适用，恢复时会清除当前线程已处理的中断标记，使后续节点可以继续执行。适合召回、特征补充等“部分数据优于完全失败”的场景。

失败节点的子上下文会被取消，父上下文可继续执行。缓存节点超时后，迟到的计算结果会被丢弃，不会覆盖降级的空表。

关闭该行为：

```sql
SET 'IGNORE_UNION_EXCEPTION' = 'false';
```

以下情况不会降级：

- 失败结果被直接返回，而不是先进入 UNION；
- 同一输入还被其他非 UNION 路径使用；
- 请求已经被取消。

开启该开关并不意味着所有 `CALL` 都会自动变成“失败返回空表”，只有最终用于合并的分支才适用。

限制按节点判定，不会因为函数中存在 IF 就关闭整段函数的降级：

- `IF` / `IF TIMEIN` 不作为 UNION 合并节点，也不自动降级为空表；即使其结果随后进入 UNION，这个限制仍然有效。
- 同一表存在多个写入节点时，这些写入节点不自动降级，避免错误处理表覆盖和旧数据保留。

子函数内部的限制不会向调用方传播。无论静态还是动态 `CALL`，调用方都根据该调用结果的用途决定能否降级：结果仅用于可降级的 UNION 分支时，可以把整个调用的失败视为空结果，即使错误来自子函数内的 IF 或重复写入节点。

在当前函数内，受保护节点及其上游来源本身不自动降级，但不会因此关闭子函数内部符合条件的 UNION 降级。子函数成功返回的结果可继续用于外层 IF 条件或其他运算；子函数抛出的错误则由当前函数根据调用结果的用途决定如何处理。同一函数中无关的 UNION 分支仍可降级。`IF TIMEIN` 自身的 ELSE 回退仍然有效。

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

- KV 主键 Join：批量查询失败后仅使用本次请求键的未过期缓存，不逐键重试后端；未命中或未启用缓存的键按无匹配处理，LEFT JOIN 保留左行并补右侧 NULL；
- KV 非主键 Join：跳过查询失败的 key，继续处理其他 key；
- Vector Join：失败的左侧行没有匹配结果，其他左侧行继续执行。

需要严格失败时：

```sql
SET 'IGNORE_JOIN_QUERY_EXCEPTION' = 'false';
```

该开关只处理外部查询失败，不会把整个 SQL 查询、输入表或 schema 错误转换成空结果。KV Join 的取消、中断和执行超时异常仍向上抛出；缓存回退仅适用于主键 Join。

## 分区调用保留成功结果：`IGNORE_PARTITION_EXCEPTION`

默认情况下，任意一个分区失败都会立即结束整个分区调用的等待，并取消该调用的子上下文；已经运行的任务协作退出：

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

每个分区使用独立的子上下文，失败时只取消该分区及其后台子任务。等待线程被中断时，会保留中断标记、取消整个分区调用并抛出异常，严格模式和部分成功模式均适用。

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
- 请求上下文已取消。

请求上下文取消始终会终止执行，不会触发 UNION 降级或 `IF TIMEIN` 的 ELSE 回退。线程中断本身不等于请求上下文已取消。

并行执行时，未被降级处理的节点失败会立即结束等待并抛出异常。其他节点沿用上下文取消检查，已经运行的任务协作退出。

恢复策略会改变“失败范围”，不会保证结果顺序。多路合并或分区合并后，如果业务需要稳定顺序，请显式添加 `ORDER BY`。
