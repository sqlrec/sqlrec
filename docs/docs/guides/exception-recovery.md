# 超时、降级与异常恢复

根据业务能否接受备用或部分结果，选择恢复方式：

| 目标 | 方式 | 默认值 |
| --- | --- | --- |
| 主方案失败时使用备用方案 | `IF TIMEIN ... ELSE ...` | 显式编写 |
| 多路召回缺一路仍继续合并 | `IGNORE_UNION_EXCEPTION` | `true` |
| 外部 Join 查询失败时继续处理 | `IGNORE_JOIN_QUERY_EXCEPTION` | `true` |
| 分区调用保留成功分区 | `IGNORE_PARTITION_EXCEPTION` | `false` |

忽略开关会改变返回结果的完整性。必需的数据源应关闭对应开关，让错误阻断请求。

## 主方案失败时回退：`IF TIMEIN`

```sql
IF TIMEIN (SELECT 100) THEN (
  CACHE TABLE result_table AS CALL online_rank(items)
) ELSE (
  CACHE TABLE result_table AS SELECT * FROM items
);
```

`100` 是 THEN 分支的时间预算，单位为毫秒：

- 预算内成功时使用 THEN 结果。
- THEN 超时或执行失败时使用 ELSE，包括执行过程中的错误和中断。
- 预算小于等于 0 时，跳过 THEN，直接使用 ELSE。
- 请求已取消或外层节点已超时时，不再执行 ELSE；ELSE 自身失败仍会报错。

两个分支必须都写入同名、结构兼容的缓存表，或都使用 `RETURN`。超时或失败的 THEN 结果不会覆盖备用结果。分支语法见 [IF 参考](../reference/sql.md#if)。

动态预算可从请求变量读取：

```sql
IF TIMEIN (SELECT CAST(`get`('rank_timeout_ms', '80') AS BIGINT)) THEN (
  CACHE TABLE result_table AS CALL online_rank(items)
) ELSE (
  CACHE TABLE result_table AS SELECT * FROM items
);
```

普通 `IF` 只选择条件分支，THEN 失败不会自动执行 ELSE。`TIMEIN <= 0` 表示直接降级；`NODE_EXEC_TIMEOUT <= 0` 则表示不设置节点超时。

## 多路召回继续合并：`IGNORE_UNION_EXCEPTION`

```sql
SET 'IGNORE_UNION_EXCEPTION' = 'true';

CACHE TABLE recall_a AS CALL itemcf_recall(user_info);
CACHE TABLE recall_b AS CALL hot_recall(user_info);
CACHE TABLE all_recall AS
SELECT * FROM recall_a
UNION ALL
SELECT * FROM recall_b;
```

仅用于 UNION 合并的缓存分支失败时，该分支按空结果处理，其余分支继续。静态调用的 `weighted_merge` 也支持此行为；动态选择 `weighted_merge` 不支持。

以下情况不能依赖自动空表降级：

- 结果被直接返回，或还用于其他非合并操作。
- 当前函数的 IF 节点，或同一缓存表的多个写入节点，以及它们依赖的上游节点。
- 请求已经取消。

子函数的失败由调用方按结果用途处理：即使错误来自子函数内部的 IF，只要整个调用结果仅用于可降级的合并分支，仍可按空表处理。同一函数中无关的合并分支不受影响。

必需的召回源可设置 `SET 'IGNORE_UNION_EXCEPTION' = 'false';`，或通过 `IF TIMEIN` 明确编写备用方案。

## Join 查询失败继续处理：`IGNORE_JOIN_QUERY_EXCEPTION`

```sql
SET 'IGNORE_JOIN_QUERY_EXCEPTION' = 'true';

CACHE TABLE enriched AS
SELECT u.user_id, i.item_id, i.category
FROM users u
LEFT JOIN item_features i ON u.item_id = i.item_id;
```

| 查询类型 | 失败后的结果 |
| --- | --- |
| KV 主键 Join | 使用本次查询键的未过期本地缓存；缺少缓存的键按无匹配处理，不逐键重试后端 |
| KV 非主键 Join | 跳过失败的 key，继续其他查询 |
| Vector Join | 失败的左侧行没有匹配结果，其他行继续 |

LEFT JOIN 无匹配时保留左行、右侧补 NULL；INNER JOIN 不返回该匹配。需要严格失败时，设置 `IGNORE_JOIN_QUERY_EXCEPTION=false`。

该开关只处理外部查询失败，不处理 SQL、表结构或输入错误。KV Join 的取消、中断和执行超时仍向上传播。

## 分区调用保留成功结果：`IGNORE_PARTITION_EXCEPTION`

```sql
SET 'IGNORE_PARTITION_EXCEPTION' = 'true';

CACHE TABLE ranked AS
CALL rank_partition(items)
PARTITION BY items SIZE GET('partition_size', 100);
```

默认任一分区失败会使调用失败。开启后合并成功分区，丢弃失败分区；至少一个成功才返回部分结果，全部失败仍报错。请求取消、超时和严重错误不会作为普通分区失败忽略。

空输入返回空结果。需要同步判断成功与否时不要使用 `ASYNC`；分区大小和调用规则见 [CALL](../reference/sql.md#call)。

## 配置与排障

这些开关的取值优先级为“本次执行参数 > 同名环境变量 > 默认值”。SQL `SET` 和 API 的 `params` 可设置本次执行参数。

`NODE_EXEC_TIMEOUT` 为节点执行预算，单位为毫秒，默认 `0`（不限制）。整个 IF 共用这一预算，条件、THEN、ELSE 不会分别重置计时。

编译期的表、函数、字段或类型错误应先修正 SQL。运行期是否恢复取决于所在分支；请求取消始终终止执行。需要结果顺序时，在合并后显式使用 `ORDER BY`。

通过[可观测性](../operations/observability.md#常用指标)查看节点失败、合并降级和 TIMEIN 分支指标，确认部分结果是否符合预期。
