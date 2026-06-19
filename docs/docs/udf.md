# 内置 UDF

本文档介绍 SQLRec 内置的用户定义函数（UDF），包括表函数（Table Function）和标量函数（Scalar Function）。

## 概述

SQLRec 提供了丰富的内置 UDF，用于推荐系统开发中的常见操作，如去重、打散、向量计算等。

**UDF 分类**：

| 类型 | 说明 | 返回值 |
|------|------|--------|
| Table Function | 表函数，接收表作为输入，返回表 | `CacheTable` |
| Scalar Function | 标量函数，接收标量值，返回标量值 | 单个值 |

## 表函数（Table Function）

### dedup

去重函数，根据指定列从输入表中排除已存在于去重表中的记录。

**函数签名**：

```java
public CacheTable evaluate(CacheTable input, CacheTable dedupTable, String col1, String col2)
```

**参数说明**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `input` | CacheTable | 输入表 |
| `dedupTable` | CacheTable | 去重表，包含需要排除的值 |
| `col1` | String | 输入表中用于去重的列名 |
| `col2` | String | 去重表中用于匹配的列名 |

**返回值**：返回去重后的 `CacheTable`，结构与输入表相同。

**使用示例**：

```sql
-- 获取用户已曝光的物品
CACHE TABLE exposured_item AS
SELECT item_id
FROM user_info JOIN user_exposure_item ON user_id = user_info.id;

-- 从召回结果中排除已曝光物品
CACHE TABLE dedup_recall AS
CALL dedup(recall_item, exposured_item, 'item_id', 'item_id');
```

**工作原理**：
1. 从 `dedupTable` 的 `col2` 列收集所有值
2. 遍历 `input` 表，排除 `col1` 列值存在于去重集合中的记录
3. 返回去重后的结果表

---

### shuffle

随机打乱函数，将输入表中的记录随机排序。

**函数签名**：

```java
public CacheTable evaluate(CacheTable input)
```

**参数说明**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `input` | CacheTable | 输入表 |

**返回值**：返回随机排序后的 `CacheTable`，结构和数据与输入表相同。

**使用示例**：

```sql
-- 随机打乱推荐结果
CACHE TABLE shuffled_result AS
CALL shuffle(recall_item);

-- 取打乱后的前 N 个
CACHE TABLE random_top_n AS
SELECT * FROM shuffled_result LIMIT 10;
```

---

### window_diversify

窗口打散函数，确保相邻的记录不会过于集中在某个类目，实现推荐结果的多样性。

**函数签名**：

```java
public CacheTable evaluate(
    CacheTable input,
    String categoryColumnName,
    String windowSize,
    String maxCategoryNumInWindow,
    String maxReturnRecord
)
```

**参数说明**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `input` | CacheTable | 输入表 |
| `categoryColumnName` | String | 类目列名，用于打散的依据 |
| `windowSize` | String | 滑动窗口大小 |
| `maxCategoryNumInWindow` | String | 窗口内每个类目最多出现的次数 |
| `maxReturnRecord` | String | 最大返回记录数 |

**返回值**：返回打散后的 `CacheTable`，结构与输入表相同。

**使用示例**：

```sql
-- 类目打散：窗口大小为 3，每个类目在窗口内最多出现 1 次，返回 10 条
CACHE TABLE diversify_result AS
CALL window_diversify(rec_item, 'category1', '3', '1', '10');
```

**工作原理**：
1. 维护一个滑动窗口，统计窗口内各类目的出现次数
2. 遍历输入记录，优先选择窗口内未超限的类目
3. 当窗口滑动时，移除最旧记录的类目计数
4. 确保推荐结果的多样性，避免同类目物品连续出现

---

### add_col

添加列函数，为输入表添加一个新列，所有行的该列值相同。

**函数签名**：

```java
public CacheTable evaluate(CacheTable input, String colName, String value)
```

**参数说明**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `input` | CacheTable | 输入表 |
| `colName` | String | 新列名 |
| `value` | String | 新列的值（所有行相同） |

**返回值**：返回添加新列后的 `CacheTable`。

**使用示例**：

```sql
-- 添加一个来源标识列
CACHE TABLE result_with_source AS
CALL add_col(recall_item, 'source', 'daily_rec');

-- 添加时间戳列
CACHE TABLE result_with_time AS
CALL add_col(recall_item, 'rec_time', '2024-01-01');
```

**注意事项**：
- 新列名不能与已有列名重复
- 新列类型为 `VARCHAR`

---

### call_service

模型服务调用函数，用于调用已部署的模型服务进行推理。详见 [模型文档](./model/basic_concepts.md#call_service)。

---

### batch_call_service

批量模型服务调用函数，用于在 Flink SQL 中批量调用已部署的模型服务进行推理。该函数将多行数据批量发送到远程服务，并将返回结果与原始数据合并输出。

::: warning 注意
此函数只能在 Flink SQL 中使用，不支持 SQLRec 的 CACHE TABLE 语法。
:::

**函数签名**：

```java
@FunctionHint(output = @DataTypeHint("ROW&lt;" +
        "long_map MAP&lt;STRING, BIGINT&gt;, " +
        "double_map MAP&lt;STRING, DOUBLE&gt;, " +
        "string_map MAP&lt;STRING, STRING&gt;, " +
        "long_array_map MAP&lt;STRING, ARRAY&lt;BIGINT&gt;&gt;, " +
        "double_array_map MAP&lt;STRING, ARRAY&lt;DOUBLE&gt;&gt;, " +
        "string_array_map MAP&lt;STRING, ARRAY&lt;STRING&gt;&gt;" +
        "&gt;"))
public void eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object... args)
```

**参数说明**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `serviceUrl` | String | 模型服务的 URL 地址 |
| `batchSize` | Integer | 批量大小，每次请求发送的行数 |
| `fieldName-value pairs` | Object... | 字段名-值对，用于指定要发送到服务的字段，必须成对出现 |

**返回值**：返回一个 ROW 类型，包含以下字段：

| 字段名 | 类型 | 说明 |
|--------|------|------|
| `long_map` | MAP&lt;STRING, BIGINT&gt; | 长整型字段的 Map |
| `double_map` | MAP&lt;STRING, DOUBLE&gt; | 双精度浮点型字段的 Map |
| `string_map` | MAP&lt;STRING, STRING&gt; | 字符串型字段的 Map |
| `long_array_map` | MAP&lt;STRING, ARRAY&lt;BIGINT&gt;&gt; | 长整型数组字段的 Map |
| `double_array_map` | MAP&lt;STRING, ARRAY&lt;DOUBLE&gt;&gt; | 双精度浮点型数组字段的 Map |
| `string_array_map` | MAP&lt;STRING, ARRAY&lt;STRING&gt;&gt; | 字符串型数组字段的 Map |

**使用示例**：

```sql
-- 创建临时函数
CREATE TEMPORARY FUNCTION batch_call_service AS 'com.sqlrec.udf.udtf.BatchCallServiceUDTF';

-- 调用模型服务生成物品向量
INSERT INTO item_embedding
SELECT 
    r.long_map['movie_id'] AS id,
    r.string_map['title'] AS title,
    r.string_array_map['genres'] AS genres,
    r.double_array_map['item_tower_emb'] AS embedding
FROM ml_movies, LATERAL TABLE(batch_call_service(
    'http://test-recall-service-item.sqlrec.svc.cluster.local:80/predict', 
    128, 
    'movie_id', movie_id, 
    'title', title, 
    'genres', genres
)) AS r
WHERE dt = '2024-01-01';
```

**工作原理**：
1. 函数接收多行数据，将字段名-值对缓存到缓冲区
2. 当缓冲区大小达到 `batchSize` 时，将数据批量发送到模型服务
3. 模型服务接收 JSON 数组格式的请求，返回包含预测结果的 JSON 对象
4. 函数将预测结果与原始数据合并，按类型分类存储到不同的 Map 中
5. 每行数据输出一个 ROW，可通过 Map 访问原始字段和预测结果

**请求格式**：

发送到模型服务的 JSON 格式为对象数组：

```json
[
  {"movie_id": 1, "title": "Toy Story", "genres": ["Animation", "Comedy"]},
  {"movie_id": 2, "title": "Jumanji", "genres": ["Adventure", "Children"]}
]
```

**响应格式**：

模型服务应返回一个 JSON 对象，其中每个字段的值是一个数组，数组长度与请求数据行数相同：

```json
{
  "item_tower_emb": [[0.1, 0.2, ...], [0.3, 0.4, ...]],
  "score": [0.95, 0.87]
}
```

**注意事项**：
- 此函数只能在 Flink SQL 中使用，需要使用 `LATERAL TABLE` 语法
- `batchSize` 建议根据模型服务的性能和网络延迟进行调整，通常设置为 64-256
- 模型服务需要支持 POST 请求，接收 JSON 数组并返回 JSON 对象
- 返回结果中的数组字段会自动按行索引与输入数据对应
- 在 `close()` 方法中会处理缓冲区中剩余的数据

---

### dpp_diversity

DPP（Determinantal Point Process）多样性函数，基于行列式点过程实现推荐结果的多样性打散。采用快速贪心 MAP 推理算法（参考 Hulu NIPS 2018 论文），在保证相关性的同时提升推荐结果的多样性。

**函数签名**：

```java
public CacheTable evaluate(
    CacheTable input,
    String embeddingColumnName,
    String scoreColumnName,
    String theta,
    String maxLength
)
```

**参数说明**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `input` | CacheTable | 输入表 |
| `embeddingColumnName` | String | 向量列名，用于计算物品间的相似度 |
| `scoreColumnName` | String | 相关性得分列名，用于衡量物品质量 |
| `theta` | String | 相关性-多样性权衡参数，取值范围 [0, 1)，值越接近 1 越偏向相关性，越接近 0 越偏向多样性 |
| `maxLength` | String | 最大返回记录数 |

**返回值**：返回多样性选择后的 `CacheTable`，结构与输入表相同。

**使用示例**：

```sql
-- DPP 多样性打散：theta=0.5 平衡相关性与多样性，返回 20 条
CACHE TABLE dpp_result AS
CALL dpp_diversity(rec_item, 'item_embedding', 'score', '0.5', '20');
```

**工作原理**：
1. 从输入表中提取相关性得分和向量
2. 对负得分裁剪为极小正值，然后进行指数变换：`score = exp(alpha * r)`，其中 `alpha = theta / (2 * (1 - theta))`
3. 对向量进行 L2 归一化
4. 构建核矩阵 `L = Diag(scores) * S * Diag(scores)`，其中 `S[i][j] = (1 + dot(emb[i], emb[j])) / 2`
5. 运行贪心 DPP MAP 推理算法，选择多样性子集

**注意事项**：
- `theta` 必须在 [0, 1) 范围内
- `maxLength` 必须为正整数
- 向量列中的所有向量维度必须一致
- 得分或向量为 NULL 的行会被自动跳过

---

### rule_diversity

基于规则的多样性函数，通过贪心算法根据用户定义的规则对推荐结果进行打散重排，支持灵活的多样性约束配置。

**函数签名**：

```java
public CacheTable evaluate(
    CacheTable targetTable,
    CacheTable ruleTable,
    String maxReturn
)
```

**参数说明**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `targetTable` | CacheTable | 待打散的目标表 |
| `ruleTable` | CacheTable | 规则表，定义多样性约束规则 |
| `maxReturn` | String | 最大返回记录数 |

**规则表字段说明**：

| 字段名 | 类型 | 说明 |
|--------|------|------|
| `window_size` | Integer | 窗口大小 |
| `window_start` | Integer | 窗口起始位置（从 1 开始） |
| `window_num` | Integer | 滑动窗口数量（1 表示不滑动） |
| `diversity_column` | String | 目标表中用于打散的列名 |
| `diversity_value` | String | 匹配值（为空时约束适用于每个不同的值） |
| `op` | String | 比较运算符（`>`、`=`、`<`） |
| `diversity_num` | Integer | 约束阈值 |
| `weight` | Double | 规则权重，权重越高优先级越高 |

**返回值**：返回打散后的 `CacheTable`，结构与目标表相同。

**使用示例**：

```sql
-- 创建规则表
CACHE TABLE diversity_rules AS
SELECT
    5 AS window_size,
    1 AS window_start,
    1 AS window_num,
    'category' AS diversity_column,
    '' AS diversity_value,
    '<' AS op,
    2 AS diversity_num,
    1.0 AS weight
UNION ALL
SELECT
    3, 1, 1, 'brand', 'Nike', '=', 1, 2.0;

-- 基于规则打散，返回 20 条
CACHE TABLE rule_diversify_result AS
CALL rule_diversity(rec_item, diversity_rules, '20');
```

**工作原理**：
1. 解析规则表，为每条规则构建滑动窗口
2. 对每个输出位置，贪心选择违反约束惩罚最小的未分配物品
3. 如果没有违反约束，则按原始排名顺序选择
4. 权重越高的规则，违反时的惩罚越大

**注意事项**：
- 规则表必须包含所有必需字段
- `diversity_column` 必须在目标表中存在
- `diversity_value` 为空时，约束适用于窗口内每个不同的属性值
- 目标表中用于打散的列可以是单值或列表

---

### json_to_table

JSON 转表函数，将 JSON 字符串转换为 CacheTable 表。

**函数签名**：

```java
public CacheTable evaluate(String jsonString)
```

**参数说明**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `jsonString` | String | JSON 字符串，支持 JSON 对象或 JSON 数组 |

**返回值**：返回转换后的 `CacheTable`，列名和类型根据 JSON 内容自动推断。

**使用示例**：

```sql
-- 将 JSON 数组转换为表
CACHE TABLE json_result AS
CALL json_to_table('[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]');

-- 将单个 JSON 对象转换为表
CACHE TABLE single_obj AS
CALL json_to_table('{"id": 1, "name": "Alice", "score": 95.5}');
```

**工作原理**：
1. 解析 JSON 字符串，支持 JSON 对象和 JSON 数组
2. 收集所有键作为列名，保持插入顺序
3. 根据第一个非空值自动推断列类型（`BOOLEAN`、`DOUBLE`、`VARCHAR`、`ARRAY<...>`）
4. 将每个 JSON 对象转换为一行记录

**注意事项**：
- JSON 字符串不能为空
- 必须是 JSON 对象或 JSON 数组格式
- 数组中的嵌套对象会以 JSON 字符串形式存储为 `VARCHAR`
- 数组类型会自动推断元素类型（`ARRAY<DOUBLE>`、`ARRAY<BOOLEAN>`、`ARRAY<VARCHAR>`）

---

### tag_to_vec

标签转向量函数，将标签列转换为 Multi-Hot 向量表示。

**函数签名**：

```java
public CacheTable evaluate(CacheTable input, String tagColName, String outputColName)
```

**参数说明**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `input` | CacheTable | 输入表 |
| `tagColName` | String | 标签列名，可以是单值或列表 |
| `outputColName` | String | 输出向量列名 |

**返回值**：返回添加了向量列的 `CacheTable`，新列类型为 `ARRAY<FLOAT>`。

**使用示例**：

```sql
-- 将用户标签转换为 Multi-Hot 向量
CACHE TABLE user_with_vec AS
CALL tag_to_vec(user_info, 'tags', 'tag_vector');

-- 将物品类目转换为向量
CACHE TABLE item_with_vec AS
CALL tag_to_vec(item_info, 'categories', 'category_vector');
```

**工作原理**：
1. 遍历所有行，收集标签列中的所有唯一标签，构建标签到索引的映射
2. 为每行生成 Multi-Hot 向量，向量维度等于唯一标签数
3. 如果行中包含某个标签，对应位置为 1.0，否则为 0.0
4. 将向量列追加到原始表中

**注意事项**：
- 标签列可以是单值（字符串）或列表（`ARRAY<STRING>`）
- 输出列名不能与已有列名重复
- 向量维度取决于所有行中唯一标签的总数

---

### weighted_merge

加权合并函数，按指定权重将多个表合并为一个表，支持按主键去重。

**函数签名**：

```java
public CacheTable evaluate(String primaryKey, String weights, String limit, CacheTable... tables)
```

**参数说明**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `primaryKey` | String | 主键列名，用于去重 |
| `weights` | String | 各表权重，逗号分隔，如 `"2,1,1"` |
| `limit` | String | 最大返回记录数 |
| `tables` | CacheTable... | 两个或多个输入表，所有表结构必须相同 |

**返回值**：返回合并后的 `CacheTable`，结构与输入表相同。

**使用示例**：

```sql
-- 按权重 2:1:1 合并三个召回通道，返回 100 条
CACHE TABLE merged_recall AS
CALL weighted_merge('item_id', '2,1,1', '100', recall_channel_a, recall_channel_b, recall_channel_c);

-- 按权重 3:2 合并两个召回通道，返回 50 条
CACHE TABLE merged_result AS
CALL weighted_merge('item_id', '3,2', '50', recall_a, recall_b);
```

**工作原理**：
1. 每轮按表的顺序，从每个表中取权重数量的记录
2. 按主键去重，已出现的记录不再重复添加
3. 重复轮次直到达到 `limit` 或所有表遍历完毕
4. 权重越大的表，每轮贡献的记录越多

**注意事项**：
- 所有输入表的结构（列名和类型）必须完全相同
- 权重数量必须与表的数量一致
- 权重和 limit 必须为正整数
- 主键列必须存在于所有表中

---

### call_service_with_qv

带 Query-Value 模式的模型服务调用函数。详见 [模型文档](./model/basic_concepts.md#call_service_with_qv)。

---

### truncate_table

表截取函数，从输入表中截取指定范围的行记录。

**函数签名**：

```java
public CacheTable evaluate(CacheTable input, String start, String end)
```

**参数说明**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `input` | CacheTable | 输入表 |
| `start` | String | 起始行索引（从 0 开始，包含） |
| `end` | String | 结束行索引（不包含） |

**返回值**：返回截取后的 `CacheTable`，结构与输入表相同。

**使用示例**：

```sql
-- 获取第 10 到 20 条记录
CACHE TABLE partial_result AS
CALL truncate_table(recall_item, '10', '20');

-- 获取前 100 条记录
CACHE TABLE top_100 AS
CALL truncate_table(recall_item, '0', '100');
```

**注意事项**：
- `start` 和 `end` 必须为有效的整数字符串
- `start` 和 `end` 必须为非负数
- `start` 必须小于或等于 `end`
- 截取范围为左闭右开区间 `[start, end)`

---

### get_variables

获取变量函数，从执行上下文中获取所有变量，返回一个包含变量键值对的表。

**函数签名**：

```java
public CacheTable evaluate(ExecuteContext context)
```

**参数说明**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `context` | ExecuteContext | 执行上下文 |

**返回值**：返回一个 2 列的 `CacheTable`，列名为 `key` 和 `value`，类型均为 `VARCHAR`。

**使用示例**：

```sql
-- 设置一些变量
SET 'user_id' = '12345';
SET 'limit' = '100';

-- 获取所有变量
CACHE TABLE all_vars AS
CALL get_variables();

-- 查看变量
SELECT * FROM all_vars;
```

**工作原理**：
1. 从执行上下文中获取所有变量
2. 将每个变量的键值对转换为一行记录
3. 返回包含所有变量的表

---

### set_variables

设置变量函数，从表中读取键值对并设置到执行上下文中。

**函数签名**：

```java
public CacheTable evaluate(ExecuteContext context, CacheTable input)
```

**参数说明**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `context` | ExecuteContext | 执行上下文 |
| `input` | CacheTable | 输入表，必须恰好有 2 列，且均为字符串类型 |

**返回值**：返回输入表本身。

**使用示例**：

```sql
-- 创建变量表
CACHE TABLE var_table AS
SELECT 'user_id' AS key, '12345' AS value
UNION ALL
SELECT 'limit', '100';

-- 设置变量
CALL set_variables(var_table);

-- 使用设置的变量
SELECT `get`('user_id') AS user_id;
```

**注意事项**：
- 输入表必须恰好有 2 列
- 两列都必须是字符串类型（VARCHAR 或 CHAR）
- 第一列为变量名，第二列为变量值
- 如果变量值为 NULL，则会删除该变量

---

### feature_coverage_metrics

特征覆盖率打点函数，计算表中各字段的特征覆盖率并上报指标。

**函数签名**：

```java
public Void evaluate(ExecuteContext context, String metricsName, CacheTable... tables)
```

**参数说明**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `context` | ExecuteContext | 执行上下文 |
| `metricsName` | String | 指标名称 |
| `tables` | CacheTable... | 一个或多个输入表 |

**返回值**：无返回值。

**使用示例**：

```sql
-- 计算并上报特征覆盖率
CALL feature_coverage_metrics('feature.coverage', user_features, item_features);

-- 仅计算单个表的覆盖率
CALL feature_coverage_metrics('user.feature.coverage', user_info);
```

**工作原理**：
1. 遍历每个表的每个字段
2. 统计每个字段的非空值数量（`null`、空 `Collection`、空 `Map` 视为缺失）
3. 计算覆盖率 = 非空值数量 / 总行数
4. 使用 summary 类型上报指标，tags 包含 `table`（表名）和 `field`（字段名）

**注意事项**：
- 如果表为空，则跳过该表
- 指标名称不能为空

## 标量函数（Scalar Function）

### array_contains

数组包含函数，检查数组是否包含指定元素。

**函数签名**：

```java
public static Boolean evaluate(List<?> list, Object element)
```

**参数说明**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `list` | List<?> | 输入数组 |
| `element` | Object | 要检查的元素 |

**返回值**：如果数组包含该元素返回 `true`，否则返回 `false`；如果任一参数为 `null` 则返回 `null`。

**使用示例**：

```sql
-- 检查用户标签是否包含 'vip'
SELECT
    user_id,
    array_contains(tags, 'vip') AS is_vip
FROM user_info;

-- 筛选包含特定标签的用户
SELECT *
FROM user_info
WHERE array_contains(tags, 'active') = true;
```

---

### array_contains_all

数组全包含函数，检查数组是否包含所有指定元素。

**函数签名**：

```java
public static Boolean evaluate(List<?> list, List<?> elements)
```

**参数说明**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `list` | List<?> | 输入数组 |
| `elements` | List<?> | 要检查的元素列表 |

**返回值**：如果数组包含所有指定元素返回 `true`，否则返回 `false`；如果任一参数为 `null` 则返回 `null`。

**使用示例**：

```sql
-- 检查用户是否同时拥有多个标签
SELECT
    user_id,
    array_contains_all(tags, ARRAY['vip', 'active']) AS is_vip_active
FROM user_info;

-- 筛选同时满足多个条件的用户
SELECT *
FROM user_info
WHERE array_contains_all(tags, ARRAY['premium', 'verified']) = true;
```

---

### array_contains_any

数组任一包含函数，检查数组是否包含指定元素中的任意一个。

**函数签名**：

```java
public static Boolean evaluate(List<?> list, List<?> elements)
```

**参数说明**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `list` | List<?> | 输入数组 |
| `elements` | List<?> | 要检查的元素列表 |

**返回值**：如果数组包含任一指定元素返回 `true`，否则返回 `false`；如果任一参数为 `null` 则返回 `null`。

**使用示例**：

```sql
-- 检查用户是否拥有任意一个 VIP 等级
SELECT
    user_id,
    array_contains_any(levels, ARRAY['gold', 'platinum', 'diamond']) AS is_high_level
FROM user_info;

-- 筛选拥有任意指定标签的用户
SELECT *
FROM user_info
WHERE array_contains_any(tags, ARRAY['new_user', 'trial']) = true;
```

---

### random_vec

随机向量生成函数，生成指定维度的归一化随机向量。

**函数签名**：

```java
public List<Double> evaluate(String dimensionStr)
```

**参数说明**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `dimensionStr` | String | 向量维度，必须是正整数字符串 |

**返回值**：返回归一化的随机向量（`List<Double>`），向量的 L2 范数为 1。

**使用示例**：

```sql
-- 生成 64 维随机向量
SELECT
    user_id,
    random_vec('64') AS random_embedding
FROM user_info;

-- 为冷启动用户生成随机向量
CACHE TABLE cold_start_users AS
SELECT
    user_id,
    random_vec('128') AS user_embedding
FROM new_users;
```

**工作原理**：
1. 解析维度参数为整数
2. 生成指定维度的随机向量
3. 对向量进行 L2 归一化，使范数为 1

**注意事项**：
- 维度必须是正整数
- 生成的向量已归一化，可直接用于相似度计算

---

### uuid

UUID 生成函数，生成一个随机的 UUID 字符串。

**函数签名**：

```java
public String evaluate()
```

**返回值**：返回一个随机 UUID 字符串，格式如 `ee073e63-b74a-4c7e-8fea-60459729099c`。

**使用示例**：

```sql
-- 生成请求 ID
CACHE TABLE request_meta AS
SELECT
    user_id,
    CAST(CURRENT_TIMESTAMP AS BIGINT) AS req_time,
    uuid() AS req_id
FROM user_info;
```

---

### l2_norm

L2 归一化函数，对向量进行 L2 归一化处理。

**函数签名**：

```java
public Object evaluate(Object vector)
```

**参数说明**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `vector` | Object | 输入向量，必须是数字列表 |

**返回值**：返回归一化后的向量（`List<Double>`），使得向量的 L2 范数为 1。

**使用示例**：

```sql
-- 对用户向量进行归一化
CACHE TABLE normalized_user AS
SELECT
    user_id,
    l2_norm(user_embedding) AS normalized_embedding
FROM user_features;
```

**工作原理**：
1. 计算向量的 L2 范数：`norm = sqrt(sum(x_i^2))`
2. 对每个元素除以范数：`x_i' = x_i / norm`
3. 归一化后的向量常用于余弦相似度计算

---

### ip

内积（Inner Product）计算函数，计算两个向量的内积（点积）。

**函数签名**：

```java
public Object evaluate(Object emb1, Object emb2)
```

**参数说明**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `emb1` | Object | 第一个向量，必须是数字列表 |
| `emb2` | Object | 第二个向量，必须是数字列表 |

**返回值**：返回两个向量的内积（`Float`）。

**使用示例**：

```sql
-- 计算用户向量和物品向量的内积
SELECT
    user_id,
    item_id,
    ip(user_embedding, item_embedding) AS similarity
FROM user_item_pairs;

-- 向量召回：按内积排序
CACHE TABLE vector_recall AS
SELECT item_embedding.id AS item_id
FROM user_embedding JOIN item_embedding ON 1=1
ORDER BY ip(user_embedding.embedding, item_embedding.embedding) DESC
LIMIT 300;
```

**工作原理**：
- 内积计算：`ip = sum(emb1[i] * emb2[i])`
- 如果向量已归一化，内积等于余弦相似度
- 常用于向量检索和相似度计算

---

### get

变量获取函数，从执行上下文中获取变量的值。常用于在SQL中引用通过 `SET` 语句设置的变量。

**函数签名**：

```java
public static String evaluate(DataContext context, String key)
```

**参数说明**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `key` | String | 变量名 |

**返回值**：返回变量的值（`String`），如果变量不存在则返回 `NULL`。

::: warning 注意
由于 `get` 是 SQL 关键字，使用时需要用反引号包裹函数名，写作 `` `get` ``。
:::

**使用示例**：

```sql
-- 设置变量
SET 'user_id' = '12345';

-- 获取变量值
SELECT `get`('user_id') AS user_id;

-- 在表达式中使用
SELECT `get`('user_id') || '_suffix' AS user_id_with_suffix;

-- 类型转换
SELECT CAST(`get`('limit_count') AS INT) AS limit_count;

-- 从表中获取变量名并使用
CACHE TABLE var_names AS SELECT 'user_id' AS var_name;
SELECT `get`(var_name) AS var_value FROM var_names;
```

**工作原理**：
1. 函数接收一个变量名作为参数
2. 从执行上下文（`ExecuteContext`）中查找对应的变量值
3. 返回变量值，如果变量不存在则返回 `NULL`

**典型应用场景**：
- 参数化SQL查询
- 动态配置传递
- 跨语句共享变量

---

### get_or_default

变量获取函数（带默认值），从执行上下文中获取变量的值，如果变量不存在则返回指定的默认值。

**函数签名**：

```java
public static String evaluate(DataContext context, String key, String defaultValue)
```

**参数说明**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `key` | String | 变量名 |
| `defaultValue` | String | 默认值，当变量不存在时返回 |

**返回值**：返回变量的值（`String`），如果变量不存在则返回 `defaultValue`。

**使用示例**：

```sql
-- 设置变量
SET 'func_name' = 'add_col';

-- 获取变量值，如果不存在则使用默认值
SELECT `get_or_default`('user_id', 'default_user') AS user_id;

-- 动态调用函数：变量存在时使用变量值
CALL `get_or_default`('func_name', 'shuffle')(my_table);

-- 动态调用函数：变量不存在时使用默认值
CALL `get_or_default`('unknown_func', 'shuffle')(my_table);
```

**工作原理**：
1. 函数接收变量名和默认值两个参数
2. 从执行上下文（`ExecuteContext`）中查找对应的变量值
3. 如果变量存在，返回变量值；如果变量不存在，返回默认值

**典型应用场景**：
- 动态函数调用，提供兜底函数
- 配置项获取，提供默认配置
- 参数化 SQL，提供默认参数

## 自定义 UDF

可以参考 [编程模型](program_model.md#udf) 文档了解如何开发自定义 UDF。
