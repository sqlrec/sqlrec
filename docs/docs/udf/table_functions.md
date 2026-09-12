# 表函数

SQLRec 表函数通过 `CALL` 调用，以一张或多张缓存表为输入，并返回新的缓存表；仅产生副作用的函数除外。参数必须按文档顺序传入，字符串参数需要使用单引号。

## 函数列表

| 场景 | 函数 |
|------|------|
| 结果处理 | [`dedup`](#dedup)、[`shuffle`](#shuffle)、[`add_col`](#add_col)、[`truncate_table`](#truncate_table) |
| 多样性 | [`window_diversify`](#window_diversify)、[`dpp_diversity`](#dpp_diversity)、[`rule_diversity`](#rule_diversity) |
| 数据转换与合并 | [`json_to_table`](#json_to_table)、[`tag_to_vec`](#tag_to_vec)、[`weighted_merge`](#weighted_merge) |
| 外部服务 | [`call_service`](#call_service)、[`batch_call_service`](#batch_call_service)、[`call_sqlrec_api`](#call_sqlrec_api)、[`get_growthbook_features`](#get_growthbook_features) |
| 变量与可观测性 | [`get_variables`](#get_variables)、[`set_variables`](#set_variables)、[`feature_coverage_metrics`](#feature_coverage_metrics) |
| 测试辅助 | [`sleep`](#sleep) |

### dedup

去重函数，根据指定列从输入表中排除已存在于去重表中的记录。

**参数**：

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
FROM user_info JOIN exposure_item ON user_id = user_info.id;

-- 从召回结果中排除已曝光物品
CACHE TABLE dedup_recall AS
CALL dedup(recall_item, exposured_item, 'item_id', 'item_id');
```

---

### shuffle

随机打乱函数，将输入表中的记录随机排序。

**参数**：

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

**参数**：

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

---

### add_col

添加列函数，为输入表添加一个新列，所有行的该列值相同。

**参数**：

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

模型服务调用函数，用于调用已部署的模型服务进行推理。详见 [模型文档](../model/basic_concepts.md#call_service)。

SQL 调用支持以下两种形式：

- `CALL call_service(serviceName, input)`：使用行式 JSON 调用服务，返回表包含输入列以及模型输出列。
- `CALL call_service(serviceName, user, item)`：使用 User-Item 模式调用服务，请求体为列式 JSON，返回表保留 Item 表列并追加模型输出列。

User-Item 模式的请求规则：

1. User 表必须恰好一行；Item 表可以包含多行。
2. 仅处理模型定义的输入字段。字段名如果存在于 User 表中（匹配时忽略大小写），该字段归入 User；否则归入 Item。若两张表存在同名字段，优先使用 User 表中的字段；在对应表中找不到的模型字段不会写入请求。两张表中不属于模型输入的额外列也不会写入请求。
3. 每个字段都序列化为一个 JSON 数组。User 字段是单元素数组，因此一份用户数据在一次请求中只发送一遍；Item 字段按 Item 表的行顺序组成数组，不会为每个 Item 重复发送 User 数据。

例如，一行 User 数据和三行 Item 数据会生成：

```json
{
  "user_id": [1001],
  "user_age": [25],
  "item_id": [1, 2, 3],
  "category": ["phone", "tablet", "laptop"]
}
```

当 Item 表为空时不会发送 HTTP 请求，函数直接返回空表，其字段为 Item 表字段加模型输出字段。

HTTP 请求超时默认为连接、读写各 30 秒。详细协议和示例参见[模型文档](../model/basic_concepts.md#call_service)。

---

### batch_call_service

批量模型服务调用函数，用于在 Flink SQL 中批量调用已部署的模型服务进行推理。该函数将多行数据批量发送到远程服务，并将返回结果与原始数据合并输出。

::: warning 注意
此函数只能在 Flink SQL 中使用，不支持 SQLRec 的 CACHE TABLE 语法。
:::

请通过 `LATERAL TABLE` 调用。函数每收集 `batchSize` 行就批量 POST 一个 JSON 数组，最后不足一批的记录也会发送。服务必须返回 JSON 对象，其数组值按输入行顺序映射。

**参数**：

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

---

### dpp_diversity

DPP（Determinantal Point Process）多样性函数，结合物品向量和相关性得分对推荐结果进行打散。

**参数**：

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

**注意事项**：
- `theta` 必须在 [0, 1) 范围内
- `maxLength` 必须为正整数
- 向量列中的所有向量维度必须一致
- 得分或向量为 NULL 的行会被自动跳过

---

### rule_diversity

根据用户定义的窗口规则对推荐结果进行打散重排，适合同时约束类目、品牌等多个属性。

**参数**：

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

**注意事项**：
- 规则表必须包含所有必需字段
- `diversity_column` 必须在目标表中存在
- `diversity_value` 为空时，约束适用于窗口内每个不同的属性值
- 目标表中用于打散的列可以是单值或列表

---

### json_to_table

JSON 转表函数，将 JSON 字符串转换为 CacheTable 表。

**参数**：

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

**注意事项**：
- JSON 字符串不能为空
- 必须是 JSON 对象或 JSON 数组格式
- 数组中的嵌套对象会以 JSON 字符串形式存储为 `VARCHAR`
- 数组类型会自动推断元素类型（`ARRAY<DOUBLE>`、`ARRAY<BOOLEAN>`、`ARRAY<VARCHAR>`）

---

### tag_to_vec

标签转向量函数，将标签列转换为 Multi-Hot 向量表示。

**参数**：

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

**注意事项**：
- 标签列可以是单值（字符串）或列表（`ARRAY<STRING>`）
- 输出列名不能与已有列名重复
- 向量维度取决于所有行中唯一标签的总数

---

### weighted_merge

加权合并函数，按指定权重将多个表合并为一个表，支持按主键去重。

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `primaryKey` | String | 主键列名，用于去重 |
| `weights` | String | 各表权重，逗号分隔，如 `"2,1,1"` |
| `limit` | String | 最大返回记录数 |
| `tables` | CacheTable... | 一个或多个输入表，所有表结构必须相同 |

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

**注意事项**：
- 所有输入表的结构（列名和类型）必须完全相同
- `primaryKey` 为空字符串时不去重；指定主键时按该列的字符串值去重
- 权重数量必须与表的数量一致
- 权重和 limit 必须为正整数
- 主键列必须存在于所有表中
- 在 SQL 函数中启用 `IGNORE_UNION_EXCEPTION=true` 时，如果某个输入只被 UNION 或 `weighted_merge` 消费，该输入失败可按空表处理，其余输入继续合并。通过 `GET()` 动态选择 `weighted_merge` 时不支持此降级行为

---

### call_sqlrec_api

远程 SQLRec API 调用函数，用于调用远端 SQLRec 实例上已发布的 API（即通过 `CREATE API` 暴露的 SQL 函数），并将返回结果转换为 `CacheTable`。适用于跨集群/跨实例调用其他 SQLRec 服务的场景。

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `url` | String | 远端 SQLRec API 地址，例如 `http://host:port/api/v1/function_name` |
| `tables` | CacheTable... | 一个或多个输入表，表名作为远端函数的输入占位符 |

**返回值**：返回包含远端函数执行结果的 `CacheTable`，列名和类型根据响应数据自动推断。

**使用示例**：

```sql
-- 准备输入数据
CACHE TABLE user_input AS
SELECT 1001 AS user_id, 'Alice' AS user_name;

-- 调用远端 SQLRec 实例上已发布的推荐 API
CACHE TABLE remote_rec AS
CALL call_sqlrec_api(
    'http://remote-sqlrec:30001/api/v1/recommend',
    user_input
);

SELECT * FROM remote_rec;
```

**注意事项**：
- `url` 不能为空，且必须指向有效的 SQLRec API 端点
- 至少需要传入一个输入表，且每个输入表必须有表名
- 远端 API 调用失败（返回空数据或错误消息）时会抛出异常
- 输入表的表名需与远端函数定义中的输入表占位符匹配

---

### truncate_table

表截取函数，从输入表中截取指定范围的行记录。

**参数**：

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

---

### set_variables

设置变量函数，从表中读取键值对并设置到执行上下文中。

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
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

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
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

**注意事项**：
- 如果表为空，则跳过该表
- 指标名称不能为空

---

### get_growthbook_features

GrowthBook 特征获取函数，从 GrowthBook 平台获取 A/B 实验特征值，并将实验参数设置为执行上下文变量，同时返回实验追踪数据用于指标计算。

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `apiHost` | String | GrowthBook API 地址 |
| `clientKey` | String | GrowthBook 客户端密钥 |
| `usertable` | CacheTable | 用户表，表中的列将作为用户属性传入 GrowthBook |
| `featureKeys` | String... | 一个或多个特征键名 |

**返回值**：返回包含实验追踪数据的 `CacheTable`，包含以下字段：

| 字段名 | 类型 | 说明 |
|--------|------|------|
| `experiment_id` | VARCHAR | 实验标识 |
| `variation_id` | VARCHAR | 实验分组标识 |
| `user_id` | VARCHAR | 用户标识 |

**使用示例**：

```sql
-- 获取 GrowthBook 特征并设置变量
CACHE TABLE gb_tracking AS
CALL get_growthbook_features(
    'https://cdn.growthbook.io',
    'sdk-abc123',
    user_info,
    'new_recommendation_algo',
    'ui_theme'
);

-- 使用设置的实验变量
SELECT `get`('new_recommendation_algo') AS algo;
```

**注意事项**：
- `apiHost` 和 `clientKey` 不能为空
- `usertable` 不能为空
- 至少需要指定一个 `featureKey`
- GrowthBookClient 初始化失败时会抛出异常
- 同一组 `apiHost` 和 `clientKey` 会复用同一个客户端实例

---

### sleep

睡眠函数，使当前线程休眠指定的毫秒数。主要用于测试、限流或模拟延迟等场景。

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `millisStr` | String | 休眠时长，单位为毫秒，必须是非负整数字符串 |

**返回值**：无返回值。

**使用示例**：

```sql
-- 休眠 1000 毫秒（1 秒）
CALL sleep('1000');

-- 休眠 500 毫秒
CALL sleep('500');
```

**注意事项**：
- `millisStr` 必须是有效的长整数字符串
- 休眠时长必须为非负数
- 该函数仅产生副作用，无返回值

---
