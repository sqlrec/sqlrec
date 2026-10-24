# 标量函数

标量函数在 `SELECT`、`WHERE`、`ORDER BY` 等表达式中使用，对每行输入返回一个值。SQLRec 同时提供 Calcite 标准函数和面向推荐场景的内置标量函数。

## Calcite 内置函数

除 SQLRec 自身提供的 UDF 外，普通 `SELECT` 查询还可以直接使用 SQLRec 所依赖的 Apache Calcite 1.32.0 标准内置函数，无需通过 `CREATE FUNCTION` 注册。

```sql
SELECT ABS(-10), POWER(2, 3), UPPER('sqlrec');

SELECT category,
       COUNT(*) AS item_count,
       ROW_NUMBER() OVER (PARTITION BY category ORDER BY score DESC) AS row_num
FROM candidates
GROUP BY category, score;

SELECT JSON_VALUE('{"item_id": 1001}', 'strict $.item_id');
```

SQLRec 当前已经验证支持的主要函数如下。表格中的函数名是代表性清单，同一函数支持的参数类型和具体语法请参考 Calcite SQL 文档。

| 分类 | 函数 |
|------|------|
| 数值函数 | `ABS`、`ACOS`、`ASIN`、`ATAN`、`ATAN2`、`CBRT`、`CEIL`、`COS`、`COT`、`DEGREES`、`EXP`、`FLOOR`、`LN`、`LOG10`、`MOD`、`PI`、`POWER`、`RADIANS`、`RAND`、`RAND_INTEGER`、`ROUND`、`SIGN`、`SIN`、`SQRT`、`TAN`、`TRUNCATE` |
| 字符串函数 | `ASCII`、`CHAR_LENGTH`、`CHARACTER_LENGTH`、`UPPER`、`LOWER`、`INITCAP`、`POSITION`、`OVERLAY`、`REPLACE`、`SUBSTRING`、`TRIM`，以及字符串连接运算符 `\|\|` |
| 空值和类型转换 | `COALESCE`、`NULLIF`、`CAST`、`CASE` |
| 日期时间函数 | `CURRENT_DATE`、`CURRENT_TIME`、`CURRENT_TIMESTAMP`、`EXTRACT`、`YEAR`、`QUARTER`、`MONTH`、`WEEK`、`DAYOFYEAR`、`DAYOFMONTH`、`DAYOFWEEK`、`HOUR`、`MINUTE`、`SECOND`、`LAST_DAY`、`TIMESTAMPADD`、`TIMESTAMPDIFF` |
| 集合函数 | `ARRAY`、`MAP`、`MULTISET` 构造器，集合下标访问，以及 `CARDINALITY`、`ELEMENT` |
| 聚合函数 | `COUNT`、`SUM`、`AVG`、`MIN`、`MAX`、`EVERY`、`SOME`、`ANY_VALUE`、`SINGLE_VALUE`、`MODE`、`APPROX_COUNT_DISTINCT`、`BIT_AND`、`BIT_OR`、`BIT_XOR`、`COLLECT`、`LISTAGG`、`FUSION`、`INTERSECTION` |
| 统计聚合函数 | `STDDEV`、`STDDEV_POP`、`STDDEV_SAMP`、`VARIANCE`、`VAR_POP`、`VAR_SAMP`、`COVAR_POP`、`COVAR_SAMP`、`REGR_COUNT`、`REGR_SXX`、`REGR_SYY` |
| 分组和窗口函数 | `GROUPING`、`GROUPING_ID`、`GROUP_ID`、`ROW_NUMBER`、`RANK`、`DENSE_RANK`、`NTILE`、`FIRST_VALUE`、`LAST_VALUE`、`NTH_VALUE`、`LEAD`、`LAG` |
| JSON 函数 | `JSON_EXISTS`、`JSON_VALUE`、`JSON_QUERY`、`JSON_OBJECT`、`JSON_ARRAY`、`JSON_OBJECTAGG`、`JSON_ARRAYAGG`、`JSON_TYPE`、`JSON_DEPTH`、`JSON_LENGTH`、`JSON_KEYS`、`JSON_PRETTY`、`JSON_REMOVE`、`JSON_STORAGE_SIZE` |

使用时请注意以下限制：

- `PI` 是无参数函数，语法为 `SELECT PI`，而不是 `PI()`。
- SQLRec 当前只接入 Calcite 标准函数表，没有接入 MySQL、PostgreSQL、Oracle、Spark 或 BigQuery 等 `SqlLibrary` 方言扩展。使用 MySQL 风格词法解析并不表示 `IFNULL`、`DATE_FORMAT`、`NVL` 等方言函数自动可用。
- `CUME_DIST`、`PERCENT_RANK`、`PERCENTILE_CONT` 和 `PERCENTILE_DISC` 当前无法生成可执行的 Enumerable 计划。
- `LOCALTIME`、`LOCALTIMESTAMP`、`USER`、`CURRENT_USER`、`SESSION_USER`、`SYSTEM_USER` 和 `CURRENT_SCHEMA` 依赖的运行上下文尚未完整提供。
- `OCTET_LENGTH` 对普通 `VARCHAR` 不可用；Calcite 的当前实现要求二进制字符串。
- `TUMBLE`、`HOP`、`SESSION` 以及 `MATCH_RECOGNIZE` 专用函数不属于普通标量 UDF，目前没有 SQLRec 端到端支持保证。

相关参考：

- [Apache Calcite SQL 语言和内置函数参考](https://calcite.apache.org/docs/reference.html)

Calcite 官方参考页可能对应比 SQLRec 依赖更新的版本，并且包含方言扩展。判断函数是否可用于 SQLRec 时，应以上述已验证清单和项目实际依赖版本为准。

## SQLRec 内置标量函数

| 用途 | 函数 |
|------|------|
| 数组判断 | [`array_contains`](#array-contains)、[`array_contains_all`](#array-contains-all)、[`array_contains_any`](#array-contains-any) |
| 向量计算 | [`random_vec`](#random-vec)、[`l2_norm`](#l2-norm)、[`ip`](#ip) |
| 请求标识 | [`uuid`](#uuid) |
| 执行变量 | [`get`](#get)、[`get_or_default`](#get-or-default) |
| 远程模型 | [`llm_embed_text`、`llm_complete`、`ark_embed_image`](#远程模型函数) |

### array_contains

数组包含函数，检查数组是否包含指定元素。

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `list` | ARRAY | 输入数组 |
| `element` | ANY | 要检查的元素 |

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

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `list` | ARRAY | 输入数组 |
| `elements` | ARRAY | 要检查的元素列表 |

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

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `list` | ARRAY | 输入数组 |
| `elements` | ARRAY | 要检查的元素列表 |

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

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `dimensionStr` | VARCHAR | 向量维度，必须是正整数字符串 |

**返回值**：返回归一化的随机向量（`ARRAY<DOUBLE>`），向量的 L2 范数为 1。

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

**注意事项**：
- 维度必须是正整数
- 生成的向量已归一化，可直接用于相似度计算

---

### uuid

UUID 生成函数，生成一个随机的 UUID 字符串。

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

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `vector` | ARRAY | 输入向量，必须是数字列表 |

**返回值**：返回归一化后的向量（`ARRAY<DOUBLE>`），使得向量的 L2 范数为 1。

**使用示例**：

```sql
-- 对用户向量进行归一化
CACHE TABLE normalized_user AS
SELECT
    user_id,
    l2_norm(user_embedding) AS normalized_embedding
FROM user_features;
```

---

### ip

内积（Inner Product）计算函数，计算两个向量的内积（点积）。

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `emb1` | ARRAY | 第一个向量，必须是数字列表 |
| `emb2` | ARRAY | 第二个向量，必须是数字列表 |

**返回值**：返回两个向量的内积（`DOUBLE`）。

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

---

### get

变量获取函数，从执行上下文中获取变量的值。常用于在SQL中引用通过 `SET` 语句设置的变量。

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `key` | VARCHAR | 变量名 |

**返回值**：返回变量的值（`VARCHAR`），如果变量不存在则返回 `NULL`。

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

**典型应用场景**：
- 参数化SQL查询
- 动态配置传递
- 跨语句共享变量

---

### get_or_default

变量获取函数（带默认值），从执行上下文中获取变量的值，如果变量不存在则返回指定的默认值。

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `key` | VARCHAR | 变量名 |
| `defaultValue` | VARCHAR | 默认值，当变量不存在时返回 |

**返回值**：返回变量的值（`VARCHAR`），如果变量不存在则返回 `defaultValue`。

**使用示例**：

```sql
SET 'limit_count' = '10';

SELECT CAST(`get_or_default`('limit_count', '50') AS INT) AS limit_count;
```

动态选择被调函数时使用 `GET_OR_DEFAULT(...)`，写法见[推荐流程指南](../../guides/recommendation-flow.md#动态选择函数)。

**典型应用场景**：
- 动态函数调用，提供兜底函数
- 配置项获取，提供默认配置
- 参数化 SQL，提供默认参数


### 远程模型函数

三个函数均接收五个 `VARCHAR` 参数，连接参数直接传入：

| 函数 | 签名 | 返回类型 |
|------|------|----------|
| `llm_embed_text` | `(endpoint, api_key, model, text, options_json)` | `ARRAY<DOUBLE>` |
| `llm_complete` | `(endpoint, api_key, model, prompt, options_json)` | `VARCHAR` |
| `ark_embed_image` | `(endpoint, api_key, model, image, options_json)` | `ARRAY<DOUBLE>` |

`endpoint` 是完整 HTTP(S) 接口地址，不自动拼接路径。`model` 原样传给服务端。非空 `api_key` 通过 `Authorization: Bearer ...` 发送；空字符串用于无需鉴权的兼容服务。建议由执行上下文注入密钥并使用 `` `get`('llm_api_key') `` 读取，避免 SQL 字面量进入 SQL 日志、计划或元数据；UDF 自身的异常不包含密钥、输入和响应正文。

```sql
-- OpenAI 文本向量协议
SELECT llm_embed_text(
    'https://api.openai.com/v1/embeddings',
    `get`('llm_api_key'),
    'text-embedding-3-small',
    title,
    '{"request":{"dimensions":1024},"runtime":{"timeout_ms":10000}}'
) AS embedding
FROM items;

-- OpenAI Chat Completions 协议，prompt 包装为一条 user 消息
SELECT llm_complete(
    'https://api.openai.com/v1/chat/completions',
    `get`('llm_api_key'),
    `get`('llm_model'),
    CONCAT('请总结：', content),
    '{"request":{"max_completion_tokens":128}}'
) AS summary
FROM articles;

-- 方舟多模态协议：单张图片，支持图片 URL 或 data:image/...;base64,...
SELECT ark_embed_image(
    'https://ark.cn-beijing.volces.com/api/v3/embeddings/multimodal',
    `get`('ark_api_key'),
    'doubao-embedding-vision-251215',
    image_url,
    '{"request":{"dimensions":1024},"runtime":{"timeout_ms":10000}}'
) AS embedding
FROM items;
```

`options_json` 使用严格 JSON 对象；无额外参数传 `'{}'`，SQL `NULL` 也使用默认配置。只接受下面列出的字段，其他字段直接报错：

| 位置 | 字段 | 说明 |
|------|------|------|
| `request`，文本 embedding | `dimensions`、`user` | 维度为正整数；是否支持降维由远端模型决定 |
| `request`，文本生成 | `temperature`、`top_p`、`max_completion_tokens`、`reasoning_effort`、`user` | 只发送显式设置的参数；需与远端模型能力一致 |
| `request`，方舟图片 embedding | `dimensions`、`instructions` | 维度为 1024 或 2048；提示词原样发送 |
| `runtime` | `timeout_ms` | 默认 30000，正整数且不超过 300000；覆盖等待并发名额和完整 HTTP 响应 |

本地参数错误、远端 HTTP 失败、超时、响应格式错误和执行中断均抛出异常，不提供失败时返回 `NULL` 的配置。输入内容为 SQL `NULL` 时直接返回 `NULL`，不校验其他参数，也不发送请求。空文本 embedding 报错。

向量以 `float` 编码请求，转换为 Java `List<Double>`，校验非空、有限数值及显式指定的维度，不额外归一化。OpenAI 读取 `data[0].embedding`，方舟读取 `data.embedding`。方舟图片函数仅暴露单图稠密向量，不支持视频、混合内容、稀疏或多向量输出；不能用 OpenAI 文本 embedding 与另一个模型的图片向量直接做跨模态检索。

文本生成固定 `stream=false`、`n=1`、`store=false`，读取 `choices[0].message.content`。拒绝、内容过滤和缺少文本按远端错误处理；因长度上限截断时返回已有文本。首版不支持 Responses API、工具调用或会话历史。

客户端使用共享 JDK HTTP 连接池，连接超时 2 秒，每个类加载器最多 32 个在途请求，禁用重定向，无应用层自动重试或结果缓存。函数声明为非确定性，SQLRec 编译和 Flink 计划生成不会调用远端。运行时按行同步请求，查询重复执行或任务恢复可能再次调用服务，不提供远端请求的恰好一次保证。

SQLRec 内置注册上述函数。Flink 使用 HiveCatalog/HiveModule，并部署包含 UDF 的 JAR 后注册：

```sql
CREATE FUNCTION llm_embed_text AS 'com.sqlrec.udf.scalar.LlmEmbedTextFunction';
CREATE FUNCTION llm_complete AS 'com.sqlrec.udf.scalar.LlmCompleteFunction';
CREATE FUNCTION ark_embed_image AS 'com.sqlrec.udf.scalar.ArkEmbedImageFunction';
```

协议参考：[OpenAI Embeddings](https://developers.openai.com/api/reference/resources/embeddings/methods/create)、[OpenAI Chat Completions](https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create)、[方舟多模态向量化](https://console.volcengine.com/ark/region:cn-beijing/docs/multimodal-vectorization-api)。
