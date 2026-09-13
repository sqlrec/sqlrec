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
| 数组判断 | [`array_contains`](#array_contains)、[`array_contains_all`](#array_contains_all)、[`array_contains_any`](#array_contains_any) |
| 向量计算 | [`random_vec`](#random_vec)、[`l2_norm`](#l2_norm)、[`ip`](#ip) |
| 请求标识 | [`uuid`](#uuid) |
| 执行变量 | [`get`](#get)、[`get_or_default`](#get_or_default) |

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
-- 设置变量
SET 'func_name' = 'add_col';

-- 获取变量值，如果不存在则使用默认值
SELECT `get_or_default`('user_id', 'default_user') AS user_id;

-- 动态调用函数：变量存在时使用变量值
CALL `get_or_default`('func_name', 'shuffle')(my_table);

-- 动态调用函数：变量不存在时使用默认值
CALL `get_or_default`('unknown_func', 'shuffle')(my_table);
```

**典型应用场景**：
- 动态函数调用，提供兜底函数
- 配置项获取，提供默认配置
- 参数化 SQL，提供默认参数
