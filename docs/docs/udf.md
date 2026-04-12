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
public CacheTable eval(CacheTable input, CacheTable dedupTable, String col1, String col2)
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
public CacheTable eval(CacheTable input)
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
public CacheTable eval(
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
public CacheTable eval(CacheTable input, String colName, String value)
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

模型服务调用函数，用于调用已部署的模型服务进行推理。详见 [模型文档](models.md#内置模型调用-udf)。

---

### call_service_with_qv

带 Query-Value 模式的模型服务调用函数。详见 [模型文档](models.md#内置模型调用-udf)。

## 标量函数（Scalar Function）

### uuid

UUID 生成函数，生成一个随机的 UUID 字符串。

**函数签名**：

```java
public String eval()
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
public Object eval(Object vector)
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
public Object eval(Object emb1, Object emb2)
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
public static String eval(DataContext context, String key)
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

## 自定义 UDF

可以参考 [编程模型](program_model.md#udf) 文档了解如何开发自定义 UDF。
