# 发布和调用 API

完成 SQL 函数后，可以把它发布为 HTTP API。调用方只需按函数声明传入数据，无需了解函数内部的召回、排序等步骤。

## 发布函数

假设已经定义了名为 `recommend` 的 SQL 函数：

```sql
CREATE OR REPLACE API recommend WITH recommend;
```

第一个 `recommend` 是 API 名称，`WITH` 后的 `recommend` 是 SQL 函数名称。发布后通过以下地址调用：

```text
POST /api/v1/recommend
```

如需覆盖同名 API，使用 `OR REPLACE`。SQL 函数的编写方法见[编写推荐流程](./recommendation-flow.md)。

## 发起请求

请求体中的 `data` 按“输入表名 → 行数组”组织。每个由 `DEFINE INPUT TABLE` 声明的输入表都必须提供，并且字段应与声明的结构一致。

例如，函数声明如下：

```sql
DEFINE INPUT TABLE user_info (
  user_id BIGINT,
  country VARCHAR
);
```

调用请求为：

```bash
curl -X POST http://localhost:30001/api/v1/recommend \
  -H 'Content-Type: application/json' \
  -d '{
    "data": {
      "user_info": [
        {"user_id": 1001, "country": "CN"}
      ]
    }
  }'
```

函数有多个输入表时，把它们分别放入 `data`：

```json
{
  "data": {
    "user_info": [{"user_id": 1001, "country": "CN"}],
    "context": [{"page": "home"}]
  }
}
```

## 传递执行参数

`params` 适合传递返回数量、实验分组、动态函数名等请求级配置。值必须是字符串，可在 SQL 中通过 `` `get` `` 或 `` `get_or_default` `` 读取：

```json
{
  "data": {
    "user_info": [{"user_id": 1001, "country": "CN"}]
  },
  "params": {
    "limit_count": "10",
    "experiment": "rank_v2"
  }
}
```

```sql
SELECT CAST(`get_or_default`('limit_count', '50') AS INT);
```

如果需要给本次调用产生的监控指标附加标签，可传入 `metricTags`：

```json
{
  "data": {
    "user_info": [{"user_id": 1001, "country": "CN"}]
  },
  "metricTags": {
    "scene": "homepage"
  }
}
```

## 读取响应

调用成功时，`data` 是函数 `RETURN` 的结果行，`params` 是执行结束时的变量：

```json
{
  "data": [
    {"item_id": 2001, "score": 0.96},
    {"item_id": 2002, "score": 0.91}
  ],
  "params": {
    "limit_count": "10"
  }
}
```

函数没有返回数据，或执行失败时，响应中的 `msg` 会给出说明。路径、请求格式等错误会通过 HTTP 状态码体现，而函数执行错误可能返回在 `msg` 中，因此调用方应同时检查两者，不要只判断 `data` 是否为空。

## 常见问题

### 提示输入表不存在

检查 `data` 中的键是否与 `DEFINE INPUT TABLE` 的表名完全一致。所有声明的输入表都必须出现；没有数据时也应传入空数组。

### 字段转换失败

检查字段名和 JSON 值是否符合函数声明的数据类型，尤其是数值、数组和时间字段。

### API 找不到

确认已经执行 `CREATE API`，请求路径中只有一个 API 名称，并且调用的是 `/api/v1/{api_name}`。

### `/sql/v1` 和 `/api/v1` 有什么区别

`/api/v1/{api_name}` 调用已发布的业务函数，是业务接入的推荐方式。`/sql/v1` 用于直接提交 SQL，是否开放由服务配置决定，不应作为普通业务 API 使用。

API 的完整定义语法见 [SQL 语法参考](../reference/sql.md)。
