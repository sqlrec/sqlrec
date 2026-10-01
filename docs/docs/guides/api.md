# 发布和调用 API

完成 SQL 函数后，可以把它发布为 HTTP API。调用方只需按函数声明传入数据，无需了解函数内部的召回、排序等步骤。

## 发布函数

假设已经定义了名为 `recommend` 的 SQL 函数，API 定义为：

```sql
CREATE OR REPLACE API recommend WITH recommend;
```

第一个 `recommend` 是 API 名称，`WITH` 后的 `recommend` 是 SQL 函数名称。发布后通过以下地址调用：

```text
POST /api/v1/recommend
```

如需覆盖同名 API，使用 `OR REPLACE`。SQL 函数的编写方法见[编写推荐流程](./recommendation-flow.md)。

API 定义与 SQL 函数采用相同的加载方式。使用 SQL 文件时，将上述语句保存为 `api/recommend.sql` 并重启服务，步骤见[管理本地 SQL 定义](../getting-started/docker.md#管理本地-sql-定义)。使用远程元数据时，通过 Beeline 或 JDBC 执行该语句。

## 发起请求

请求体中的 `data` 按“输入表名 → 行数组”组织。每个由 `DEFINE INPUT TABLE` 声明的输入表都必须提供，并且字段应与声明的结构一致。

例如，函数声明如下：

```sql
DEFINE INPUT TABLE user_info (
  user_id BIGINT
);
```

调用请求为：

```bash
curl -X POST http://localhost:30001/api/v1/recommend \
  -H 'Content-Type: application/json' \
  -d '{
    "data": {
      "user_info": [
        {"user_id": 1000001}
      ]
    }
  }'
```

函数有多个输入表时，把它们分别放入 `data`：

```json
{
  "data": {
    "user_info": [{"user_id": 1000001}],
    "context": [{"page": "home"}]
  }
}
```

## 传递执行参数

`params` 适合传递返回数量、实验分组、动态函数名等请求级配置。值必须是字符串，可在 SQL 中通过 `` `get` `` 或 `` `get_or_default` `` 读取：

```json
{
  "data": {
    "user_info": [{"user_id": 1000001}]
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

正常执行返回 HTTP 200。结果行为空时 `data` 是空数组；函数正常结束但没有返回表时，`msg` 给出说明。请求格式错误返回 HTTP 400，函数执行失败返回 HTTP 500，错误原因在 `msg` 中。

调用方应先检查 HTTP 状态码，再读取结果或错误说明。路径不匹配返回 404，方法不支持返回 405。

## 高级选项：指标标签

如果需要给本次调用产生的监控指标附加标签，可传入 `metricTags`：

```json
{
  "data": {
    "user_info": [{"user_id": 1000001}]
  },
  "metricTags": {
    "scene": "homepage"
  }
}
```

`metricTags` 的作用范围和标签取值建议见[可观测性](../operations/observability.md#常用指标)。

## 常见问题

### 提示输入表不存在

检查 `data` 中的键是否与 `DEFINE INPUT TABLE` 的表名完全一致。所有声明的输入表都必须出现；没有数据时也应传入空数组。

### 字段转换失败

检查字段名和 JSON 值是否符合函数声明的数据类型，尤其是数值、数组和时间字段。

### API 找不到

确认请求路径中的名称与 API 定义一致：`/api/v1/{api_name}`，并检查定义是否已按[发布函数](#发布函数)中的说明加载。

### `/sql/v1` 和 `/api/v1` 有什么区别

`/api/v1/{api_name}` 调用已发布的业务函数，是业务接入的推荐方式。`/sql/v1` 用于直接提交 SQL，是否开放由服务配置决定，不应作为普通业务 API 使用。

API 的完整定义语法见 [SQL 语法参考](../reference/sql.md)。
