# 模型训练与在线推理

按你已有的条件选择路径：

- 已有 HTTP 模型服务：使用下面的 `external` 模型接入，不需要 SQLRec 训练、导出或部署推理容器。
- 需要 SQLRec 训练并部署模型：先准备[服务环境](../operations/deployment.md)，再按[训练型模型流程](#训练型模型的完整流程)操作。
- 使用 Hugging Face 模型：下载 Hub 快照后直接创建服务，见[后端差异](#hugging-face-模型的不同之处)。

## 接入已有 HTTP 模型服务

准备 SQLRec 可访问的推理地址，并声明服务的输入、输出字段：

```sql
CREATE MODEL external_rank_model (
  user_id BIGINT,
  item_id BIGINT,
  category VARCHAR,
  price DOUBLE
) WITH (
  'model' = 'external',
  'output_columns' = 'score:FLOAT'
);

CREATE SERVICE external_rank_service
ON MODEL external_rank_model
WITH (
  'url' = 'http://rank-service:8080/predict'
);
```

本地文件模式将两份定义分别保存为 `model/external_rank_model.sql` 和 `service/external_rank_service.sql`，放在完整的 `SQL_SCHEMA_DIR` 下并重启；加载方法见[Docker 指南](../getting-started/docker.md#管理本地-sql-定义)。远程元数据模式通过 Beeline 或 JDBC 执行定义。

先准备包含上述字段的 `rank_input` 缓存表，再调用服务。用空表声明输出结构：

```sql
CACHE TABLE external_rank_output AS
SELECT *, CAST(NULL AS FLOAT) AS score FROM rank_input LIMIT 0;

CACHE TABLE result_table AS
CALL call_service('external_rank_service', rank_input)
LIKE external_rank_output;
```

结果保留输入列，并追加 `score`。仅接入已有服务时，不需要 Kubernetes 训练环境；推理服务仍须遵循以下协议。

::: details HTTP 推理协议（服务提供方）
服务接受 POST 请求。单表调用发送 JSON 对象数组，只包含 Model 声明的输入字段：

```json
[
  {"user_id": 1, "item_id": 101, "category": "phone", "price": 3999.0},
  {"user_id": 1, "item_id": 102, "category": "tablet", "price": 2999.0}
]
```

服务返回按输出字段组织的 JSON 数组；每个数组长度必须与输入行数一致，顺序与输入对应：

```json
{"score": [0.85, 0.72]}
```

使用下方的 User-Item 调用形式时，请求为列式 JSON，User 字段为单元素数组，Item 字段按行排列：

```json
{
  "user_id": [1],
  "item_id": [101, 102],
  "category": ["phone", "tablet"],
  "price": [3999.0, 2999.0]
}
```
:::

## 选择模型后端

| 模型类型 | 数据来源 | TRAIN MODEL | EXPORT MODEL | 服务使用的 Checkpoint |
| --- | --- | --- | --- | --- |
| tzrec Wide & Deep / DSSM | SQL 表 | 训练 | 需要 | export |
| LightGBM / XGBoost / CatBoost | SQL 表 | 训练 | 需要 | export |
| Hugging Face Transformers | Hub 快照 | 下载，不需要 ON 表 | 不支持 | origin |
| external | 已有 HTTP 服务 | 不支持 | 不支持 | 不需要 |

配置参数见[内置模型](../reference/models/builtin-models.md)。

## 核心对象

| 对象 | 作用 |
|------|------|
| Model | 保存模型类型、输入字段和默认配置 |
| Checkpoint | 一次训练、下载或导出产生的版本化结果 |
| Service | 将指定 Checkpoint 部署为可调用的在线服务 |

Checkpoint 有两种类型：

- `origin`：训练或下载产生的原始结果。
- `export`：由 `EXPORT MODEL` 生成、可供 tzrec 或 GBDT 在线服务加载的结果。

Checkpoint 名称是用户自定义的版本标识。建议使用可追溯的值，例如日期或发布版本，不要在不同数据和配置上重复使用同一名称。

## 训练型模型的完整流程

以下训练、导出和自托管服务部署需要[完整服务环境](../operations/deployment.md)，包括 Kubernetes 和模型存储。Docker Demo 不提供这些组件。执行前还需准备可访问的 `training_sample` 训练表，字段与模型定义兼容。

### 1. 创建模型

```sql
CREATE MODEL rank_model (
  user_id BIGINT,
  item_id BIGINT,
  category VARCHAR,
  price DOUBLE,
  is_click INT
) WITH (
  'model' = 'tzrec.wide_and_deep',
  'label_columns' = 'is_click'
);
```

字段列表是模型的输入协议。在训练和推理时，应保证数据表中相应列的名称和类型匹配。

### 2. 训练并生成 origin checkpoint

```sql
TRAIN MODEL rank_model CHECKPOINT = '2026_09_13'
ON training_sample
WHERE dt = '2026-09-13'
WITH (
  'num_epochs' = '1',
  'batch_size' = '8192'
);
```

`TRAIN MODEL` 会提交 Kubernetes 训练任务。语句成功提交不等于训练已完成；创建导出任务前，先使用以下语句检查状态：

```sql
SHOW CHECKPOINTS rank_model;

DESCRIBE FORMATTED MODEL rank_model
CHECKPOINT = '2026_09_13';
```

### 3. 导出模型

```sql
EXPORT MODEL rank_model CHECKPOINT = '2026_09_13'
ON training_sample;
```

导出的具体处理由模型后端决定。导出完成后会生成 export checkpoint，常规单模型的名称为 `<origin_checkpoint>_export`。DSSM 会分别生成 user 和 item 塔产物，具体名称以 `SHOW CHECKPOINTS` 的结果为准。

### 4. 创建在线服务

```sql
CREATE SERVICE rank_service
ON MODEL rank_model
CHECKPOINT = '2026_09_13_export'
WITH (
  'replicas' = '1',
  'pod_cpu_cores' = '1',
  'pod_memory' = '2Gi'
);
```

创建服务后可以查看定义和状态信息：

```sql
SHOW SERVICES;

DESCRIBE FORMATTED SERVICE rank_service;
```

## 调用模型服务

`call_service` 将模型输出追加到输入数据。模型服务的输入字段应在 Model 中声明，并与传入表的字段对应。

### 行式输入

```sql
CACHE TABLE rank_input AS
SELECT user_id, item_id, category, price FROM candidate_item;

CACHE TABLE rank_output AS
SELECT *, CAST(NULL AS FLOAT) AS probs FROM rank_input LIMIT 0;

CACHE TABLE ranked_item AS
CALL call_service('rank_service', rank_input) LIKE rank_output;
```

这个 Wide & Deep 示例保留输入列，并追加 `probs`；其他模型的输出字段见[内置模型](../reference/models/builtin-models.md)。

### User-Item 输入

排序时可分开传入一行用户特征和多行候选物品，避免重复发送用户数据：

```sql
CACHE TABLE item_rank_output AS
SELECT *, CAST(NULL AS FLOAT) AS probs FROM item_candidates LIMIT 0;

CACHE TABLE ranked_item AS
CALL call_service('rank_service', user_features, item_candidates)
LIKE item_rank_output;
```

- User 表必须恰好一行；Item 表可以有多行。
- 同名输入字段优先从 User 表取值，其余模型字段从 Item 表取值。
- 结果保留 Item 表字段，并追加模型输出字段。
- Item 表为空时不发 HTTP 请求，直接返回结构完整的空表。

外部服务的请求和响应约定见[HTTP 推理协议](#接入已有-http-模型服务)。

## Hugging Face 模型的不同之处

Hugging Face 后端的 `TRAIN MODEL` 表示从 Hub 下载指定 revision，不是使用 SQL 表训练，因此不需要 `ON data_source`：

```sql
TRAIN MODEL text_embedding_model CHECKPOINT = 'v1' WITH (
  'revision' = 'main'
);
```

该 checkpoint 的类型为 origin，可以直接创建服务；当前不支持 `EXPORT MODEL`。完整的任务和参数见[内置模型](../reference/models/builtin-models.md)。

## 常见问题

### 创建 Service 时提示 checkpoint 类型错误

确认模型后端需要的 checkpoint 类型。tzrec 和 GBDT 需要 export，Hugging Face 使用 origin，external 不需要 checkpoint。

### 训练数据字段不匹配

对比 `DESCRIBE MODEL` 结果与训练表字段，确认字段名、类型、标签列以及模型特有的 user/item 特征配置。

### `call_service` 返回行数不正确

检查模型服务返回的每个数组是否与输入行数一致，并确认输出字段名与当前模型控制器定义一致。
