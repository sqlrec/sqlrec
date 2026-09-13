# 模型训练与在线推理

SQLRec 可以用 SQL 管理模型定义、训练结果和在线推理服务。不同模型后端的流程并不完全相同：

| 模型类型 | 数据来源 | 需要训练 | 需要导出 | 创建服务时的 Checkpoint |
|----------|----------|----------|----------|----------------------------|
| tzrec Wide & Deep / DSSM | SQL 表 | 是 | 是 | export |
| LightGBM / XGBoost / CatBoost | SQL 表 | 是 | 是 | export |
| Hugging Face Transformers | Hugging Face Hub 快照 | 是，但不需要 `ON` 数据表 | 否 | origin |
| external | 已有 HTTP 服务 | 否 | 否 | 不需要 |

各模型的配置参数见[内置模型](../reference/models/builtin-models.md)。

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

### 1. 创建模型

```sql
CREATE MODEL rank_model (
  user_id BIGINT,
  item_id BIGINT,
  category VARCHAR,
  price DOUBLE,
  is_click INT
) WITH (
  model = 'tzrec.wide_and_deep',
  label_columns = 'is_click'
);
```

字段列表是模型的输入协议。在训练和推理时，应保证数据表中相应列的名称和类型匹配。

### 2. 训练并生成 origin checkpoint

```sql
TRAIN MODEL rank_model CHECKPOINT = '2026_09_13'
ON training_sample
WHERE dt = '2026-09-13'
WITH (
  num_epochs = 1,
  batch_size = 8192
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
  replicas = 1,
  pod_cpu_cores = 1,
  pod_memory = '2Gi'
);
```

创建服务后可以查看定义和状态信息：

```sql
SHOW SERVICES;

DESCRIBE FORMATTED SERVICE rank_service;
```

## 调用模型服务

SQLRec 内置的 `call_service` 表函数负责查找 Service、组装 HTTP 请求，并将模型输出追加到输入数据。

### 行式输入

```sql
CACHE TABLE rank_input AS
SELECT user_id, item_id, category, price
FROM candidate_item;

CACHE TABLE ranked_item AS
CALL call_service('rank_service', rank_input);
```

函数只会发送模型定义中存在的输入字段。返回表保留输入表的列，并在末尾追加模型输出列。

发往模型服务的请求为 JSON 对象数组：

```json
[
  {"user_id": 1, "item_id": 101, "category": "phone", "price": 3999.0},
  {"user_id": 1, "item_id": 102, "category": "tablet", "price": 2999.0}
]
```

服务应返回以输出字段为键、数组为值的 JSON 对象：

```json
{"probs": [0.85, 0.72]}
```

每个输出数组的长度必须与输入行数一致。

### User-Item 输入

排序场景中，可以分开传入一行用户特征和多行候选物品，避免在请求体中重复用户数据：

```sql
CACHE TABLE ranked_item AS
CALL call_service('rank_service', user_features, item_candidates);
```

使用该形式时：

- User 表必须恰好有一行，Item 表可以有多行。
- 同名字段优先从 User 表取值，其余模型输入字段从 Item 表取值。
- 返回表保留 Item 表字段，并追加模型输出字段。
- Item 表为空时不发起 HTTP 请求，直接返回结构完整的空表。

请求体为列式 JSON：

```json
{
  "user_id": [1],
  "item_id": [101, 102],
  "category": ["phone", "tablet"],
  "price": [3999.0, 2999.0]
}
```

## 接入已有 HTTP 模型服务

如果已经有符合上述请求和响应协议的服务，可以使用 `external` 模型，不需要执行 `TRAIN MODEL` 和 `EXPORT MODEL`：

```sql
CREATE MODEL external_rank_model WITH (
  model = 'external',
  output_columns = 'score:FLOAT'
);

CREATE SERVICE external_rank_service
ON MODEL external_rank_model
WITH (
  url = 'http://rank-service:8080/predict'
);
```

SQL 中的调用方式与其他 Service 一致：

```sql
CACHE TABLE result AS
CALL call_service('external_rank_service', rank_input);
```

## Hugging Face 模型的不同之处

Hugging Face 后端的 `TRAIN MODEL` 表示从 Hub 下载指定 revision，不是使用 SQL 表训练，因此不需要 `ON data_source`：

```sql
TRAIN MODEL text_embedding_model CHECKPOINT = 'v1' WITH (
  revision = 'main'
);
```

该 checkpoint 的类型为 origin，可以直接创建服务；当前不支持 `EXPORT MODEL`。完整的任务和参数见[内置模型](../reference/models/builtin-models.md)。

## 运行前提

训练、导出和 Service 部署依赖 Kubernetes 以及已配置的模型存储。对 tzrec 和 GBDT 模型，还必须保证训练任务能访问 SQL 中指定的数据源。具体环境要求见[服务部署](../operations/deployment.md)。

## 常见问题

### 创建 Service 时提示 checkpoint 类型错误

确认模型后端需要的 checkpoint 类型。tzrec 和 GBDT 需要 export，Hugging Face 使用 origin，external 不需要 checkpoint。

### 训练数据字段不匹配

对比 `SHOW CREATE MODEL` 结果与训练表字段，确认字段名、类型、标签列以及模型特有的 user/item 特征配置。

### `call_service` 返回行数不正确

检查模型服务返回的每个数组是否与输入行数一致，并确认输出字段名与当前模型控制器定义一致。
