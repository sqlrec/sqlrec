# 内置模型

本文档介绍 SQLRec 内置的模型类型及其使用方法。

## 通用流程与资源配置

Model、训练、导出和 Service 的 `WITH` 选项使用字符串键和值，例如 `'batch_size' = '8192'`；SQLRec 会按参数类型解析值。

训练型模型按[模型指南](../../guides/model-lifecycle.md#训练型模型的完整流程)执行创建、训练、导出和服务部署。训练型模型的示例以 Model 定义为主，`training_sample` 等训练表需另行准备。训练配置可作为 Model 默认值，并在 `TRAIN MODEL ... WITH (...)` 中覆盖。

下表的资源配置适用于 tzrec、GBDT 和 Hugging Face 自托管任务或服务；`replicas` 用于在线服务。

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `pod_cpu_cores` | Integer | 1 | Pod CPU 核数 |
| `pod_memory` | String | "2Gi" | Pod 内存 |
| `pod_cpu_limit` | String | - | Pod CPU 上限 |
| `pod_memory_limit` | String | - | Pod 内存上限 |
| `replicas` | Integer | 1 | 服务副本数 |

镜像默认值：tzrec 使用 `sqlrec/tzrec`，GBDT 使用 `sqlrec/gbdt`，版本为 `${SQLREC_VERSION}-cpu`；Hugging Face 使用 `sqlrec/transformers:${SQLREC_VERSION}`。可用 `image`、`version` 覆盖。

## 内置模型类型

SQLRec 内置了以下模型类型：

### 1. 外部模型

外部模型用于对接已有的外部模型服务，不支持训练和导出操作。

**模型名称**：`external`

**特性**：
- 连接外部已有的模型推理服务
- 不支持训练（`TRAIN MODEL`）
- 不支持导出（`EXPORT MODEL`）
- 通过 URL 直接访问服务

**配置参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `output_columns` | String | 输出列定义，格式：`name1:type1,name2:type2` |

`output_columns` 是 Model 配置；`url` 是 Service 配置，用于指定实际调用的推理地址。

**使用示例**：

```sql
CREATE MODEL external_model (
    user_id BIGINT,
    item_id BIGINT,
    category VARCHAR,
    price DOUBLE
) WITH (
    'model' = 'external',
    'output_columns' = 'score:FLOAT,label:VARCHAR'
);

CREATE SERVICE external_service
    ON MODEL external_model
    WITH (
        'url' = 'http://external-service:8080/predict'
    );
```

### 2. Wide & Deep 模型

Wide & Deep 模型是基于 tzrec 框架实现的推荐模型，支持完整的训练、导出和服务部署流程。

**模型名称**：`tzrec.wide_and_deep`

**特性**：
- 支持 Wide & Deep 架构的推荐模型
- 支持分布式训练（PyTorch Distributed）
- 支持 Parquet 格式的训练数据
- 自动生成 Kubernetes 训练和服务 YAML
- 支持稀疏特征和稠密特征

**输出字段**：

| 字段名 | 类型 | 说明 |
|--------|------|------|
| `probs` | FLOAT | 预测概率值 |

**必需参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `label_columns` | String | 标签列名 |

**训练配置参数**：

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `sparse_lr` | Double | 0.001 | 稀疏特征学习率 |
| `dense_lr` | Double | 0.001 | 稠密特征学习率 |
| `num_epochs` | Integer | 1 | 训练轮数 |
| `batch_size` | Integer | 8192 | 批次大小 |
| `num_workers` | Integer | 8 | 数据加载工作进程数 |
| `embedding_dim` | Integer | 16 | 嵌入维度 |
| `num_buckets` | Integer | 1000000 | 整数特征分桶数 |
| `hidden_units` | String | "512,256,128" | 深度网络隐藏层单元数 |
| `mixed_precision` | String | - | 混合精度训练模式，可选 `BF16`/`FP16`，默认不开启 |

**分布式训练参数**：

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `nnodes` | Integer | 1 | 训练节点数 |
| `nproc_per_node` | Integer | 1 | 每节点进程数 |
| `master_port` | Integer | 29500 | 分布式训练主端口 |

**列级配置参数**：

可以为每个特征列单独配置参数：

| 参数格式 | 说明 |
|----------|------|
| `column.{feature_name}.bucket_size` | 特征的分桶数量 |
| `column.{feature_name}.embedding_dim` | 特征的嵌入维度 |

**使用示例**：

```sql
CREATE MODEL rec_model (
    user_id VARCHAR,
    item_id VARCHAR,
    category VARCHAR,
    price DOUBLE,
    label INT
) WITH (
    'model' = 'tzrec.wide_and_deep',
    'label_columns' = 'label',
    'embedding_dim' = '32',
    'hidden_units' = '512,256,128',
    'column.user_id.embedding_dim' = '64',
    'column.item_id.embedding_dim' = '64'
);
```

### 3. DSSM 模型

DSSM（Deep Structured Semantic Models）模型是基于 tzrec 框架实现的双塔召回模型，支持完整的训练、导出和服务部署流程。

**模型名称**：`tzrec.dssm`

**特性**：
- 支持双塔架构的召回模型
- 用户塔和物品塔分别生成嵌入向量
- 支持分布式训练（PyTorch Distributed）
- 支持 Parquet 格式的训练数据
- 自动生成 Kubernetes 训练和服务 YAML
- 支持稀疏特征和稠密特征

**输出字段**：

| 字段名 | 类型 | 说明 |
|--------|------|------|
| `user_tower_emb` | ARRAY\<FLOAT\> | 用户塔嵌入向量 |
| `item_tower_emb` | ARRAY\<FLOAT\> | 物品塔嵌入向量 |

**必需参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `user_features` | String | 用户特征列名，多个特征用逗号分隔 |
| `item_features` | String | 物品特征列名，多个特征用逗号分隔 |

**注意**：`user_features` 和 `item_features` 至少需要配置其中一个。

**训练配置参数**：

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `sparse_lr` | Double | 0.001 | 稀疏特征学习率 |
| `dense_lr` | Double | 0.001 | 稠密特征学习率 |
| `num_epochs` | Integer | 1 | 训练轮数 |
| `batch_size` | Integer | 8192 | 批次大小 |
| `num_workers` | Integer | 8 | 数据加载工作进程数 |
| `embedding_dim` | Integer | 16 | 嵌入维度 |
| `num_buckets` | Integer | 1000000 | 整数特征分桶数 |
| `hidden_units` | String | "512,256,128" | 深度网络隐藏层单元数 |
| `user_hidden_units` | String | "512,256,128" | 用户塔隐藏层单元数 |
| `item_hidden_units` | String | "512,256,128" | 物品塔隐藏层单元数 |
| `output_dim` | Integer | 64 | 输出嵌入维度 |
| `mixed_precision` | String | - | 混合精度训练模式，可选 `BF16`/`FP16`，默认不开启 |

**分布式训练参数**：

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `nnodes` | Integer | 1 | 训练节点数 |
| `nproc_per_node` | Integer | 1 | 每节点进程数 |
| `master_port` | Integer | 29500 | 分布式训练主端口 |

**列级配置参数**：

可以为每个特征列单独配置参数：

| 参数格式 | 说明 |
|----------|------|
| `column.{feature_name}.bucket_size` | 特征的分桶数量 |
| `column.{feature_name}.embedding_dim` | 特征的嵌入维度 |

**使用示例**：

```sql
CREATE MODEL dssm_model (
    user_id VARCHAR,
    user_age INT,
    item_id VARCHAR,
    item_category VARCHAR,
    label INT
) WITH (
    'model' = 'tzrec.dssm',
    'user_features' = 'user_id,user_age',
    'item_features' = 'item_id,item_category',
    'embedding_dim' = '64',
    'hidden_units' = '256,128,64'
);

-- 按模型指南完成训练和导出后，DSSM 生成两个 tower checkpoint：
-- v1.0_export/item 和 v1.0_export/user
CREATE SERVICE dssm_item_service
    ON MODEL dssm_model
    CHECKPOINT = 'v1.0_export/item';

CREATE SERVICE dssm_user_service
    ON MODEL dssm_model
    CHECKPOINT = 'v1.0_export/user';
```

### 4. LightGBM 模型

LightGBM 模型是基于 GBDT（梯度提升决策树）框架实现的模型，支持完整的训练、导出和服务部署流程。训练数据和模型文件使用配置的存储，导出时转换为 ONNX 格式用于在线推理。

**模型名称**：`gbdt.lightgbm`

**特性**：
- 基于 LightGBM 框架的梯度提升树模型
- 仅支持浮点数值特征（float/double），不支持类别特征（如需类别/整数特征请使用 CatBoost）
- 支持 Parquet 格式训练数据（存储在分布式存储）
- 模型文件持久化到分布式存储
- 导出 ONNX 格式用于 serving（通过 onnxmltools 转换）
- C++ ONNX Runtime 推理服务

**输出字段**：

| 字段名 | 类型 | 说明 |
|--------|------|------|
| `probs` | FLOAT | 预测概率值 |

**必需参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `label_columns` | String | 标签列名 |

**训练配置参数**：

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `objective` | String | "binary" | 学习目标（binary, multiclass, regression） |
| `metric` | String | "auc" | 评估指标（auc, logloss, rmse） |
| `num_iterations` | Integer | 300 | boosting 迭代次数 |
| `learning_rate` | Double | 0.1 | 学习率 |
| `num_leaves` | Integer | 63 | 每棵树最大叶子数 |
| `max_depth` | Integer | 6 | 最大树深度 |
| `feature_fraction` | Double | 0.8 | 每棵树使用的特征比例 |
| `bagging_fraction` | Double | 0.8 | 每棵树使用的数据比例 |
| `bagging_freq` | Integer | 5 | bagging 频率 |
| `min_data_in_leaf` | Integer | 20 | 叶子节点最小样本数 |
| `l2_regularization` | Double | 1.0 | L2 正则化系数 |

**使用示例**：

```sql
CREATE MODEL lgb_model (
    user_id FLOAT,
    age FLOAT,
    item_id FLOAT,
    item_price FLOAT,
    label INT
) WITH (
    'model' = 'gbdt.lightgbm',
    'label_columns' = 'label',
    'num_iterations' = '200',
    'learning_rate' = '0.05',
    'num_leaves' = '127'
);
```

### 5. XGBoost 模型

XGBoost 模型是基于 GBDT（梯度提升决策树）框架实现的模型，支持完整的训练、导出和服务部署流程。训练数据和模型文件均存储在分布式存储上，导出时转换为 ONNX 格式用于在线推理。

**模型名称**：`gbdt.xgboost`

**特性**：
- 基于 XGBoost 框架的梯度提升树模型
- 仅支持浮点数值特征（float/double）
- 支持 Parquet 格式训练数据（存储在分布式存储）
- 模型文件持久化到分布式存储
- 导出 ONNX 格式用于 serving（通过 onnxmltools 转换）
- C++ ONNX Runtime 推理服务

**输出字段**：

| 字段名 | 类型 | 说明 |
|--------|------|------|
| `probs` | FLOAT | 预测概率值 |

**必需参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `label_columns` | String | 标签列名 |

**训练配置参数**：

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `objective` | String | "binary" | 学习目标（binary, multiclass, regression） |
| `metric` | String | "auc" | 评估指标（auc, logloss, rmse） |
| `num_iterations` | Integer | 300 | boosting 迭代次数 |
| `learning_rate` | Double | 0.1 | 学习率 |
| `max_depth` | Integer | 6 | 最大树深度 |
| `feature_fraction` | Double | 0.8 | 每棵树使用的特征比例（对应 XGBoost colsample_bytree） |
| `bagging_fraction` | Double | 0.8 | 每棵树使用的数据比例（对应 XGBoost subsample） |
| `min_child_weight` | Integer | 1 | 子节点最小权重和 |
| `l2_regularization` | Double | 1.0 | L2 正则化系数（对应 XGBoost reg_lambda） |

**使用示例**：

```sql
CREATE MODEL xgb_model (
    user_id FLOAT,
    user_age FLOAT,
    item_id FLOAT,
    item_price FLOAT,
    label INT
) WITH (
    'model' = 'gbdt.xgboost',
    'label_columns' = 'label',
    'num_iterations' = '200',
    'learning_rate' = '0.05',
    'max_depth' = '8'
);
```

### 6. CatBoost 模型

CatBoost 模型是基于 GBDT 框架实现的模型，原生支持类别特征处理，支持完整的训练、导出和服务部署流程。训练数据和模型文件使用配置的存储，导出原生 `.cbm` 格式用于在线推理。

**模型名称**：`gbdt.catboost`

**特性**：
- 基于 CatBoost 框架的梯度提升树模型
- 原生支持类别特征处理（无需手动编码）；int/bigint/string 类型的列自动作为类别特征，float/double 类型的列作为数值特征
- 支持 Parquet 格式训练数据（存储在分布式存储）
- 模型文件持久化到分布式存储
- 导出原生 .cbm 格式用于 serving（通过 CatBoost C API 直接加载，支持类别特征）
- C++ CatBoost 原生推理服务

**输出字段**：

| 字段名 | 类型 | 说明 |
|--------|------|------|
| `probs` | FLOAT | 预测概率值 |

**必需参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `label_columns` | String | 标签列名 |

**训练配置参数**：

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `objective` | String | "binary" | 学习目标（binary, multiclass, regression） |
| `metric` | String | "auc" | 评估指标（auc, logloss, rmse） |
| `cb_iterations` | Integer | 1000 | CatBoost 迭代次数 |
| `cb_depth` | Integer | 6 | CatBoost 树深度 |
| `cb_l2_leaf_reg` | Double | 3.0 | L2 叶子正则化系数 |
| `learning_rate` | Double | 0.1 | 学习率 |

**使用示例**：

```sql
CREATE MODEL cb_model (
    user_id BIGINT,
    user_country VARCHAR,
    age INT,
    item_id BIGINT,
    item_category VARCHAR,
    label INT
) WITH (
    'model' = 'gbdt.catboost',
    'label_columns' = 'label',
    'cb_iterations' = '1000',
    'cb_depth' = '8',
    'learning_rate' = '0.03'
);
```

### 7. Hugging Face Transformers 模型

**模型名称**：`huggingface.transformers`

该模型类型把 `TRAIN MODEL` 定义为从 Hugging Face Hub 下载指定 revision，并通过 Hadoop CLI 将快照保存为可直接部署的 origin checkpoint。暂不支持 `EXPORT MODEL`。

首期任务包括 `text-classification`、`text-generation`、`embedding` 和 `image-embedding`。文本生成仅接受普通 prompt；图片 embedding 仅接受 HTTP/HTTPS URL。

#### Hugging Face 任务配置

`task` 和 `repo_id` 必填。输入列必须在 Model 字段列表中声明为 `STRING`，并按任务选择列名配置：

| task | 必填输入配置 | 输出字段 |
| --- | --- | --- |
| `text-classification` | `text_column` | `label`（STRING）、`score`（FLOAT） |
| `text-generation` | `prompt_column` | `generated_text`（STRING） |
| `embedding` | `text_column` | `embedding`（ARRAY&lt;FLOAT&gt;） |
| `image-embedding` | `image_column` | `embedding`（ARRAY&lt;FLOAT&gt;） |

`repo_id` 是 Hub 仓库名称；`revision` 可指定分支、标签或 commit，默认 `main`。固定 commit 可让下载版本更容易复现。下面定义一个文本 embedding 模型：

```sql
CREATE MODEL text_embedding_model (
    text STRING
) WITH (
    'model' = 'huggingface.transformers',
    'task' = 'embedding',
    'repo_id' = 'intfloat/multilingual-e5-small',
    'text_column' = 'text',
    'pooling' = 'mean',
    'normalize' = 'true'
);

TRAIN MODEL text_embedding_model CHECKPOINT = 'v1' WITH (
    'revision' = 'main'
);

CREATE SERVICE text_embedding_service
    ON MODEL text_embedding_model
    CHECKPOINT = 'v1'
    WITH (
        'device' = 'auto',
        'inference_batch_size' = '32'
    );
```

私有仓库可在 TRAIN 参数中通过 `hf_token_secret` 和 `hf_token_secret_key` 引用 Kubernetes Secret。服务仅从 checkpoint 加载模型，不访问 Hub。

#### Hugging Face 服务配置

在 `CREATE SERVICE ... WITH (...)` 中按需设置：

| 参数 | 默认值 | 用法 |
| --- | --- | --- |
| `device` | `auto` | 可选 `auto`、`cpu`、`cuda` |
| `inference_batch_size` | `8` | 服务内部推理批次大小 |
| `dtype` | `auto` | 可选 `auto`、`float32`、`float16`、`bfloat16` |
| `pod_gpu` | `0` | 请求的 GPU 数量；GPU 推理需设为正数，并准备可用 GPU 节点 |
| `pod_gpu_resource` | `nvidia.com/gpu` | 集群的 GPU 资源名称 |

GPU 推理可设置 `'device' = 'cuda', 'pod_gpu' = '1'`；还需要集群提供匹配的 GPU 驱动、设备插件和运行环境。

::: details 按任务调整
文本 embedding 可配置 `pooling`（默认 `mean`）和 `normalize`（默认 `true`）；文本分类可用 `text_pair_column` 指定第二个文本列。文本生成可配置 `max_new_tokens`（默认 `128`）、`do_sample`（默认 `false`）、`temperature` 和 `top_p`。

图片 embedding 的 `image_column` 必须包含 HTTP/HTTPS URL。需要限制下载来源或大小时，可设置 `image_url_allowed_hosts`、`image_download_timeout_ms`（默认 `5000`）、`image_max_bytes`（默认 10 MiB）和 `image_max_pixels`（默认 2000 万）。
:::
