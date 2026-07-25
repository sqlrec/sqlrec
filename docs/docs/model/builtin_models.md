# 内置模型

本文档介绍 SQLRec 内置的模型类型及其使用方法。

## 内置模型类型

SQLRec 内置了以下模型类型：

### 1. External Model（外部模型）

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
| `url` | String | 外部模型服务的 URL 地址 |
| `output_columns` | String | 输出列定义，格式：`name1:type1,name2:type2` |

**使用示例**：

```sql
CREATE MODEL external_model WITH (
    model = 'external',
    url = 'http://external-service:8080/predict',
    output_columns = 'score:FLOAT,label:VARCHAR'
);

CREATE SERVICE external_service
    ON MODEL external_model;
```

### 2. Wide & Deep Model（推荐模型）

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
| `label_columns` | String | - | 标签列名 |

**分布式训练参数**：

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `nnodes` | Integer | 1 | 训练节点数 |
| `nproc_per_node` | Integer | 1 | 每节点进程数 |
| `master_port` | Integer | 29500 | 分布式训练主端口 |

**资源配置参数**：

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `image` | String | "sqlrec/tzrec" | Docker 镜像名称 |
| `version` | String | "0.1.0-cpu" | Docker 镜像版本 |
| `pod_cpu_cores` | Integer | 2 | Pod CPU 核数 |
| `pod_memory` | String | "8Gi" | Pod 内存 |
| `replicas` | Integer | 1 | 服务副本数 |

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
    model = 'tzrec.wide_and_deep',
    label_columns = 'label',
    embedding_dim = 32,
    hidden_units = '512,256,128',
    column.user_id.embedding_dim = 64,
    column.item_id.embedding_dim = 64
);

TRAIN MODEL rec_model CHECKPOINT = 'v1.0'
    ON training_data
    WITH (
        num_epochs = 10,
        batch_size = 4096,
        sparse_lr = 0.01,
        nnodes = 2,
        nproc_per_node = 4
    );

CREATE SERVICE rec_service
    ON MODEL rec_model
    CHECKPOINT = 'v1.0'
    WITH (
        replicas = 3,
        pod_cpu_cores = 4,
        pod_memory = '16Gi'
    );
```

### 3. DSSM Model（双塔召回模型）

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

**分布式训练参数**：

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `nnodes` | Integer | 1 | 训练节点数 |
| `nproc_per_node` | Integer | 1 | 每节点进程数 |
| `master_port` | Integer | 29500 | 分布式训练主端口 |

**资源配置参数**：

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `image` | String | "sqlrec/tzrec" | Docker 镜像名称 |
| `version` | String | "0.1.0-cpu" | Docker 镜像版本 |
| `pod_cpu_cores` | Integer | 2 | Pod CPU 核数 |
| `pod_memory` | String | "8Gi" | Pod 内存 |
| `replicas` | Integer | 1 | 服务副本数 |

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
    model = 'tzrec.dssm',
    user_features = 'user_id,user_age',
    item_features = 'item_id,item_category',
    embedding_dim = 64,
    hidden_units = '256,128,64'
);

TRAIN MODEL dssm_model CHECKPOINT = 'v1.0'
    ON training_data
    WITH (
        num_epochs = 10,
        batch_size = 4096,
        nnodes = 2,
        nproc_per_node = 4
    );

EXPORT MODEL dssm_model CHECKPOINT = 'v1.0';

CREATE SERVICE dssm_service
    ON MODEL dssm_model
    CHECKPOINT = 'v1.0'
    WITH (
        replicas = 3,
        pod_cpu_cores = 4,
        pod_memory = '16Gi'
    );
```

### 4. LightGBM Model（梯度提升树模型）

LightGBM 模型是基于 GBDT（梯度提升决策树）框架实现的模型，支持完整的训练、导出和服务部署流程。训练数据和模型文件均存储在 HDFS 上，导出时转换为 ONNX 格式用于在线推理。

**模型名称**：`gbdt.lightgbm`

**特性**：
- 基于 LightGBM 框架的梯度提升树模型
- 原生支持类别特征（categorical features）
- 支持 LightGBM socket 分布式训练
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
| `num_iterations` | Integer | 100 | boosting 迭代次数 |
| `learning_rate` | Double | 0.1 | 学习率 |
| `num_leaves` | Integer | 63 | 每棵树最大叶子数 |
| `max_depth` | Integer | 6 | 最大树深度 |
| `feature_fraction` | Double | 0.9 | 每棵树使用的特征比例 |
| `bagging_fraction` | Double | 0.9 | 每棵树使用的数据比例 |
| `bagging_freq` | Integer | 5 | bagging 频率 |
| `min_data_in_leaf` | Integer | 20 | 叶子节点最小样本数 |
| `l2_regularization` | Double | 1.0 | L2 正则化系数 |
| `categorical_features` | String | "" | 类别特征列名（逗号分隔） |

**分布式训练参数**：

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `nnodes` | Integer | 1 | 训练节点数（>1 启用 LightGBM 分布式） |
| `nproc_per_node` | Integer | 1 | 每节点进程数 |
| `master_port` | Integer | 29500 | 分布式训练主端口 |

**资源配置参数**：

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `image` | String | "sqlrec/gbdt" | Docker 镜像名称 |
| `version` | String | "0.1.0-cpu" | Docker 镜像版本 |
| `pod_cpu_cores` | Integer | 1 | Pod CPU 核数 |
| `pod_memory` | String | "2Gi" | Pod 内存 |
| `pod_cpu_limit` | String | - | Pod CPU 上限 |
| `pod_memory_limit` | String | - | Pod 内存上限 |
| `replicas` | Integer | 1 | 服务副本数 |

**使用示例**：

```sql
CREATE MODEL lgb_model (
    user_id BIGINT,
    user_country VARCHAR,
    age INT,
    item_id BIGINT,
    item_category VARCHAR,
    label INT
) WITH (
    model = 'gbdt.lightgbm',
    label_columns = 'label',
    categorical_features = 'user_country,item_category',
    num_iterations = 200,
    learning_rate = 0.05,
    num_leaves = 127
);

TRAIN MODEL lgb_model CHECKPOINT = 'v1.0'
    ON training_data
    WITH (
        num_iterations = 500,
        nnodes = 2,
        nproc_per_node = 4
    );

EXPORT MODEL lgb_model CHECKPOINT = 'v1.0';

CREATE SERVICE lgb_service
    ON MODEL lgb_model
    CHECKPOINT = 'v1.0'
    WITH (
        replicas = 3,
        pod_cpu_cores = 4,
        pod_memory = '16Gi'
    );
```

### 5. XGBoost Model（梯度提升树模型）

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
| `num_iterations` | Integer | 100 | boosting 迭代次数 |
| `learning_rate` | Double | 0.1 | 学习率 |
| `max_depth` | Integer | 6 | 最大树深度 |
| `feature_fraction` | Double | 0.9 | 每棵树使用的特征比例（对应 XGBoost colsample_bytree） |
| `bagging_fraction` | Double | 0.9 | 每棵树使用的数据比例（对应 XGBoost subsample） |
| `min_child_weight` | Integer | 1 | 子节点最小权重和 |
| `l2_regularization` | Double | 1.0 | L2 正则化系数（对应 XGBoost reg_lambda） |

**资源配置参数**：

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `image` | String | "sqlrec/gbdt" | Docker 镜像名称 |
| `version` | String | "0.1.0-cpu" | Docker 镜像版本 |
| `pod_cpu_cores` | Integer | 1 | Pod CPU 核数 |
| `pod_memory` | String | "2Gi" | Pod 内存 |
| `pod_cpu_limit` | String | - | Pod CPU 上限 |
| `pod_memory_limit` | String | - | Pod 内存上限 |
| `replicas` | Integer | 1 | 服务副本数 |

**使用示例**：

```sql
CREATE MODEL xgb_model (
    user_id FLOAT,
    user_age FLOAT,
    item_id FLOAT,
    item_price FLOAT,
    label INT
) WITH (
    model = 'gbdt.xgboost',
    label_columns = 'label',
    num_iterations = 200,
    learning_rate = 0.05,
    max_depth = 8
);

TRAIN MODEL xgb_model CHECKPOINT = 'v1.0'
    ON training_data
    WITH (
        num_iterations = 500
    );

EXPORT MODEL xgb_model CHECKPOINT = 'v1.0';

CREATE SERVICE xgb_service
    ON MODEL xgb_model
    CHECKPOINT = 'v1.0'
    WITH (
        replicas = 3,
        pod_cpu_cores = 4,
        pod_memory = '16Gi'
    );
```

### 6. CatBoost Model（梯度提升树模型）

CatBoost 模型是基于 GBDT 框架实现的模型，原生支持类别特征处理，支持完整的训练、导出和服务部署流程。训练数据和模型文件均存储在 HDFS 上，导出时转换为 ONNX 格式用于在线推理。

**模型名称**：`gbdt.catboost`

**特性**：
- 基于 CatBoost 框架的梯度提升树模型
- 原生支持类别特征处理（无需手动编码）
- 支持 CatBoost 分布式训练（`catboost run-worker` daemon + `fit_with_workers` Python API）
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
| `categorical_features` | String | "" | 类别特征列名（逗号分隔） |

**分布式训练参数**：

> 当 `nnodes > 1` 时启用 CatBoost 分布式训练：每个 worker Pod 启动 `catboost run-worker` 守护进程，master Pod 通过 `fit_with_workers()` API 驱动训练。worker 端点遵循 Indexed-Job DNS 约定 `{job_name}-{i}.{service_name}:{master_port + i}`。

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `nnodes` | Integer | 1 | 训练节点数（>1 启用 CatBoost 分布式训练） |
| `nproc_per_node` | Integer | 1 | 每节点进程数 |
| `master_port` | Integer | 29500 | 分布式训练主端口（每个 worker 监听 master_port + node_rank） |

**资源配置参数**：

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `image` | String | "sqlrec/gbdt" | Docker 镜像名称 |
| `version` | String | "0.1.0-cpu" | Docker 镜像版本 |
| `pod_cpu_cores` | Integer | 1 | Pod CPU 核数 |
| `pod_memory` | String | "2Gi" | Pod 内存 |
| `pod_cpu_limit` | String | - | Pod CPU 上限 |
| `pod_memory_limit` | String | - | Pod 内存上限 |
| `replicas` | Integer | 1 | 服务副本数 |

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
    model = 'gbdt.catboost',
    label_columns = 'label',
    categorical_features = 'user_country,item_category',
    cb_iterations = 1000,
    cb_depth = 8,
    learning_rate = 0.03
);

TRAIN MODEL cb_model CHECKPOINT = 'v1.0'
    ON training_data
    WITH (
        nnodes = 2
    );

EXPORT MODEL cb_model CHECKPOINT = 'v1.0';

CREATE SERVICE cb_service
    ON MODEL cb_model
    CHECKPOINT = 'v1.0'
    WITH (
        replicas = 3,
        pod_cpu_cores = 4,
        pod_memory = '16Gi'
    );
```
