# 内置模型

本文档介绍 SQLRec 内置的模型类型及其使用方法。

## 内置模型类型

SQLRec 内置了两种模型类型：

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
