# SQLRec 模型系统

本文档介绍 SQLRec 的模型系统架构、内置模型类型以及如何使用和扩展模型。

## 概述

SQLRec 提供了一套完整的机器学习模型管理框架，支持模型的创建、训练、导出和服务部署。模型系统采用插件化设计，通过 `ModelController` 接口实现不同类型模型的扩展。

### 核心概念

| 概念 | 说明 |
|------|------|
| **Model（模型）** | 机器学习模型的定义，包含输入字段、输出字段和配置参数 |
| **Checkpoint（检查点）** | 模型训练过程中保存的状态快照，可用于继续训练或部署服务 |
| **Export（导出）** | 将训练好的 Checkpoint 进行优化转换，包括模型切图、量化等操作，生成适合推理的模型文件，提升推理性能 |
| **Service（服务）** | 将导出后的模型部署为在线推理服务 |
| **ModelController** | 模型控制器接口，定义模型的核心行为 |

### 模型生命周期

```
创建模型 → 训练模型 → 导出模型 → 部署服务
   │          │          │          │
   │          │          │          └── 创建 Kubernetes Deployment
   │          │          └── 模型优化（切图、量化）生成推理模型
   │          └── 创建 Kubernetes Job 执行训练，保存 Checkpoint
   └── 定义模型结构和参数
```

**导出模型的作用**：

模型导出是将训练好的 Checkpoint 转换为适合在线推理的格式，主要包含以下优化：

1. **模型切图（Graph Optimization）**：对计算图进行优化，如常量折叠、算子融合、死代码消除等，减少计算开销
2. **模型量化（Quantization）**：将浮点模型转换为低精度模型（如 INT8），减少模型大小和推理延迟
3. **格式转换**：将训练框架的模型格式转换为推理引擎优化格式（如 TorchScript、ONNX、TensorRT 等）

通过导出优化，可以显著提升模型的推理性能，降低延迟和资源消耗。

## ModelController 接口

`ModelController` 是所有模型控制器的核心接口，定义了模型的生命周期管理方法。

### 接口定义

```java
public interface ModelController {
    String getModelName();
    List<FieldSchema> getOutputFields(ModelConfig model);
    String checkModel(ModelConfig model);
    String genModelTrainK8sYaml(ModelConfig model, ModelTrainConf trainConf);
    List<String> getExportCheckpoints(ModelExportConf exportConf);
    String genModelExportK8sYaml(ModelConfig model, ModelExportConf exportConf);
    String getServiceUrl(ModelConfig model, ServiceConfig serviceConf);
    String getServiceK8sYaml(ModelConfig model, ServiceConfig serviceConf);
}
```

### 方法说明

| 方法 | 说明 |
|------|------|
| `getModelName()` | 返回模型类型名称，用于标识模型类型 |
| `getOutputFields(ModelConfig)` | 返回模型的输出字段列表 |
| `checkModel(ModelConfig)` | 检查模型配置是否有效，返回 null 表示有效 |
| `genModelTrainK8sYaml(ModelConfig, ModelTrainConf)` | 生成训练任务的 Kubernetes YAML |
| `getExportCheckpoints(ModelExportConf)` | 获取导出后的检查点名称列表 |
| `genModelExportK8sYaml(ModelConfig, ModelExportConf)` | 生成导出任务的 Kubernetes YAML |
| `getServiceUrl(ModelConfig, ServiceConfig)` | 获取服务的访问 URL |
| `getServiceK8sYaml(ModelConfig, ServiceConfig)` | 生成服务部署的 Kubernetes YAML |

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

## Kubernetes 集成

SQLRec 模型系统与 Kubernetes 深度集成，训练和服务部署都在 Kubernetes 集群中运行。

### 训练任务

训练任务通过 Kubernetes Job 执行，支持分布式训练：

```
┌─────────────────────────────────────────────────────────┐
│                    Kubernetes Job                        │
├─────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐     │
│  │   Pod 0     │  │   Pod 1     │  │   Pod N     │     │
│  │  (Master)   │  │  (Worker)   │  │  (Worker)   │     │
│  │             │  │             │  │             │     │
│  │  torchrun   │  │  torchrun   │  │  torchrun   │     │
│  │     ↓       │  │     ↓       │  │     ↓       │     │
│  │  tzrec      │  │  tzrec      │  │  tzrec      │     │
│  └─────────────┘  └─────────────┘  └─────────────┘     │
│         ↑                ↑                ↑             │
│         └────────────────┴────────────────┘             │
│              Headless Service (通信)                     │
└─────────────────────────────────────────────────────────┘
```

**训练流程**：
1. 生成 pipeline.config 配置文件
2. 生成 start.sh 启动脚本
3. 创建 ConfigMap 存储配置和脚本
4. 创建 Headless Service 用于 Pod 间通信
5. 创建 Indexed Job 执行分布式训练
6. 训练完成后保存 Checkpoint（类型为 `origin`）

### 导出任务

导出任务将训练好的 Checkpoint 进行优化转换，生成适合推理的模型文件。导出任务同样通过 Kubernetes Job 执行：

```
┌─────────────────────────────────────────────────────────┐
│                    Kubernetes Job                        │
├─────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────────────────────┐   │
│  │                     Pod                          │   │
│  │                                                  │   │
│  │  torchrun                                        │   │
│  │     ↓                                            │   │
│  │  tzrec export                                    │   │
│  │     ↓                                            │   │
│  │  模型优化（切图、量化、格式转换）                    │   │
│  └─────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

**导出流程**：
1. 读取训练好的 Checkpoint（类型为 `origin`）
2. 生成导出配置文件
3. 创建 Kubernetes Job 执行导出
4. 执行模型优化（切图、量化、格式转换）
5. 保存导出后的 Checkpoint（类型为 `export`）

**重要说明**：

服务部署**只能使用导出后的模型**，不能直接使用训练后的模型。原因如下：

1. **性能优化**：导出过程会对模型进行切图、量化等优化，显著提升推理性能
2. **格式兼容**：导出后的模型格式更适合推理引擎加载
3. **检查机制**：系统在创建服务时会验证 Checkpoint 类型必须为 `export`

```java
// ServiceManager.java 中的验证逻辑
if (!Consts.CHECKPOINT_TYPE_EXPORT.equals(checkpoint.getCheckpointType())) {
    throw new IllegalArgumentException("service only supports export checkpoint");
}
```

**Checkpoint 类型**：

| 类型 | 说明 | 用途 |
|------|------|------|
| `origin` | 训练产生的原始检查点 | 继续训练、导出优化 |
| `export` | 导出优化后的检查点 | 部署服务 |

**完整流程示例**：

```sql
-- 1. 训练模型，生成 origin 类型的 Checkpoint
TRAIN MODEL rec_model CHECKPOINT = 'v1.0' ON training_data;

-- 2. 导出模型，生成 export 类型的 Checkpoint（如 v1.0_export）
EXPORT MODEL rec_model CHECKPOINT = 'v1.0' ON training_data;

-- 3. 使用导出的 Checkpoint 创建服务
CREATE SERVICE rec_service
    ON MODEL rec_model
    CHECKPOINT = 'v1.0_export';
```

### 服务部署

模型服务通过 Kubernetes Deployment 部署：

```
┌─────────────────────────────────────────────────────────┐
│                 Kubernetes Deployment                    │
├─────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐     │
│  │   Pod 1     │  │   Pod 2     │  │   Pod 3     │     │
│  │             │  │             │  │             │     │
│  │  tzrec      │  │  tzrec      │  │  tzrec      │     │
│  │  server     │  │  server     │  │  server     │     │
│  │  :80        │  │  :80        │  │  :80        │     │
│  └─────────────┘  └─────────────┘  └─────────────┘     │
│         ↑                ↑                ↑             │
│         └────────────────┴────────────────┘             │
│                   Kubernetes Service                     │
│                      (LoadBalancer)                      │
└─────────────────────────────────────────────────────────┘
```

**服务 URL 格式**：
```
http://{service_id}.{namespace}.svc.cluster.local:80/predict
```

## 通用配置参数

`ModelConfigs` 类定义了模型系统的通用配置参数：

| 参数 | 说明 |
|------|------|
| `MODEL` | 模型类型名称 |
| `MODEL_BASE_PATH` | 模型基础路径，默认 `/user/sqlrec/models` |
| `MODEL_PATH` | 模型路径 |
| `JAVA_HOME` | Java 主目录 |
| `HADOOP_HOME` | Hadoop 主目录 |
| `CLASSPATH` | 类路径 |
| `HADOOP_CONF_DIR` | Hadoop 配置目录 |
| `CLIENT_DIR` | 客户端目录 |
| `CLIENT_PV_NAME` | 客户端持久卷名称 |
| `CLIENT_PVC_NAME` | 客户端持久卷声明名称 |
| `NAMESPACE` | Kubernetes 命名空间 |

## 内置模型调用 UDF

SQLRec 提供了两个内置的 UDF（用户定义函数）用于调用模型服务进行推理。

### call_service

`call_service` 是基本的服务调用函数，用于将输入数据发送到模型服务并获取预测结果。

**函数签名**：

```java
public CacheTable eval(ExecuteContext context, String serviceName, CacheTable input)
```

**参数说明**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `context` | ExecuteContext | 执行上下文（自动注入） |
| `serviceName` | String | 服务名称 |
| `input` | CacheTable | 输入数据表 |

**返回值**：返回一个新的 `CacheTable`，包含原始输入列和模型输出列。

**使用示例**：

```sql
-- 创建模型
CREATE MODEL test_model (
    user_id BIGINT,
    user_name STRING,
    user_country STRING,
    user_age INT,
    item_id BIGINT,
    item_name STRING
) WITH (
    model = 'tzrec.wide_and_deep',
    label_columns = 'is_click'
);

-- 训练模型
TRAIN MODEL test_model CHECKPOINT = 'test' ON behavior_sample;

-- 导出模型
EXPORT MODEL test_model CHECKPOINT = 'test' ON behavior_sample;

-- 创建服务
CREATE SERVICE test_service ON MODEL test_model CHECKPOINT = 'test_export';

-- 准备输入数据
CACHE TABLE t1 AS
SELECT
    1 AS user_id,
    'Zhang' AS user_name,
    'China' AS user_country,
    28 AS user_age,
    2 AS item_id,
    'Smart Watch' AS item_name;

-- 调用服务进行预测
CALL call_service('test_service', t1);
```

### call_service_with_qv

`call_service_with_qv` 是带 Query-Value 模式的服务调用函数，适用于推荐系统场景。它将输入分为 Query（查询特征，单行）和 Value（候选特征，多行），用于批量预测多个候选项。

**函数签名**：

```java
public CacheTable eval(ExecuteContext context, String serviceName, CacheTable query, CacheTable value)
```

**参数说明**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `context` | ExecuteContext | 执行上下文（自动注入） |
| `serviceName` | String | 服务名称 |
| `query` | CacheTable | 查询特征表，必须只有一行 |
| `value` | CacheTable | 候选特征表，可以有多行 |

**返回值**：返回一个新的 `CacheTable`，包含 Value 表的列和模型输出列。

**使用场景**：
- 推荐系统中，Query 表包含用户特征，Value 表包含多个候选物品特征
- 一次请求预测用户对所有候选物品的偏好分数

**使用示例**：

```sql
-- 用户特征（Query，单行）
CACHE TABLE user_query AS
SELECT
    1001 AS user_id,
    'Alice' AS user_name,
    'USA' AS user_country,
    25 AS user_age;

-- 候选物品特征（Value，多行）
CACHE TABLE item_candidates AS
SELECT item_id, item_name, item_category
FROM items
WHERE category = 'Electronics'
LIMIT 100;

-- 批量预测用户对所有候选物品的偏好
CALL call_service_with_qv('rec_service', user_query, item_candidates);
```

## 服务调用数据协议

模型服务调用遵循特定的数据协议，确保客户端和服务端之间的正确通信。

### HTTP 请求格式

**请求方法**：POST

**请求头**：
```
Content-Type: application/json; charset=utf-8
Accept: application/json
```

**超时配置**：
- 连接超时：30 秒
- 读取超时：30 秒
- 写入超时：30 秒

### 输入数据格式

#### 行式 JSON 格式（call_service）

`call_service` 使用行式 JSON 数组格式发送数据：

```json
[
    {
        "user_id": 1,
        "user_name": "Zhang",
        "user_country": "China",
        "user_age": 28,
        "item_id": 2,
        "item_name": "Smart Watch"
    },
    {
        "user_id": 2,
        "user_name": "Li",
        "user_country": "USA",
        "user_age": 30,
        "item_id": 3,
        "item_name": "Phone"
    }
]
```

#### 列式 JSON 格式（call_service_with_qv）

`call_service_with_qv` 使用列式 JSON 格式，将 Query 和 Value 数据组合发送：

```json
{
    "user_id": [1001, 1001, 1001],
    "user_name": ["Alice", "Alice", "Alice"],
    "user_country": ["USA", "USA", "USA"],
    "user_age": [25, 25, 25],
    "item_id": [1, 2, 3],
    "item_name": ["Phone", "Tablet", "Laptop"],
    "item_category": ["Electronics", "Electronics", "Electronics"]
}
```

**格式说明**：
- Query 表的字段值会复制扩展到与 Value 表行数相同
- Value 表的字段保持原值
- 所有字段以列式存储，每个字段对应一个数组

### 输出数据格式

服务返回的预测结果为 JSON 对象格式：

```json
{
    "probs": [0.85, 0.72, 0.91]
}
```

**格式说明**：
- 返回一个 JSON 对象
- 每个输出字段对应一个键
- 值为预测结果数组，数组长度与输入行数相同
- 字段名由 `ModelController.getOutputFields()` 定义

### 数据合并逻辑

UDF 会将输入数据与预测结果合并：

1. **call_service**：将预测结果追加到输入行的末尾
2. **call_service_with_qv**：将预测结果追加到 Value 表行的末尾

**合并示例**：

输入数据：
```
| user_id | item_id |
|---------|---------|
| 1       | 100     |
| 2       | 200     |
```

预测结果：
```json
{"probs": [0.85, 0.72]}
```

合并后输出：
```
| user_id | item_id | probs |
|---------|---------|-------|
| 1       | 100     | 0.85  |
| 2       | 200     | 0.72  |
```

### 错误处理

服务调用过程中可能出现的错误：

| 错误类型 | 说明 |
|----------|------|
| Service not exist | 服务不存在或格式错误 |
| Service url is empty | 服务 URL 为空 |
| Model controller not exist | 模型控制器不存在 |
| HTTP request failed | HTTP 请求失败（返回非 2xx 状态码） |
| Failed to call prediction service | 网络或 I/O 错误 |

## 扩展自定义模型

可以通过实现 `ModelController` 接口来扩展自定义模型。

### 实现步骤

1. **创建模型配置类**（可选）

```java
public class MyModelConfig {
    public static final ConfigOption<String> CUSTOM_PARAM = new ConfigOption<>(
        "custom_param",
        "default_value", 
        "Custom parameter description", 
        null, 
        String.class
    );
}
```

2. **实现 ModelController 接口**

```java
public class MyModel implements ModelController {
    
    @Override
    public String getModelName() {
        return "my_model";
    }
    
    @Override
    public List<FieldSchema> getOutputFields(ModelConfig model) {
        return Arrays.asList(
            new FieldSchema("prediction", "DOUBLE"),
            new FieldSchema("confidence", "DOUBLE")
        );
    }
    
    @Override
    public String checkModel(ModelConfig model) {
        return null;
    }
    
    @Override
    public String genModelTrainK8sYaml(ModelConfig model, ModelTrainConf trainConf) {
        return "...";
    }
    
    @Override
    public List<String> getExportCheckpoints(ModelExportConf exportConf) {
        return Arrays.asList(exportConf.getCheckpointName() + "_export");
    }
    
    @Override
    public String genModelExportK8sYaml(ModelConfig model, ModelExportConf exportConf) {
        return "...";
    }
    
    @Override
    public String getServiceUrl(ModelConfig model, ServiceConfig serviceConf) {
        return "http://" + serviceConf.getId() + ".svc.cluster.local:80/predict";
    }
    
    @Override
    public String getServiceK8sYaml(ModelConfig model, ServiceConfig serviceConf) {
        return "...";
    }
}
```

3. **注册模型控制器**

```java
ModelControllerRegistry.register(new MyModel());
```

### 使用自定义模型

```sql
CREATE MODEL my_custom_model (
    feature1 VARCHAR,
    feature2 DOUBLE
) WITH (
    model = 'my_model',
    custom_param = 'value'
);
```
