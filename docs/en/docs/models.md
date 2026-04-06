# SQLRec Model System

This document introduces the SQLRec model system architecture, built-in model types, and how to use and extend models.

## Overview

SQLRec provides a complete machine learning model management framework, supporting model creation, training, export, and service deployment. The model system uses a plugin design, implementing different model type extensions through the `ModelController` interface.

### Core Concepts

| Concept | Description |
|---------|-------------|
| **Model** | Machine learning model definition, including input fields, output fields, and configuration parameters |
| **Checkpoint** | State snapshot saved during model training, can be used for continued training or service deployment |
| **Export** | Optimize and convert trained Checkpoint, including model graph optimization, quantization, etc., generating model files suitable for inference, improving inference performance |
| **Service** | Deploy exported models as online inference services |
| **ModelController** | Model controller interface, defining core model behaviors |

### Model Lifecycle

```
Create Model → Train Model → Export Model → Deploy Service
   │          │          │          │
   │          │          │          └── Create Kubernetes Deployment
   │          │          └── Model optimization (graph optimization, quantization) generates inference model
   │          └── Create Kubernetes Job to execute training, save Checkpoint
   └── Define model structure and parameters
```

**Purpose of Model Export**:

Model export converts trained Checkpoints into formats suitable for online inference, mainly including the following optimizations:

1. **Model Graph Optimization**: Optimize the computation graph, such as constant folding, operator fusion, dead code elimination, etc., reducing computational overhead
2. **Model Quantization**: Convert floating-point models to low-precision models (like INT8), reducing model size and inference latency
3. **Format Conversion**: Convert training framework model formats to inference engine optimized formats (like TorchScript, ONNX, TensorRT, etc.)

Through export optimization, model inference performance can be significantly improved, reducing latency and resource consumption.

## ModelController Interface

`ModelController` is the core interface for all model controllers, defining model lifecycle management methods.

### Interface Definition

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

### Method Description

| Method | Description |
|--------|-------------|
| `getModelName()` | Return model type name, used to identify model type |
| `getOutputFields(ModelConfig)` | Return model output field list |
| `checkModel(ModelConfig)` | Check if model configuration is valid, return null if valid |
| `genModelTrainK8sYaml(ModelConfig, ModelTrainConf)` | Generate training task Kubernetes YAML |
| `getExportCheckpoints(ModelExportConf)` | Get exported checkpoint name list |
| `genModelExportK8sYaml(ModelConfig, ModelExportConf)` | Generate export task Kubernetes YAML |
| `getServiceUrl(ModelConfig, ServiceConfig)` | Get service access URL |
| `getServiceK8sYaml(ModelConfig, ServiceConfig)` | Generate service deployment Kubernetes YAML |

## Built-in Model Types

SQLRec has two built-in model types:

### 1. External Model

External models are used to interface with existing external model services and do not support training and export operations.

**Model Name**: `external`

**Features**:
- Connect to existing external model inference services
- Does not support training (`TRAIN MODEL`)
- Does not support export (`EXPORT MODEL`)
- Access services directly via URL

**Configuration Parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `url` | String | External model service URL address |
| `output_columns` | String | Output column definition, format: `name1:type1,name2:type2` |

**Usage Example**:

```sql
CREATE MODEL external_model WITH (
    model = 'external',
    url = 'http://external-service:8080/predict',
    output_columns = 'score:FLOAT,label:VARCHAR'
);

CREATE SERVICE external_service
    ON MODEL external_model;
```

### 2. Wide & Deep Model

Wide & Deep model is a recommendation model implemented based on the tzrec framework, supporting complete training, export, and service deployment workflow.

**Model Name**: `tzrec.wide_and_deep`

**Features**:
- Supports Wide & Deep architecture recommendation models
- Supports distributed training (PyTorch Distributed)
- Supports Parquet format training data
- Automatically generates Kubernetes training and service YAML
- Supports sparse and dense features

**Output Fields**:

| Field Name | Type | Description |
|-----------|------|-------------|
| `probs` | FLOAT | Predicted probability value |

**Training Configuration Parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `sparse_lr` | Double | 0.001 | Sparse feature learning rate |
| `dense_lr` | Double | 0.001 | Dense feature learning rate |
| `num_epochs` | Integer | 1 | Number of training epochs |
| `batch_size` | Integer | 8192 | Batch size |
| `num_workers` | Integer | 8 | Data loader worker process count |
| `embedding_dim` | Integer | 16 | Embedding dimension |
| `num_buckets` | Integer | 1000000 | Integer feature bucket count |
| `hidden_units` | String | "512,256,128" | Deep network hidden layer unit count |
| `label_columns` | String | - | Label column name |

**Distributed Training Parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `nnodes` | Integer | 1 | Training node count |
| `nproc_per_node` | Integer | 1 | Processes per node |
| `master_port` | Integer | 29500 | Distributed training master port |

**Resource Configuration Parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `image` | String | "sqlrec/tzrec" | Docker image name |
| `version` | String | "0.1.0-cpu" | Docker image version |
| `pod_cpu_cores` | Integer | 2 | Pod CPU core count |
| `pod_memory` | String | "8Gi" | Pod memory |
| `replicas` | Integer | 1 | Service replica count |

**Column-level Configuration Parameters**:

Can configure parameters separately for each feature column:

| Parameter Format | Description |
|-----------------|-------------|
| `column.{feature_name}.bucket_size` | Feature bucket count |
| `column.{feature_name}.embedding_dim` | Feature embedding dimension |

**Usage Example**:

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

## Kubernetes Integration

SQLRec model system is deeply integrated with Kubernetes, with training and service deployment running in Kubernetes clusters.

### Training Tasks

Training tasks are executed through Kubernetes Jobs, supporting distributed training:

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
│              Headless Service (communication)           │
└─────────────────────────────────────────────────────────┘
```

**Training Flow**:
1. Generate pipeline.config configuration file
2. Generate start.sh startup script
3. Create ConfigMap to store configuration and scripts
4. Create Headless Service for inter-Pod communication
5. Create Indexed Job to execute distributed training
6. Save Checkpoint after training completes (type is `origin`)

### Export Tasks

Export tasks optimize and convert trained Checkpoints, generating model files suitable for inference. Export tasks are also executed through Kubernetes Jobs:

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
│  │  Model optimization (graph optimization,         │   │
│  │  quantization, format conversion)                │   │
│  └─────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

**Export Flow**:
1. Read trained Checkpoint (type is `origin`)
2. Generate export configuration file
3. Create Kubernetes Job to execute export
4. Execute model optimization (graph optimization, quantization, format conversion)
5. Save exported Checkpoint (type is `export`)

**Important Note**:

Service deployment **can only use exported models**, not directly use trained models. Reasons:

1. **Performance Optimization**: Export process performs graph optimization, quantization, etc., significantly improving inference performance
2. **Format Compatibility**: Exported model formats are more suitable for inference engine loading
3. **Check Mechanism**: System verifies Checkpoint type must be `export` when creating service

```java
// Validation logic in ServiceManager.java
if (!Consts.CHECKPOINT_TYPE_EXPORT.equals(checkpoint.getCheckpointType())) {
    throw new IllegalArgumentException("service only supports export checkpoint");
}
```

**Checkpoint Types**:

| Type | Description | Usage |
|------|-------------|-------|
| `origin` | Original checkpoint from training | Continue training, export optimization |
| `export` | Exported optimized checkpoint | Deploy service |

**Complete Flow Example**:

```sql
-- 1. Train model, generate origin type Checkpoint
TRAIN MODEL rec_model CHECKPOINT = 'v1.0' ON training_data;

-- 2. Export model, generate export type Checkpoint (like v1.0_export)
EXPORT MODEL rec_model CHECKPOINT = 'v1.0' ON training_data;

-- 3. Create service using exported Checkpoint
CREATE SERVICE rec_service
    ON MODEL rec_model
    CHECKPOINT = 'v1.0_export';
```

### Service Deployment

Model services are deployed through Kubernetes Deployment:

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

**Service URL Format**:
```
http://{service_id}.{namespace}.svc.cluster.local:80/predict
```

## General Configuration Parameters

The `ModelConfigs` class defines general configuration parameters for the model system:

| Parameter | Description |
|-----------|-------------|
| `MODEL` | Model type name |
| `MODEL_BASE_PATH` | Model base path, default `/user/sqlrec/models` |
| `MODEL_PATH` | Model path |
| `JAVA_HOME` | Java home directory |
| `HADOOP_HOME` | Hadoop home directory |
| `CLASSPATH` | Class path |
| `HADOOP_CONF_DIR` | Hadoop configuration directory |
| `CLIENT_DIR` | Client directory |
| `CLIENT_PV_NAME` | Client persistent volume name |
| `CLIENT_PVC_NAME` | Client persistent volume claim name |
| `NAMESPACE` | Kubernetes namespace |

## Built-in Model Call UDF

SQLRec provides two built-in UDFs (User Defined Functions) for calling model services for inference.

### call_service

`call_service` is a basic service call function used to send input data to model services and get prediction results.

**Function Signature**:

```java
public CacheTable eval(ExecuteContext context, String serviceName, CacheTable input)
```

**Parameter Description**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `context` | ExecuteContext | Execution context (auto-injected) |
| `serviceName` | String | Service name |
| `input` | CacheTable | Input data table |

**Return Value**: Returns a new `CacheTable` containing original input columns and model output columns.

**Usage Example**:

```sql
-- Create model
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

-- Train model
TRAIN MODEL test_model CHECKPOINT = 'test' ON behavior_sample;

-- Export model
EXPORT MODEL test_model CHECKPOINT = 'test' ON behavior_sample;

-- Create service
CREATE SERVICE test_service ON MODEL test_model CHECKPOINT = 'test_export';

-- Prepare input data
CACHE TABLE t1 AS
SELECT
    1 AS user_id,
    'Zhang' AS user_name,
    'China' AS user_country,
    28 AS user_age,
    2 AS item_id,
    'Smart Watch' AS item_name;

-- Call service for prediction
CALL call_service('test_service', t1);
```

### call_service_with_qv

`call_service_with_qv` is a service call function with Query-Value mode, suitable for recommendation system scenarios. It divides input into Query (query features, single row) and Value (candidate features, multiple rows), used for batch prediction of multiple candidates.

**Function Signature**:

```java
public CacheTable eval(ExecuteContext context, String serviceName, CacheTable query, CacheTable value)
```

**Parameter Description**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `context` | ExecuteContext | Execution context (auto-injected) |
| `serviceName` | String | Service name |
| `query` | CacheTable | Query feature table, must have only one row |
| `value` | CacheTable | Candidate feature table, can have multiple rows |

**Return Value**: Returns a new `CacheTable` containing Value table columns and model output columns.

**Use Cases**:
- In recommendation systems, Query table contains user features, Value table contains multiple candidate item features
- One request predicts user preference scores for all candidate items

**Usage Example**:

```sql
-- User features (Query, single row)
CACHE TABLE user_query AS
SELECT
    1001 AS user_id,
    'Alice' AS user_name,
    'USA' AS user_country,
    25 AS user_age;

-- Candidate item features (Value, multiple rows)
CACHE TABLE item_candidates AS
SELECT item_id, item_name, item_category
FROM items
WHERE category = 'Electronics'
LIMIT 100;

-- Batch predict user preference for all candidate items
CALL call_service_with_qv('rec_service', user_query, item_candidates);
```

## Service Call Data Protocol

Model service calls follow a specific data protocol to ensure correct communication between client and server.

### HTTP Request Format

**Request Method**: POST

**Request Headers**:
```
Content-Type: application/json; charset=utf-8
Accept: application/json
```

**Timeout Configuration**:
- Connect timeout: 30 seconds
- Read timeout: 30 seconds
- Write timeout: 30 seconds

### Input Data Format

#### Row-wise JSON Format (call_service)

`call_service` uses row-wise JSON array format to send data:

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

#### Column-wise JSON Format (call_service_with_qv)

`call_service_with_qv` uses column-wise JSON format, combining Query and Value data:

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

**Format Description**:
- Query table field values are copied and extended to match Value table row count
- Value table fields keep original values
- All fields stored column-wise, each field corresponds to an array

### Output Data Format

Prediction results returned by service are in JSON object format:

```json
{
    "probs": [0.85, 0.72, 0.91]
}
```

**Format Description**:
- Returns a JSON object
- Each output field corresponds to a key
- Value is prediction result array, array length matches input row count
- Field names defined by `ModelController.getOutputFields()`

### Data Merge Logic

UDF merges input data with prediction results:

1. **call_service**: Appends prediction results to the end of input rows
2. **call_service_with_qv**: Appends prediction results to the end of Value table rows

**Merge Example**:

Input data:
```
| user_id | item_id |
|---------|---------|
| 1       | 100     |
| 2       | 200     |
```

Prediction results:
```json
{"probs": [0.85, 0.72]}
```

Merged output:
```
| user_id | item_id | probs |
|---------|---------|-------|
| 1       | 100     | 0.85  |
| 2       | 200     | 0.72  |
```

### Error Handling

Errors that may occur during service calls:

| Error Type | Description |
|-----------|-------------|
| Service not exist | Service doesn't exist or format error |
| Service url is empty | Service URL is empty |
| Model controller not exist | Model controller doesn't exist |
| HTTP request failed | HTTP request failed (non-2xx status code) |
| Failed to call prediction service | Network or I/O error |

## Extending Custom Models

You can extend custom models by implementing the `ModelController` interface.

### Implementation Steps

1. **Create Model Configuration Class** (optional)

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

2. **Implement ModelController Interface**

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

3. **Register Model Controller (SPI method)**

Create file `com.sqlrec.common.model.ModelController` in `src/main/resources/META-INF/services/` directory, file content is the fully qualified name of the implementation class:

```
com.example.MyModel
```

### Using Custom Models

```sql
CREATE MODEL my_custom_model (
    feature1 VARCHAR,
    feature2 DOUBLE
) WITH (
    model = 'my_model',
    custom_param = 'value'
);
```
