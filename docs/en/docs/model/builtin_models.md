# Built-in Models

This document introduces SQLRec built-in model types and their usage.

## Built-in Model Types

SQLRec has three built-in model types:

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

### 3. DSSM Model

DSSM (Deep Structured Semantic Models) is a two-tower retrieval model implemented based on the tzrec framework, supporting complete training, export, and service deployment workflow.

**Model Name**: `tzrec.dssm`

**Features**:
- Supports two-tower architecture retrieval models
- User tower and item tower generate embedding vectors separately
- Supports distributed training (PyTorch Distributed)
- Supports Parquet format training data
- Automatically generates Kubernetes training and service YAML
- Supports sparse and dense features

**Output Fields**:

| Field Name | Type | Description |
|-----------|------|-------------|
| `user_tower_emb` | ARRAY\<FLOAT\> | User tower embedding vector |
| `item_tower_emb` | ARRAY\<FLOAT\> | Item tower embedding vector |

**Required Parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `user_features` | String | User feature column names, multiple features separated by commas |
| `item_features` | String | Item feature column names, multiple features separated by commas |

**Note**: At least one of `user_features` or `item_features` must be configured.

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
