# Built-in Models

This document introduces SQLRec built-in model types and their usage.

## Built-in Model Types

SQLRec has the following built-in model types:

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

### 4. LightGBM Model (Gradient Boosting Decision Tree)

The LightGBM model is based on the GBDT (Gradient Boosting Decision Tree) framework, supporting the full train/export/serve lifecycle. Training data and model artifacts are stored on HDFS; export produces ONNX format for online inference.

**Model name**: `gbdt.lightgbm`

**Features**:
- LightGBM-based gradient boosting decision tree
- Native categorical feature support
- LightGBM socket-based distributed training
- Parquet training data on distributed storage
- Model artifacts persisted to distributed storage
- Exports ONNX format for serving (via onnxmltools)
- C++ ONNX Runtime inference server

**Output fields**:

| Field | Type | Description |
|-------|------|-------------|
| `probs` | FLOAT | Predicted probability |

**Required parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `label_columns` | String | Label column name |

**Training parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `objective` | String | "binary" | Learning objective (binary, multiclass, regression) |
| `metric` | String | "auc" | Evaluation metric (auc, logloss, rmse) |
| `num_iterations` | Integer | 100 | Number of boosting iterations |
| `learning_rate` | Double | 0.1 | Learning rate |
| `num_leaves` | Integer | 63 | Maximum leaves per tree |
| `max_depth` | Integer | 6 | Maximum tree depth |
| `feature_fraction` | Double | 0.9 | Fraction of features used per tree |
| `bagging_fraction` | Double | 0.9 | Fraction of data used per tree |
| `bagging_freq` | Integer | 5 | Bagging frequency |
| `min_data_in_leaf` | Integer | 20 | Minimum samples in a leaf |
| `l2_regularization` | Double | 1.0 | L2 regularization coefficient |
| `categorical_features` | String | "" | Categorical feature names (comma separated) |

**Distributed training parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `nnodes` | Integer | 1 | Number of nodes (>1 enables LightGBM distributed mode) |
| `nproc_per_node` | Integer | 1 | Processes per node |
| `master_port` | Integer | 29500 | Distributed training master port |

**Resource parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `image` | String | "sqlrec/gbdt" | Docker image name |
| `version` | String | "0.1.0-cpu" | Docker image version |
| `pod_cpu_cores` | Integer | 1 | Pod CPU cores |
| `pod_memory` | String | "2Gi" | Pod memory |
| `pod_cpu_limit` | String | - | Pod CPU limit |
| `pod_memory_limit` | String | - | Pod memory limit |
| `replicas` | Integer | 1 | Service replica count |

**Usage Example**:

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

### 5. XGBoost Model (Gradient Boosting Decision Tree)

The XGBoost model is based on the GBDT (Gradient Boosting Decision Tree) framework, supporting the full train/export/serve lifecycle. Training data and model artifacts are stored on distributed storage; export produces ONNX format for online inference.

**Model name**: `gbdt.xgboost`

**Features**:
- XGBoost-based gradient boosting decision tree
- Float/double numerical features only (no categorical support)
- Parquet training data on distributed storage
- Model artifacts persisted to distributed storage
- Exports ONNX format for serving (via onnxmltools)
- C++ ONNX Runtime inference server

**Output fields**:

| Field | Type | Description |
|-------|------|-------------|
| `probs` | FLOAT | Predicted probability |

**Required parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `label_columns` | String | Label column name |

**Training parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `objective` | String | "binary" | Learning objective (binary, multiclass, regression) |
| `metric` | String | "auc" | Evaluation metric (auc, logloss, rmse) |
| `num_iterations` | Integer | 100 | Number of boosting iterations |
| `learning_rate` | Double | 0.1 | Learning rate |
| `max_depth` | Integer | 6 | Maximum tree depth |
| `feature_fraction` | Double | 0.9 | Fraction of features used per tree (XGBoost colsample_bytree) |
| `bagging_fraction` | Double | 0.9 | Fraction of data used per tree (XGBoost subsample) |
| `min_child_weight` | Integer | 1 | Minimum sum of instance weight in a child |
| `l2_regularization` | Double | 1.0 | L2 regularization coefficient (XGBoost reg_lambda) |

**Resource parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `image` | String | "sqlrec/gbdt" | Docker image name |
| `version` | String | "0.1.0-cpu" | Docker image version |
| `pod_cpu_cores` | Integer | 1 | Pod CPU cores |
| `pod_memory` | String | "2Gi" | Pod memory |
| `pod_cpu_limit` | String | - | Pod CPU limit |
| `pod_memory_limit` | String | - | Pod memory limit |
| `replicas` | Integer | 1 | Service replica count |

**Usage Example**:

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

### 6. CatBoost Model (Gradient Boosting Decision Tree)

The CatBoost model is based on the GBDT framework with native categorical feature handling. It supports the full train/export/serve lifecycle. Training data and model artifacts are stored on HDFS; export produces ONNX format for online inference.

**Model name**: `gbdt.catboost`

**Features**:
- CatBoost-based gradient boosting decision tree
- Native categorical feature handling (no manual encoding needed)
- CatBoost distributed training (`catboost run-worker` daemon + `fit_with_workers` Python API)
- Parquet training data on distributed storage
- Model artifacts persisted to distributed storage
- Exports native .cbm format for serving (loaded via CatBoost C API, supports categorical features)
- C++ CatBoost native inference server

**Output fields**:

| Field | Type | Description |
|-------|------|-------------|
| `probs` | FLOAT | Predicted probability |

**Required parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `label_columns` | String | Label column name |

**Training parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `objective` | String | "binary" | Learning objective (binary, multiclass, regression) |
| `metric` | String | "auc" | Evaluation metric (auc, logloss, rmse) |
| `cb_iterations` | Integer | 1000 | CatBoost iterations |
| `cb_depth` | Integer | 6 | CatBoost tree depth |
| `cb_l2_leaf_reg` | Double | 3.0 | L2 leaf regularization |
| `learning_rate` | Double | 0.1 | Learning rate |
| `categorical_features` | String | "" | Categorical feature names (comma separated) |

**Distributed training parameters**:

> When `nnodes > 1`, CatBoost distributed training is enabled: each worker pod starts a `catboost run-worker` daemon, and the master pod drives training via the `fit_with_workers()` API. Worker endpoints follow the Indexed-Job DNS convention `{job_name}-{i}.{service_name}:{master_port + i}`.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `nnodes` | Integer | 1 | Number of nodes (>1 enables CatBoost distributed training) |
| `nproc_per_node` | Integer | 1 | Processes per node |
| `master_port` | Integer | 29500 | Distributed training master port (each worker listens on master_port + node_rank) |

**Resource parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `image` | String | "sqlrec/gbdt" | Docker image name |
| `version` | String | "0.1.0-cpu" | Docker image version |
| `pod_cpu_cores` | Integer | 1 | Pod CPU cores |
| `pod_memory` | String | "2Gi" | Pod memory |
| `pod_cpu_limit` | String | - | Pod CPU limit |
| `pod_memory_limit` | String | - | Pod memory limit |
| `replicas` | Integer | 1 | Service replica count |

**Usage Example**:

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
