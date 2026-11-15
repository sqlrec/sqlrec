# GBDT Models

LightGBM and XGBoost accept numeric features; CatBoost also accepts categories. See [the lifecycle guide](../../guides/model-lifecycle.md) and [common resource settings](./builtin-models.md#common-lifecycle-and-resources).

## LightGBM Model

The LightGBM model is based on the GBDT (Gradient Boosting Decision Tree) framework, supporting the full train/export/serve lifecycle. Training data and artifacts use the configured storage; export produces ONNX format for online inference.

LightGBM training features are converted to float32 to match ONNX and serving precision. NaN missing values are retained; Infinity and float32 overflow are rejected. Models previously trained on double values require retraining to guarantee this precision consistency.

**Model name**: `gbdt.lightgbm`

GBDT TRAIN, EXPORT, and SERVICE may override runtime and resource settings, but cannot change the declared model type or `label_columns`. Explicitly repeating the declared value is allowed; a different value raises an error. Create a new model to change its type or label. This rule applies to LightGBM, XGBoost, and CatBoost.

GBDT serving converts missing numeric fields and `null` internally to float32 `NaN` for the model's missing-value handling, matching training; `0` remains a valid number. Clients should send `null` or omit the field; do not send `NaN` in JSON. Numeric strings must be complete, finite float32 decimal numbers; invalid values return HTTP 400. Services inherit model image, version and resource settings, with service parameters taking precedence. Traffic starts after model download/loading and a successful `/health` probe. This also applies to XGBoost and CatBoost.

`POST /predict` accepts a nonempty array of row objects or a column map. Column values must be arrays with matching lengths; length-one columns can broadcast. JSON parsing failures, non-object rows, non-array columns, incompatible column lengths, and empty batches return HTTP 400. Model runtime exceptions return HTTP 500. Error responses use JSON with an `error` field.

**Output fields**:

| Field | Type | Description |
|-------|------|-------------|
| `probs` | FLOAT | Positive-class probability for binary classification; predicted value for regression |

**Required parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `label_columns` | String | One label column, without commas or surrounding whitespace |

**Training parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `objective` | String | "binary" | Learning objective (binary, regression) |
| `metric` | String | "auc" | Evaluation metric (auc, logloss, rmse) |
| `num_iterations` | Integer | 300 | Number of boosting iterations |
| `learning_rate` | Double | 0.1 | Learning rate |
| `num_leaves` | Integer | 63 | Maximum leaves per tree |
| `max_depth` | Integer | 6 | Maximum tree depth |
| `feature_fraction` | Double | 0.8 | Fraction of features used per tree |
| `bagging_fraction` | Double | 0.8 | Fraction of data used per tree |
| `bagging_freq` | Integer | 5 | Bagging frequency |
| `min_data_in_leaf` | Integer | 20 | Minimum samples in a leaf |
| `l2_regularization` | Double | 1.0 | L2 regularization coefficient |

**Usage Example**:

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

## XGBoost Model

The XGBoost model is based on the GBDT (Gradient Boosting Decision Tree) framework, supporting the full train/export/serve lifecycle. Training data and model artifacts are stored on distributed storage; export produces ONNX format for online inference.

**Model name**: `gbdt.xgboost`

**Output fields**:

| Field | Type | Description |
|-------|------|-------------|
| `probs` | FLOAT | Positive-class probability for binary classification; predicted value for regression |

**Required parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `label_columns` | String | One label column, without commas or surrounding whitespace |

**Training parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `objective` | String | "binary" | Learning objective (binary, regression) |
| `metric` | String | "auc" | Evaluation metric (auc, logloss, rmse) |
| `num_iterations` | Integer | 300 | Number of boosting iterations |
| `learning_rate` | Double | 0.1 | Learning rate |
| `max_depth` | Integer | 6 | Maximum tree depth |
| `feature_fraction` | Double | 0.8 | Fraction of features used per tree (XGBoost colsample_bytree) |
| `bagging_fraction` | Double | 0.8 | Fraction of data used per tree (XGBoost subsample) |
| `min_child_weight` | Integer | 1 | Minimum sum of instance weight in a child |
| `l2_regularization` | Double | 1.0 | L2 regularization coefficient (XGBoost reg_lambda) |

**Usage Example**:

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

## CatBoost Model

The CatBoost model is based on the GBDT framework with native categorical feature handling. It supports the full train/export/serve lifecycle. Training data and artifacts use the configured storage; export produces native `.cbm` format for online inference.

**Model name**: `gbdt.catboost`

Training and serving convert integer categories to decimal strings and null categories to `""`, preserving large integer ID precision. Online categories must be integers, strings or `null`; floats, booleans and arrays return HTTP 400.

**Output fields**:

| Field | Type | Description |
|-------|------|-------------|
| `probs` | FLOAT | Positive-class probability for binary classification; predicted value for regression |

**Required parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `label_columns` | String | One label column, without commas or surrounding whitespace |

**Training parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `objective` | String | "binary" | Learning objective (binary, regression) |
| `metric` | String | "auc" | Evaluation metric (auc, logloss, rmse) |
| `cb_iterations` | Integer | 1000 | CatBoost iterations |
| `cb_depth` | Integer | 6 | CatBoost tree depth |
| `cb_l2_leaf_reg` | Double | 3.0 | L2 leaf regularization |
| `learning_rate` | Double | 0.1 | Learning rate |

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
    'model' = 'gbdt.catboost',
    'label_columns' = 'label',
    'cb_iterations' = '1000',
    'cb_depth' = '8',
    'learning_rate' = '0.03'
);
```
