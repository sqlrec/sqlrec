# Model Training and Online Inference

Choose a path based on what you already have:

- Existing HTTP model service: use `external` below, without SQLRec training, export, or inference-container deployment.
- Train and deploy with SQLRec: prepare the [service environment](../operations/deployment.md), then follow [the trained-model lifecycle](#lifecycle-for-a-trained-model).
- Hugging Face model: download a Hub snapshot and create a service directly; see [backend differences](#how-hugging-face-differs).

## Connect an Existing HTTP Model Service

Prepare an inference URL reachable from SQLRec and declare the service's input and output fields:

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

In local file mode, save these definitions separately as `model/external_rank_model.sql` and `service/external_rank_service.sql` under the complete `SQL_SCHEMA_DIR`, then restart. See [the Docker guide](../getting-started/docker.md#managing-local-sql-definitions) for loading files. In remote metadata mode, execute the definitions through Beeline or JDBC.

Prepare a cached `rank_input` table containing those fields. Declare the result schema with an empty table, then call the service:

```sql
CACHE TABLE external_rank_output AS
SELECT *, CAST(NULL AS FLOAT) AS score FROM rank_input LIMIT 0;

CACHE TABLE result_table AS
CALL call_service('external_rank_service', rank_input)
LIKE external_rank_output;
```

The result keeps the input columns and appends `score`. Connecting an existing service does not require a Kubernetes training environment; the inference service must implement the protocol below.

::: details HTTP Inference Protocol (Service Providers)
The service accepts POST requests. A single-table call sends an array of JSON objects containing only Model input fields:

```json
[
  {"user_id": 1, "item_id": 101, "category": "phone", "price": 3999.0},
  {"user_id": 1, "item_id": 102, "category": "tablet", "price": 2999.0}
]
```

SQL NULL values in existing input columns are sent as explicit JSON `null`, with the key retained. Columns absent from the input table do not produce keys. Services should handle nulls according to the model contract; see [built-in models](../reference/models/builtin-models.md) for TZRec, GBDT, and Hugging Face input requirements.

The response maps each output field to an array with the same row count and order as the input:

```json
{"score": [0.85, 0.72]}
```

For the User-Item call below, the request is column-oriented. User fields are single-element arrays; Item fields follow item row order:

```json
{
  "user_id": [1],
  "item_id": [101, 102],
  "category": ["phone", "tablet"],
  "price": [3999.0, 2999.0]
}
```
:::

## Choose a Backend

| Model type | Data source | TRAIN MODEL | EXPORT MODEL | Service checkpoint |
| --- | --- | --- | --- | --- |
| tzrec WideAndDeep / DeepFM / DSSM / MMoE / RocketLaunching | SQL table | Train | Required | export |
| LightGBM / XGBoost / CatBoost | SQL table | Train | Required | export |
| Hugging Face Transformers | Hub snapshot | Download, without an ON table | Unsupported | origin |
| external | Existing HTTP service | Unsupported | Unsupported | None |

See [Built-in Models](../reference/models/builtin-models.md) for options.

## Core Objects

| Object | Purpose |
|--------|---------|
| Model | Stores the model type, input fields, and default configuration |
| Checkpoint | A versioned result produced by training, downloading, or export |
| Service | Deploys a selected checkpoint as an online endpoint |

Checkpoint types are:

- `origin`: the original training or download result;
- `export`: an artifact produced by `EXPORT MODEL` for a tzrec or GBDT service.

Checkpoint names are user-defined version identifiers. TRAIN/EXPORT with an existing target name cleans up and recreates the old checkpoint after configuration validation, including succeeded versions. Running tasks are reused, and checkpoints referenced by services still cannot be deleted or overwritten. Incremental training must use a different source checkpoint, and EXPORT requires a succeeded origin checkpoint. Use a traceable name such as a date or release ID.

## Lifecycle for a Trained Model

Training, export, and self-hosted serving require the [full service environment](../operations/deployment.md), including Kubernetes and model storage. The Docker demo does not provide these components. Prepare an accessible `training_sample` table with fields compatible with the model before running the statements.

Prepare TZRec datasets according to the [sample data formats](../reference/models/builtin-models.md#tzrec-sample-data-formats). The training entry does not pre-scan training or evaluation labels; ensure label types, values, and positive/negative sample requirements when generating data.

### 1. Create the Model

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

Training data must contain the labels specified by `label_columns` and the model's feature columns, with names and types matching the model configuration. TZRec and GBDT online inference require only non-label features; label columns are not required.

### 2. Train an Origin Checkpoint

```sql
TRAIN MODEL rank_model CHECKPOINT = '2026_09_13'
ON training_sample
WHERE dt = '2026-09-13'
WITH (
  'num_epochs' = '1',
  'batch_size' = '8192'
);
```

`TRAIN MODEL` submits a Kubernetes training job. Successful submission does not mean training is finished. Check status before exporting:

```sql
SHOW CHECKPOINTS rank_model;

DESCRIBE FORMATTED MODEL rank_model
CHECKPOINT = '2026_09_13';
```

### 3. Export the Model

```sql
EXPORT MODEL rank_model CHECKPOINT = '2026_09_13'
ON training_sample;
```

The backend determines the export work. A normal single model produces an export checkpoint named `<origin_checkpoint>_export`. DSSM produces separate user- and item-tower artifacts; use `SHOW CHECKPOINTS` to get their exact names.

RocketLaunching produces one export checkpoint that serves only the light network and declares `probs_light FLOAT`. Prepare exposure pairs with both positive and negative rows before training. Network, feature, label, and distillation settings cannot change during fine-tuning, export, or service creation; see [RocketLaunching](../reference/models/builtin-models.md#rocket-launching).

### 4. Create an Online Service

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

Inspect the definition and status after creating the service:

```sql
SHOW SERVICES;

DESCRIBE FORMATTED SERVICE rank_service;
```

## Call a Model Service

`call_service` appends model outputs to the input data. Declare input fields in the Model and supply matching fields in the input table.

### Row-Oriented Input

```sql
CACHE TABLE rank_input AS
SELECT user_id, item_id, category, price FROM candidate_item;

CACHE TABLE rank_output AS
SELECT *, CAST(NULL AS FLOAT) AS probs FROM rank_input LIMIT 0;

CACHE TABLE ranked_item AS
CALL call_service('rank_service', rank_input) LIKE rank_output;
```

This Wide & Deep example keeps input columns and appends `probs`. See [Built-in Models](../reference/models/builtin-models.md) for other output fields.

### User-Item Input

For ranking, pass one user row and multiple item rows separately to avoid repeating user features:

```sql
CACHE TABLE item_rank_output AS
SELECT *, CAST(NULL AS FLOAT) AS probs FROM item_candidates LIMIT 0;

CACHE TABLE ranked_item AS
CALL call_service('rank_service', user_features, item_candidates)
LIKE item_rank_output;
```

- User must contain exactly one row; Item may contain many.
- Duplicate input field names are read from User, and remaining model fields from Item.
- The result keeps Item fields and appends model outputs.
- Empty Item input returns a correctly typed empty table without an HTTP request.

See [the HTTP inference protocol](#connect-an-existing-http-model-service) for external service request and response formats.

## How Hugging Face Differs

For the Hugging Face backend, `TRAIN MODEL` downloads a selected Hub revision instead of training from a SQL table, so it has no `ON data_source` clause:

```sql
TRAIN MODEL text_embedding_model CHECKPOINT = 'v1' WITH (
  'revision' = 'main'
);
```

The resulting `origin` checkpoint can be used directly by a Service, and `EXPORT MODEL` is not supported. See [Built-in Models](../reference/models/builtin-models.md) for the complete task and options.

## Troubleshooting

### Service creation reports the wrong checkpoint type

Use the type required by the backend: `export` for tzrec and GBDT, `origin` for Hugging Face, and no checkpoint for `external`.

### Training fields do not match

Compare `DESCRIBE MODEL` with the training table. Check field names and types, label columns, and backend-specific user/item feature settings.

### `call_service` returns the wrong row count

Make sure every output array has the same length as the input and that output field names match those defined by the model backend.
