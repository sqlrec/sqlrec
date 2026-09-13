# Model Training and Online Inference

SQLRec uses SQL to manage model definitions, training results, and online inference services. The exact lifecycle depends on the backend:

| Model type | Data source | Train | Export | Checkpoint used by Service |
|------------|-------------|-------|--------|----------------------------|
| tzrec Wide & Deep / DSSM | SQL table | Yes | Yes | `export` |
| LightGBM / XGBoost / CatBoost | SQL table | Yes | Yes | `export` |
| Hugging Face Transformers | Hugging Face Hub snapshot | Yes, without an `ON` table | No | `origin` |
| `external` | Existing HTTP service | No | No | None |

See [Built-in Models](../reference/models/builtin-models.md) for backend-specific options.

## Core Objects

| Object | Purpose |
|--------|---------|
| Model | Stores the model type, input fields, and default configuration |
| Checkpoint | A versioned result produced by training, downloading, or export |
| Service | Deploys a selected checkpoint as an online endpoint |

Checkpoint types are:

- `origin`: the original training or download result;
- `export`: an artifact produced by `EXPORT MODEL` for a tzrec or GBDT service.

Checkpoint names are user-defined version identifiers. Use a traceable value such as a date or release ID, and do not reuse a name for different data or settings.

## Lifecycle for a Trained Model

### 1. Create the Model

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

The field list is the model's input contract. Matching columns in training and inference data must have compatible names and types.

### 2. Train an Origin Checkpoint

```sql
TRAIN MODEL rank_model CHECKPOINT = '2026_09_13'
ON training_sample
WHERE dt = '2026-09-13'
WITH (
  num_epochs = 1,
  batch_size = 8192
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

### 4. Create an Online Service

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

Inspect the definition and status after creating the service:

```sql
SHOW SERVICES;

DESCRIBE FORMATTED SERVICE rank_service;
```

## Call a Model Service

The built-in `call_service` table function finds the Service, builds the HTTP request, and appends model outputs to the input rows.

### Row-Oriented Input

```sql
CACHE TABLE rank_input AS
SELECT user_id, item_id, category, price
FROM candidate_item;

CACHE TABLE ranked_item AS
CALL call_service('rank_service', rank_input);
```

Only fields present in the model definition are sent. The result keeps all input columns and appends the model output columns.

The request sent to the service is an array of JSON objects:

```json
[
  {"user_id": 1, "item_id": 101, "category": "phone", "price": 3999.0},
  {"user_id": 1, "item_id": 102, "category": "tablet", "price": 2999.0}
]
```

The service returns a JSON object whose keys are output fields and whose values are arrays:

```json
{"probs": [0.85, 0.72]}
```

Each output array must have the same length as the input.

### User-Item Input

For ranking, pass one user row and multiple candidate rows separately to avoid repeating user features in the request:

```sql
CACHE TABLE ranked_item AS
CALL call_service('rank_service', user_features, item_candidates);
```

In this form:

- the User table must contain exactly one row; the Item table may contain many;
- duplicate field names are read from User, and other model fields are read from Item;
- the result keeps Item fields and appends model outputs;
- an empty Item table returns a correctly typed empty result without an HTTP request.

The request uses column-oriented JSON:

```json
{
  "user_id": [1],
  "item_id": [101, 102],
  "category": ["phone", "tablet"],
  "price": [3999.0, 2999.0]
}
```

## Connect an Existing HTTP Model Service

Use an `external` model for an existing service that implements the request and response protocol above. It requires neither `TRAIN MODEL` nor `EXPORT MODEL`:

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

Call it like any other Service:

```sql
CACHE TABLE result AS
CALL call_service('external_rank_service', rank_input);
```

## How Hugging Face Differs

For the Hugging Face backend, `TRAIN MODEL` downloads a selected Hub revision instead of training from a SQL table, so it has no `ON data_source` clause:

```sql
TRAIN MODEL text_embedding_model CHECKPOINT = 'v1' WITH (
  revision = 'main'
);
```

The resulting `origin` checkpoint can be used directly by a Service, and `EXPORT MODEL` is not supported. See [Built-in Models](../reference/models/builtin-models.md) for the complete task and options.

## Prerequisites

Training, export, and Service deployment require Kubernetes and configured model storage. tzrec and GBDT jobs must also be able to reach the SQL data source. See [Service Deployment](../operations/deployment.md).

## Troubleshooting

### Service creation reports the wrong checkpoint type

Use the type required by the backend: `export` for tzrec and GBDT, `origin` for Hugging Face, and no checkpoint for `external`.

### Training fields do not match

Compare `SHOW CREATE MODEL` with the training table. Check field names and types, label columns, and backend-specific user/item feature settings.

### `call_service` returns the wrong row count

Make sure every output array has the same length as the input and that output field names match those defined by the model backend.
