# Built-in Models

Choose a backend based on your inputs and task:

| Backend | Typical use | Inputs | Outputs |
| --- | --- | --- | --- |
| [TZRec](./tzrec.md) | Retrieval, ranking, and multi-target prediction | Categorical and numeric features | Probabilities, predictions, or tower embeddings |
| [GBDT](./gbdt.md) | Tree-based ranking and regression | Numeric features; CatBoost also supports categories | `probs` |
| [Hugging Face](./huggingface.md) | Text classification, generation, and text/image embeddings | Text or image URLs | Labels, text, or embeddings |
| [external](#external-model) | Connect an existing HTTP inference service | Fields declared by the service | Configured output fields |

For creation, training, export, and serving steps, see [Model Training and Online Inference](../../guides/model-lifecycle.md).

## Common Lifecycle and Resources

Model, training, export, and Service `WITH` options use string keys and values, such as `'batch_size' = '8192'`. SQLRec converts values to the declared option types.

For trained models, follow [the model guide](../../guides/model-lifecycle.md#lifecycle-for-a-trained-model) to create, train, export, and deploy a service. Trained-model examples focus on Model definitions; prepare training tables such as `training_sample` separately. TZRec model structure belongs to MODEL, training settings to TRAIN, and export/service resources to their respective operations.

These resource settings apply to self-hosted tzrec, GBDT, and Hugging Face jobs or services. `replicas` applies to online services.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `pod_cpu_cores` | Integer | 1 | Pod CPU core count |
| `pod_memory` | String | "2Gi" | Pod memory |
| `pod_cpu_limit` | String | - | Pod CPU limit |
| `pod_memory_limit` | String | - | Pod memory limit |
| `replicas` | Integer | 1 | Service replica count |

Default images are `sqlrec/tzrec` for tzrec and `sqlrec/gbdt` for GBDT, with version `${SQLREC_VERSION}-cpu`. Hugging Face uses `sqlrec/transformers:${SQLREC_VERSION}`. Override them with `image` and `version`.

## External Model

External models are used to interface with existing external model services and do not support training and export operations.

**Model Name**: `external`

**Configuration Parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `output_columns` | String | Output column definition, format: `name1:type1,name2:type2` |

`output_columns` is a Model option. `url` is a Service option that specifies the inference endpoint.

**Usage Example**:

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

CREATE SERVICE `external-service`
    ON MODEL external_model
    WITH (
        'url' = 'http://external-service:8080/predict'
    );
```
