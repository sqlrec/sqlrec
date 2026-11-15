# TZRec Models

Model choices: WideAndDeep and DeepFM for ranking, DSSM for retrieval, MMoE for multiple targets, and RocketLaunching for pre-ranking. See [the lifecycle guide](../../guides/model-lifecycle.md) for execution steps and [common resources](./builtin-models.md#common-lifecycle-and-resources) for pod settings.

## TZRec configuration stages

| Location | Options |
| --- | --- |
| CREATE MODEL WITH | Model type/path, fields/labels, feature encoding, networks, tower groups and MMoE task types/dimensions; optional training and resource defaults |
| TRAIN MODEL WITH | Training budget/optimizers, loading, evaluation input, sampling, task weights/metrics, observation/checkpoints and topology |
| EXPORT MODEL WITH | `batch_size` (default 8192) and export resources; fixed to one process and one loading worker |
| CREATE SERVICE WITH | `replicas`, inference environment and serving resources |

Training and resource options use: operation `WITH` > Model `WITH` > built-in default. Network structure, features, and labels belong in `CREATE MODEL`; changing them requires a new model. TRAIN, EXPORT, and SERVICE may override the defaults relevant to that operation.

Common options may be declared in MODEL and overridden in TRAIN:

| Option | Default | Meaning |
| --- | --- | --- |
| `batch_size` | 8192 | Positive samples per rank; global batch is approximately batch × nodes × processes |
| `num_workers` | 8 | Data readers per rank; 0 reads in the training process |
| `num_epochs` | 1 | New-run epoch budget; cumulative target on same-name recovery |
| `sparse_lr` / `dense_lr` | 0.001 / 0.001 | Adagrad / Adam learning rates |
| `random_seed` | Unset | Non-negative Torch and NumPy seed |
| `mixed_precision` | Unset | BF16 or FP16 |
| `restore_optimizer` | true | Restore optimizer state for a new FROM task; use false to change learning rates |
| `eval_input_path` | Unset | Validation data path |
| `nnodes` / `nproc_per_node` | 1 / 1 | Training nodes and processes per node |
| `master_port` | 29500 | Distributed training port |

DSSM sampling settings and item ID binding support MODEL defaults and TRAIN overrides; pools are local to each loading worker. MMoE task weights/metrics also support MODEL defaults and TRAIN overrides, while task types and tower dimensions belong to MODEL. New FROM tasks may change sampling counts/capacity and task weights/metrics; the sampling mode must match the source checkpoint.

`restore_optimizer=true` controls initialization of a new FROM task. Different learning rates require explicit false, restoring weights and resetting all optimizer state. Same-name recovery from this task's own checkpoint always restores its optimizer state.

Running or succeeded same-name TRAIN tasks reuse the existing checkpoint. To start another training run, choose a new checkpoint name. On a failed-task retry, provide the training table and options again; `num_epochs` is the cumulative epoch target for that retry.

## TZRec Online Serving

TZRec services use C++ CPU inference by default. Set `TZREC_SERVING_BACKEND=python` on the serving Deployment and restart to use Python; `cpp` selects C++. Invalid values fail startup. Services receive traffic after model loading and warmup. Scale throughput with service replicas.

Switch an existing Kubernetes deployment through its environment:

```sh
kubectl -n sqlrec set env deployment/rank-service TZREC_SERVING_BACKEND=python
# Switch back to the default C++ backend.
kubectl -n sqlrec set env deployment/rank-service TZREC_SERVING_BACKEND=cpp
```

Use models exported by SQLRec. Supported features are unweighted integer/string IDs, string arrays, separator-delimited strings, continuous scalars, fixed-size float vectors, and bucketized numeric features. Sequence, weighted, vocabulary, and DAG features are unsupported.

`POST /predict` accepts rows or a column map with length-one broadcasting. An
entirely missing input column returns 400; missing row values, nulls, empty
strings, and empty arrays use feature defaults. Empty sparse defaults produce empty
features; dense defaults must match the fixed dimension. Extra columns are ignored. Input errors return 400 and model execution
errors return 500.

The serving container needs Hadoop filesystem configuration and access to its exported model directory. `LOCAL_CACHE_DIR` selects the local model-download cache, default `/tmp/tzrec_model_cache`. A failed download or invalid export prevents startup.

## TZRec Training Curves

Loss, learning rate, available total gradient norm, and task-appropriate training metrics are enabled by default. Set `eval_input_path` for validation curves. Configure these options in `TRAIN MODEL WITH`:

| Option | Default | Description |
|--------|---------|-------------|
| `use_tensorboard` | `true` | Control TensorBoard writes during training; console logs and evaluation results remain available |
| `tensorboard_summaries` | `loss,learning_rate,global_gradient_norm` | Comma-separated native summary types; empty, duplicate, and unknown entries are rejected |
| `is_profiling` | `false` | Collect native training profiler traces; only local or mounted paths are supported, and remote URIs fail before training |
| `log_step_count_steps` | `100` | Positive step interval for training logs and summaries |
| `save_checkpoints_steps` | `1000` | Nonnegative step interval for checkpoints and evaluation; `0` disables periodic step saves |
| `save_checkpoints_epochs` | Unset | Nonnegative epoch interval; `0` disables periodic epoch saves; cannot be positive alongside an explicitly configured positive `save_checkpoints_steps` |
| `enable_train_metrics` | `true` | Compute supported online training metrics; NE is evaluation-only |
| `train_metric_decay_step` | `100` | Native update interval; must be a positive multiple of the log interval when training metrics are enabled |
| `train_metric_decay_rate` | `0.9` | Finite historical metric decay rate in `[0,1)` |

```sql
TRAIN MODEL rank_model CHECKPOINT = 'curves_v1'
ON training_sample
WITH (
  'num_epochs' = '3',
  'eval_input_path' = 'jfs://myjfs/eval/*.parquet',
  'log_step_count_steps' = '100',
  'save_checkpoints_steps' = '500',
  'enable_train_metrics' = 'true'
);
```

For checkpoints only at the end of training, set `save_checkpoints_steps=0` and leave `save_checkpoints_epochs` unset or zero. When selecting epoch-based saving, set an explicitly configured positive step interval to zero first.

Training events are under the origin checkpoint directory; validation events are in `eval_val/`. Copy or mount the directory locally, then run:

```bash
tensorboard --logdir /local/rank_model/curves_v1 --port 6006
```

`tensorboard_summaries` accepts `loss`, `learning_rate`, `global_gradient_norm`, `gradient_norm`, `parameter`, and `gradient`. Loss and learning rate remain available; parameter and gradient histograms add overhead and are best enabled while troubleshooting. The total gradient norm does not include all sharded embedding gradients.

WideAndDeep/DeepFM use AUC and Accuracy; validation also includes normalized entropy. MMoE reports per-task metrics. RocketLaunching validation uses the light branch. DSSM reports Recall@1/5/10/20 within the current batch or sampled candidates, which is different from full-index recall.

Training metrics use decayed history and are not directly comparable with fixed validation-set metrics. AUC requires 0/1 labels; disable training metrics when training WideAndDeep/DeepFM with soft labels. Keep checkpoint directories on persistent storage so curves survive pod deletion.

`is_profiling=true` requires a local or mounted model directory; remote URIs such as `jfs://`, `hdfs://`, and `s3://` are unsupported. Profiling files are stored in `train_eval_trace/`, separately from scalar curves.

## TZRec Sample Data Formats

For TZRec training, select a Hive table with `ON` and partitions with `WHERE`. An `ON` data source specified for export follows the same process. SQLRec resolves the selected table or partitions to storage directories, then selects data files using `*.parquet` to avoid marker files such as `_SUCCESS`. These data files must use Parquet format and have the `.parquet` suffix. SQLRec handles file selection automatically; users specify tables and partitions as usual.

Configure evaluation data through `eval_input_path`, which accepts file paths or glob patterns, including comma-separated paths. A trailing `/*` is automatically converted to `/*.parquet`. Explicit file paths and existing `*.parquet` patterns are used as specified.

Each row is one sample containing the model's feature columns and the labels specified by `label_columns`. Column names and actual Parquet types must match their declarations. Labels are numeric scalars, not strings, booleans or arrays. All target labels must be non-null and finite after conversion to float32, without NaN, Infinity or overflow. Every MMoE row must contain all targets; missing-target masks are not supported. See below for feature types and encoding requirements.

Sample preparation must guarantee the label requirements below; invalid data can fail training or produce misleading metrics.

| Model | Example sample columns (SQL types) | Training and evaluation label requirements |
|-------|-----------------------------------|--------------------------------------------|
| `tzrec.wide_and_deep` | `user_id BIGINT, item_id BIGINT, label FLOAT` | Training labels in `[0,1]`, including soft labels; AUC evaluation labels are 0/1. Hard labels may also use INT/BIGINT |
| `tzrec.deepfm` | `user_id BIGINT, item_id BIGINT, label FLOAT` | Same as WideAndDeep: `[0,1]` soft training labels and 0/1 AUC evaluation labels |
| `tzrec.dssm` | `user_id BIGINT, item_id BIGINT, label INT` | Positive user/item pairs only for both training and evaluation; use a constant numeric label of 1 |
| `tzrec.mmoe` | `user_id BIGINT, item_id BIGINT, click INT, watch_time FLOAT` | Binary targets use numeric 0/1 scalars; regression targets must be declared FLOAT/DOUBLE and stored as floating Parquet columns |
| `tzrec.rocket_launching` | `user_id BIGINT, item_id BIGINT, click INT` | Exposure pairs with both positive and negative rows and one numeric 0/1 label; no negative sampler |

These SQL examples select the required columns from an existing sample table. Write their output as Parquet in the format above and add the features used by your model:

```sql
-- WideAndDeep / DeepFM: keep positive and negative samples; use 0/1 for AUC evaluation too.
SELECT user_id, item_id, CAST(clicked AS INT) AS label
FROM source_sample;

-- DSSM: filter positive pairs first; other items in the batch provide negatives.
SELECT user_id, item_id, CAST(1 AS INT) AS label
FROM source_sample WHERE clicked = 1;

-- MMoE: provide every target in each row and cast regression labels before writing Parquet.
SELECT user_id, item_id, CAST(clicked AS INT) AS click,
       CAST(watch_time AS FLOAT) AS watch_time
FROM source_sample;
```

The native DSSM loss treats every row's user/item pair as positive. Label values do not filter positive/negative samples or act as sample weights. The training entry does not reject or filter rows with zero or negative labels; those pairs are still treated as positives. Filter both training and evaluation samples when preparing the datasets.

## TZRec Features and Checkpoint Compatibility

SQL prediction requests preserve explicit nulls; feature defaults apply to missing values.

Fine-tuning and export require the Model network, features, and labels to match the checkpoint. Put structural options in MODEL; TRAIN/EXPORT cannot change them.

WideAndDeep, DeepFM and DSSM require exactly one `label_columns` entry. The label may be declared in the Model schema or exist only in the training table. It is excluded from features, feature groups, and inferred DSSM towers; online requests do not need it. Explicit DSSM towers must contain declared non-label features and cover all features; specifying one tower infers the other from remaining features. Both towers must be nonempty.

| SQL type | Feature handling |
|----------|------------------|
| INT / INTEGER / BIGINT | Original ID in `[0, bucket_size)`; INT and INTEGER are aliases for the same integer type |
| `VARCHAR` / `STRING` / `ARRAY<STRING>` | FarmHash ID; lists or separator-delimited strings |
| FLOAT / DOUBLE | Continuous RawFeature converted to float32 |
| `ARRAY<FLOAT>` / `ARRAY<DOUBLE>` | Fixed-size raw vector requiring `column.{name}.value_dim` |

Convert multivalue integer IDs to `ARRAY<STRING>`; `ARRAY<INT>` and `ARRAY<BIGINT>` are rejected. Booleans, nonfinite/out-of-float32 values, and vector dimension errors return 400. Request limits are 16 MiB, 4096 rows, and 65536 feature values.

| Column option | Meaning |
|---------------|---------|
| `column.{name}.bucket_size` | Positive ID/hash bucket count |
| `column.{name}.embedding_dim` | Sparse embedding dimension: positive multiple of 4; MLP/AutoDis: positive |
| `column.{name}.default_value` | ID default empty; a nonempty ID default is one token; raw default zero per dimension; separator-delimited vector defaults |
| `column.{name}.separator` | Single ASCII character; default ASCII 29 (`\035`); multivalue strings and vector defaults |
| `column.{name}.value_dim` | Required for float arrays; scalar floats must use 1 |
| `column.{name}.normalizer` | Raw zscore, minmax, or log10 normalization |
| `column.{name}.boundaries` | Comma-separated, finite, strictly increasing raw bucket boundaries; equality selects the right bucket |
| `column.{name}.embedding` | Raw `none`, `mlp` (learned linear projection), or `autodis` (scalar only); cannot combine with boundaries |
| `column.{name}.autodis.num_channels` | AutoDis channels; default 3 |

Normalizer examples: `method=zscore,mean=10,standard_deviation=2`, `method=minmax,min=0,max=100`, or `method=log10,threshold=0.0001,default=-4`. Omitted log10 parameters are threshold=1 and default=0. Compute statistics from training data beforehand. Defaults are in the **normalized space** and bypass normalization. Scalar bucketized raw features with an empty default bucketize missing online values as unnormalized zero; vectors retain empty rows. DOUBLE inputs use float32 precision.

Bucketized `ARRAY<FLOAT/DOUBLE>` features with `value_dim=1` require a nonempty default: serving converts them to numeric scalars, whose missing-value semantics differ from training lists with an empty default.

```sql
CREATE MODEL float_rec (
    user_id BIGINT,
    item_id BIGINT,
    price DOUBLE,
    embedding ARRAY<FLOAT>,
    score DOUBLE,
    label INT
) WITH (
    'model' = 'tzrec.wide_and_deep',
    'label_columns' = 'label',
    'column.price.normalizer' = 'method=zscore,mean=10,standard_deviation=2',
    'column.embedding.value_dim' = '3',
    'column.score.embedding' = 'autodis',
    'column.score.embedding_dim' = '16'
);
```

Plain raw, MLP, and AutoDis features enter the Deep group. ID and bucketized raw features enter both Wide and Deep; DeepFM's FM group contains sparse features with equal embedding dimensions. WideAndDeep/DeepFM require a sparse feature; DSSM, MMoE, and RocketLaunching support continuous-only features. RocketLaunching generates a single Deep group.

`tzrec.wide_and_deep` uses WideAndDeep and `tzrec.deepfm` uses DeepFM. Export and fine-tuning require the checkpoint architecture to match the declared model. Checkpoints containing label features must be retrained with labels excluded. New FROM tasks preserve the model contract and accept new training settings. Optimizer state is restored by default; changing learning rates requires explicit `restore_optimizer=false`, which restores weights and resets all optimizer state. Create a new model for feature or architecture changes; TRAIN/EXPORT cannot switch model types.

Training and resource defaults declared in MODEL may be overridden by the corresponding TRAIN, EXPORT, or SERVICE options. Export uses `batch_size` (default 8192). Use a completed export checkpoint for serving and retain its complete directory.

## Wide & Deep Model

Wide & Deep model is a recommendation model implemented based on the tzrec framework, supporting complete training, export, and service deployment workflow.

**Model Name**: `tzrec.wide_and_deep`

**Output Fields**:

| Field Name | Type | Description |
|-----------|------|-------------|
| `probs` | FLOAT | Predicted probability value |

**Required parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `label_columns` | String | Label column name |

**Training Configuration Parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `embedding_dim` | Integer | 16 | Embedding dimension |
| `num_buckets` | Integer | 1000000 | Integer feature bucket count |
| `hidden_units` | String | "512,256,128" | Deep network hidden layer unit count |

Training, topology, and column options use the common configuration above.

**Usage Example**:

```sql
CREATE MODEL rec_model (
    user_id VARCHAR,
    item_id VARCHAR,
    category VARCHAR,
    price DOUBLE,
    label INT
) WITH (
    'model' = 'tzrec.wide_and_deep',
    'label_columns' = 'label',
    'embedding_dim' = '32',
    'hidden_units' = '512,256,128',
    'column.user_id.embedding_dim' = '64',
    'column.item_id.embedding_dim' = '64'
);
```

## DeepFM Model

**Model Name**: `tzrec.deepfm`. It shares Wide & Deep training and feature options, adds FM pairwise interactions, and requires equal sparse embedding dimensions. Its output is `probs`.

## MMoE Multi-target Ranking

**Model Name**: `tzrec.mmoe`. Shared experts and separate task towers return every target in one prediction request. Binary classification, scalar regression, mixed tasks, and continuous-only features are supported.

```sql
CREATE MODEL multi_rank (
    user_id BIGINT, item_id BIGINT, price DOUBLE,
    click INT, `like` INT, watch_time DOUBLE
) WITH (
    'model' = 'tzrec.mmoe',
    'label_columns' = 'click,like,watch_time',
    'task.watch_time.type' = 'regression'
);

TRAIN MODEL multi_rank CHECKPOINT = 'v1' ON training_sample
WITH (
    'task.like.weight' = '2.0',
    'task.watch_time.weight' = '0.1'
);
```

`label_columns` defines at least two ordered tasks. Each label must be a declared numeric scalar field with a unique name matching `[A-Za-z_][A-Za-z0-9_]*`. Task names equal label names. All tasks share the non-label features; online inputs do not need labels.

| Option | Default | Meaning |
| --- | --- | --- |
| `num_expert` | `3` | Positive expert count |
| `expert_hidden_units` | `256,128` | Expert MLP dimensions |
| `task_hidden_units` | `64,32` | Default task tower dimensions |
| `task.<label>.hidden_units` | Task tower default | Per-task dimensions |
| `task.<label>.type` | `binary` | `binary` (BCE) or `regression` (L2/MSE) |

Training options (set in TRAIN MODEL WITH):

| Option | Default | Meaning |
| --- | --- | --- |
| `task.<label>.weight` | `1.0` | Finite positive float32 training loss weight |
| `task.<label>.metrics` | Binary: `auc,normalized_entropy,accuracy`; regression: `mean_squared_error,mean_absolute_error` | Comma-separated subset of the task defaults; NE is evaluation-only and other selected metrics also collect online training statistics |
| `eval_input_path` | Unset | Separate Parquet evaluation path; overridable in TRAIN WITH |

Reuse TZRec's resource, optimizer, batch, epoch and `column.*` options. MMoE uses expert/task dimensions instead of single-target `hidden_units` or DSSM tower options. Unknown task names/options fail at model creation.

Training and evaluation labels must be present and non-null: binary labels are 0/1. Regression labels must be declared FLOAT/DOUBLE and stored as floating Parquet columns, with values finite after conversion to float32. Cast integer regression labels before producing training data to avoid a dtype error in TZRec's L2 backward pass. The sample preparation process must satisfy these requirements; the training entry does not pre-scan labels. Without an evaluation path, no evaluation dataset is created.

```sql
CACHE TABLE scored AS CALL call_service('multi-rank-service', rank_feature);
SELECT item_id, 0.6 * probs_click + 0.4 * probs_like AS rank_score
FROM scored ORDER BY rank_score DESC;
```

Binary outputs are `probs_<label> FLOAT`; regression outputs are `y_<label> FLOAT`. `call_service` appends these columns to the input table. Combine predictions in SQL, for example `0.6 * probs_click + 0.4 * probs_like`; training loss weights and serving fusion weights are separate. Normalize regression predictions to the appropriate business scale before fusion.

One `<checkpoint>_export` and one service contain all tasks. MODEL defines targets, task types, networks and features, which must match the checkpoint. New FROM training may change task weights/metrics, epochs, batch size, learning rates and evaluation inputs. Task aliases, multiclass targets, missing-label masks and conditional task sample spaces are not supported; every target must have valid labels over the same sample space.

## RocketLaunching Pre-ranking {#rocket-launching}

**Model name**: `tzrec.rocket_launching`. User/item features are modeled jointly. Booster and light learn exposure click labels together, while light also receives logit and hidden-feature distillation. Serving runs only the light path. The internal booster cannot load an existing ranking checkpoint as an external teacher.

```sql
CREATE MODEL prerank_model (
    user_id BIGINT, item_id BIGINT, category STRING, price DOUBLE, click INT
) WITH (
    'model' = 'tzrec.rocket_launching',
    'label_columns' = 'click',
    'embedding_dim' = '8',
    'booster_hidden_units' = '256,128,64',
    'light_hidden_units' = '64,32'
);
```

| Option | Default | Meaning |
| --- | --- | --- |
| `booster_hidden_units` | `256,128,64` | Nonempty positive integer widths for the internal booster |
| `light_hidden_units` | `64,32` | Nonempty positive integer widths for the serving light network |
| `share_hidden_units` | Unset | Optional shared MLP; increases serving computation |

Booster/light must share at least one hidden width. Feature distillation is fixed to enabled COSINE; explicitly disabling it or selecting another distance is rejected. Loss selection, external teachers, single-target `hidden_units`, DSSM tower options, and MMoE task options are unsupported. Reuse common TZRec resource, optimizer, batch, epoch, and `column.*` options.

Declare one numeric label in the model schema. Prepare 0/1 exposure labels for training and validation: unclicked exposures can be negatives, but unexposed candidates should not be labeled zero automatically. Labels are not inputs at inference time.

Evaluate `auc_light`, `normalized_entropy_light`, `accuracy_light`, and `binary_cross_entropy_light`. Native logs may retain unused booster metrics, including NaN; these are not valid evaluation results. Also evaluate candidate Top K retention, retention of full-ranking Top M, final quality, and total serving cost; AUC alone is insufficient.

Follow the [model lifecycle](../../guides/model-lifecycle.md). The single `<checkpoint>_export` exposes **`probs_light FLOAT`**; `logits_light` may also appear as an extra output. Keep network, features, labels, and distillation settings unchanged during fine-tuning, export, and service creation.

Candidate requests must satisfy the 4096-row, 16 MiB, and feature-value limits. The two-table call reduces repeated user data transmission, but does not guarantee a single user-network computation per request.

## DSSM Model

DSSM defaults to in-batch negative sampling and also supports item-pool sampling. It requires positive-only pairs for both training and evaluation, with numeric labels preferably set to 1. Labels do not filter pairs or act as sample weights. The training entry does not reject or filter zero or negative labels; filter positive pairs when preparing the datasets. See “TZRec Sample Data Formats” above.

DSSM (Deep Structured Semantic Models) is a two-tower retrieval model implemented based on the tzrec framework, supporting complete training, export, and service deployment workflow.

SQLRec DSSM uses a fixed `softmax_cross_entropy` objective with Recall@1, Recall@5, Recall@10, and Recall@20 evaluation metrics. It does not offer a choice of objectives; supplied loss options are ignored. Restoring or exporting a checkpoint saved with another objective is rejected. Users prepare both training and evaluation samples beforehand; SQLRec does not filter them automatically.

**Model Name**: `tzrec.dssm`

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
| `embedding_dim` | Integer | 16 | Embedding dimension |
| `num_buckets` | Integer | 1000000 | Integer feature bucket count |
| `hidden_units` | String | "512,256,128" | Deep network hidden layer unit count |
| `user_hidden_units` | String | "512,256,128" | User tower hidden layer unit count |
| `item_hidden_units` | String | "512,256,128" | Item tower hidden layer unit count |
| `output_dim` | Integer | 64 | Output embedding dimension |

DSSM sampling options, in addition to the common parameters:

| Option | Type | Default | Meaning |
| --- | --- | --- | --- |
| `negative_sampling` | String | `in_batch` | `in_batch` or `pool` |
| `negative_sample_count` | Integer | 256 | Shared negatives per batch in pool mode |
| `negative_pool_size` | Integer | 4096 | Maximum distinct cached items; must exceed `batch_size` |
| `item_id_column` | String | — | Required in pool mode: scalar STRING/INT/BIGINT ID in `item_features` |

**Negative Sampling**:

DSSM uses negative examples as a contrast during training, learning to give positive items higher matching scores. Choose the source of negatives with `negative_sampling`:

- `in_batch` (default): Uses items from other examples in the same batch as negatives. The number of negatives depends on the batch size.
- `pool`: Builds a candidate pool from items encountered during training and randomly draws negatives from it for each batch. Candidates can come from other batches, allowing more negatives even with a smaller training batch.

Pool sampling uses existing training data, so no separate item table is needed. `negative_sample_count` controls how many negatives to draw per batch, `negative_pool_size` limits the number of distinct items in the pool, and `item_id_column` identifies the item ID column. Pool mode requires disjoint `user_features` and `item_features`; the sampler expands only item-tower fields. The pool contains only items encountered during training, rather than the complete catalog. A larger pool does not necessarily improve quality; use validation results to choose its size.

**Usage Example**:

This example enables pool sampling with a training batch size of 256, 256 negatives per batch, and a pool capacity of 4096 distinct items:

```sql
CREATE MODEL dssm_model (
    user_id VARCHAR,
    user_age INT,
    item_id VARCHAR,
    item_category VARCHAR,
    label INT
) WITH (
    'model' = 'tzrec.dssm',
    'label_columns' = 'label',
    'user_features' = 'user_id,user_age',
    'item_features' = 'item_id,item_category',
    'embedding_dim' = '64',
    'user_hidden_units' = '256,128,64',
    'item_hidden_units' = '256,128,64'
);

TRAIN MODEL dssm_model CHECKPOINT = 'v1' ON training_sample
WITH (
    'batch_size' = '256',
    'negative_sampling' = 'pool',
    'negative_sample_count' = '256',
    'negative_pool_size' = '4096',
    'item_id_column' = 'item_id'
);

-- After training and exporting as in the model guide, DSSM produces:
-- v1_export/item and v1_export/user
CREATE SERVICE `dssm-item-service`
    ON MODEL dssm_model
    CHECKPOINT = 'v1_export/item';

CREATE SERVICE `dssm-user-service`
    ON MODEL dssm_model
    CHECKPOINT = 'v1_export/user';
```

User and item vectors for retrieval must come from the same checkpoint. Item vectors can be computed offline to build an ANN index. When updating a model, update the user tower service, item vectors, and index together to avoid mixing versions.
