# Built-in Models

This document introduces SQLRec built-in model types and their usage.

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

### TZRec configuration stages

| Location | Options |
| --- | --- |
| CREATE MODEL WITH | Model type/path, fields/labels, feature encoding, networks, tower groups and MMoE task types/dimensions; optional training and resource defaults |
| TRAIN MODEL WITH | Training budget/optimizers, loading, evaluation input, sampling, task weights/metrics, observation/checkpoints and topology |
| EXPORT MODEL WITH | `batch_size` (default 8192) and export resources; fixed to one process and one loading worker |
| CREATE SERVICE WITH | `replicas`, inference environment and serving resources |

Training and runtime options use this precedence: operation WITH > MODEL WITH > ConfigOption default. MODEL may declare batch size, workers, learning rates and resource defaults; TRAIN overrides them for that task. EXPORT and SERVICE may also override model resource defaults. Unused options are ignored. Consumed values still undergo value and cross-option validation. Raw environment settings cannot override generated topology, rank, seed or hash settings.

TRAIN options include `batch_size=8192` per rank, `num_workers=8` per rank (0 reads in the training process), `num_epochs=1`, `sparse_lr=0.001` (Adagrad), `dense_lr=0.001` (Adam), optional `random_seed`/`mixed_precision`/`eval_input_path`, and `nnodes=1`, `nproc_per_node=1`, `master_port=29500`. Learning rates are constant. Global positive batch size is approximately batch × nodes × processes; sampled negative candidates are additional.

DSSM sampling settings and item ID binding support MODEL defaults and TRAIN overrides; pools are local to each loading worker. MMoE task weights/metrics also support MODEL defaults and TRAIN overrides, while task types and tower dimensions belong to MODEL. New FROM tasks may change sampling counts/capacity and task weights/metrics; the sampling mode must match the source checkpoint.

`restore_optimizer=true` controls initialization of a new FROM task. Different learning rates require explicit false, restoring weights and resetting all optimizer state. Same-name recovery from this task's own checkpoint always restores its optimizer state.

Running or succeeded same-name tasks return the existing checkpoint. Failed retries preserve the model directory and regenerate configuration from the current TRAIN WITH, MODEL defaults and ConfigOption defaults. Supply ON and the required training options again; previous SQL options are not restored or compared. TZRec restores model/optimizer state and data progress when a checkpoint exists. Epochs are a new FROM task's budget, or the cumulative target on same-name recovery. New options do not restart a running or succeeded task.

### TZRec Online Serving

TZRec services default to native C++ CPU inference. Set the environment variable
`TZREC_SERVING_BACKEND=python` to select Python serving; `cpp` or an unset variable
selects C++. Changes take effect on process restart, and invalid values fail
startup. Both backends share the `--scripted_model_dir`, `--host`, and `--port`
arguments and remote export cache. Kubernetes startup/readiness probes use
`/health`, which becomes available after initialization. C++ also warms up before
listening. DSSM services load the respective user or item export directory.

Switch an existing Kubernetes deployment through its environment:

```sh
kubectl -n sqlrec set env deployment/rank-service TZREC_SERVING_BACKEND=python
# Switch back to the default C++ backend.
kubectl -n sqlrec set env deployment/rank-service TZREC_SERVING_BACKEND=cpp
```

Exports must contain `scripted_model.pt`, `fg.json`, and `pipeline.config`.
Preprocessing follows the exported `fg.json`: `num_buckets` preserves integer
IDs within the configured range, while `hash_bucket_size` uses FarmHash
Fingerprint64. Supported inputs are ordinary unweighted ID features with integer
or string scalars, string arrays, or strings split by the configured `separator`.
Continuous raw scalars, fixed-size float vectors, and bucketized raw features are also supported. Sequence, weighted, vocabulary, and DAG configurations fail at startup.

`POST /predict` accepts rows or a column map with length-one broadcasting. An
entirely missing input column returns 400; missing row values, nulls, empty
strings, and empty arrays use feature defaults. Empty sparse defaults produce empty
features; dense defaults must match the fixed dimension. Extra columns are ignored. Input errors return 400 and model execution
errors return 500.

`server.sh` downloads remote exports with `hadoop fs -get`, using
`$HADOOP_HOME/bin/hadoop` when set or `hadoop` on PATH otherwise. Configure the
container's Hadoop client for the selected filesystem (for example HDFS or
JuiceFS), including connection and authentication settings. Local directories
are used directly. `LOCAL_CACHE_DIR` selects the parent cache directory,
defaulting to `/tmp/tzrec_model_cache`. Each startup downloads into a private
subdirectory. Failed downloads or missing required files cause cleanup and exit;
Both backends start through `server.sh`, which calls `validate_export.py` once to
check the manifest, completion marker, checksums and pipeline contract before
using `exec` to start the selected serving backend. `server.py` loads the model
and performs inference warmup without repeating export-file validation. Failed
validation also removes the private download directory.
The Python backend uses Torch and the Python FG parser. The ARM Python FG package supports the ID/raw configurations generated by SQLRec. Both backends warm up before readiness and execute inference serially; scale with replicas.

### TZRec Training Curves

SQLRec generates native TZRec `train_config`, `metrics`, and `train_metrics` settings; TZRec collects and writes summaries without source changes. Loss, learning rate, the total norm of available gradients, and task-appropriate online metrics are enabled by default. Set `eval_input_path` to collect validation metrics as well. These options apply to all built-in TZRec models in `TRAIN MODEL WITH`:

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

To save and evaluate only at training end, set `save_checkpoints_steps=0` and leave `save_checkpoints_epochs` unset or `0`. To switch a declared cadence in `TRAIN MODEL WITH`, set the previous interval to `0` and set a positive interval for the other trigger. A positive epoch interval alone disables the default step interval; an epoch interval of `0` preserves the step interval.

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

Use the pinned TZRec version's native layout: training event files directly in the origin `model_dir`, validation events in `model_dir/eval_val/`, standalone evaluation events in `model_dir/eval/`, and profiling traces in `model_dir/train_eval_trace/`. This version has no separate log directory option. `pipeline.config`, `model.ckpt-*`, and the JSON Lines evaluation results `train_eval_result_v2.txt` also live in the training directory. After mounting or synchronizing a run to `/local/rank_model/curves_v1`, use:

```bash
tensorboard --logdir /local/rank_model/curves_v1 --port 6006
```

`tensorboard_summaries` accepts six native types, all sampled at `log_step_count_steps`. TZRec always retains baseline loss and learning rate summaries. Online metrics are controlled separately by `train_metrics`. For example, selecting only `gradient_norm` keeps loss and learning rate but omits the total gradient norm.

| Type | TensorBoard content | Enabled by default |
|------|---------------------|--------------------|
| `loss` | `loss/total` and individual `loss/*`, including MMoE task and RocketLaunching distillation losses | Yes |
| `learning_rate` | Optimizer parameter group `lr/g*` | Yes |
| `global_gradient_norm` | `global_gradient_norm`, the total L2 norm of available gradients | Yes |
| `gradient_norm` | Per-parameter `<name>/gradient_norm` | No |
| `parameter` | Parameter value histograms in Histograms / Distributions | No |
| `gradient` | Gradient histograms at `<name>/gradient` | No |

Only rank 0 writes gradient and parameter summaries. The total gradient norm excludes `ShardedTensor` and parameters without `.grad`; sharded Embedding or fused optimizer gradients may be absent, so this is not an all-rank, all-parameter norm. Sharded parameter histograms contain only the local shard. Parameter and gradient histograms may substantially increase collection cost and event size; enable them for diagnosis. Native settings do not provide a separate histogram interval or parameter filter.

```sql
-- Enable all native curves and histograms for diagnosis; training metrics remain independent.
TRAIN MODEL rank_model CHECKPOINT = 'diagnostics_v1'
ON training_sample
WITH (
  'tensorboard_summaries' = 'loss,learning_rate,global_gradient_norm,gradient_norm,parameter,gradient',
  'log_step_count_steps' = '100'
);
```

Set `'tensorboard_summaries' = 'loss,learning_rate'` to keep only baseline loss and learning rate summaries. `use_tensorboard=false` disables all training-time summary writes but does not disable `is_profiling`.

`is_profiling=true` requires a local filesystem or mounted `model_dir`; remote URIs such as `jfs://`, `hdfs://`, and `s3://` are rejected before job submission and at the training entry. Native Profiler exports through C++ Kineto rather than Python fsspec; JuiceFS fails with `No logger registered for the jfs protocol prefix`. Ordinary TensorBoard curves, gradient norms, and histograms still support remote directories.

Profiler uses the pinned TZRec window: wait 1 step, warm up 1 step, and record 10 steps once, including shapes, memory and stacks, under `model_dir/train_eval_trace/`. SQLRec does not expose the `profiler_config` from other TZRec versions. Download `train_eval_trace/` alongside the event files and use a compatible profiler viewer; traces are separate from ordinary scalar curves.

The pinned TZRec version supports 10 evaluation metric types, six of which also support online training. Individual models impose additional restrictions. See the [official TZRec metric reference](https://torcheasyrec.readthedocs.io/zh-cn/latest/models/evaluation_metrics.html). SQLRec chooses defaults by output type:

| Native metric | Online training | SQLRec default and purpose |
|---------------|-----------------|----------------------------|
| `auc` | Yes | Enabled for binary tasks; higher means better discrimination |
| `normalized_entropy` | No | Enabled for binary validation; lower is better, and below 1 beats the mean-label baseline |
| `accuracy` | Yes | Enabled for binary tasks at the native 0.5 threshold; a diagnostic alongside AUC, especially with imbalanced labels |
| `mean_squared_error` | Yes | Enabled for MMoE regression tasks; lower squared error is better |
| `mean_absolute_error` | Yes | Enabled for MMoE regression tasks; lower absolute error in label units is better |
| `recall_at_k` | Yes | Enabled for DSSM at K=1, 5, 10, 20; higher is better |
| `multiclass_auc` | No | Native multiclass support; SQLRec ranking tasks currently have scalar binary or regression outputs |
| `grouped_auc` | No | Requires an explicit `grouping_key` and feature; no grouping is inferred |
| `xauc` | Yes | Pairwise ranking of continuous targets; sampling and pair limits need workload-specific settings, so it is not enabled by default |
| `grouped_xauc` | No | Requires a group feature and within-group pair settings; not enabled by default |

Training tags include `loss/total`, individual `loss/*`, and `lr/g*`. WideAndDeep/DeepFM add `metric/auc` and `metric/accuracy`, plus validation-only `metric/normalized_entropy`. DSSM adds `metric/recall@1`, `metric/recall@5`, `metric/recall@10`, and `metric/recall@20`. MMoE follows each task's `task.<label>.metrics`, with tags such as `metric/auc_label`, `metric/normalized_entropy_label`, and `metric/mean_absolute_error_watch_time`; NE appears only in validation. RocketLaunching collects AUC and Accuracy for both booster and light during training. Validation runs the light branch; use `metric/auc_light`, `metric/accuracy_light`, and `metric/normalized_entropy_light`. Native results may also include unused booster metric placeholders.

Validation losses use `metric/*`, such as `metric/binary_cross_entropy` and `metric/l2_loss_watch_time`, and are collected automatically without an extra logloss metric. DSSM Recall uses the current batch or sampled candidate set, rather than the full retrieval index; K at or above the candidate count saturates. Validation curves require `eval_input_path` and follow the checkpoint cadence; short runs may have only one final validation point.

Training loss is a sampled batch value written by rank 0, not a global epoch mean. Online training metrics use historical decay; newly initialized metrics report zero before the first update window completes and differ from metrics on a fixed validation set. Unless explicitly overridden, SQLRec uses the native defaults `decay_step=100` and `decay_rate=0.9`. Training and evaluation AUC require 0/1 labels; disable training metrics when training WideAndDeep/DeepFM with soft labels.

Fine-tuning an existing checkpoint can override observation settings while preserving its network, features, and task structure. Writer lifecycle, buffering, and metric restoration follow the pinned TZRec version's native behavior: fine-tuning may restore existing training metric statistics. Only rank 0 writes training event files. The standalone `tzrec.eval` command creates an `eval/` writer independently of the training TensorBoard switch. It does not explicitly close that writer, so asynchronous summaries may remain unwritten; use `eval_result.txt` for standalone evaluation results and `eval_val/` for SQLRec training validation curves.

Use persistent storage or a persistent mount for `model_dir`. Remote URIs require filesystem dependencies, connectivity, and credentials on both the training and viewing sides; `USE_FSSPEC=1` alone does not guarantee TensorBoard remote access. Synchronizing events locally is also supported. Curves remain with the origin training directory; model export does not archive training curves.

### TZRec Sample Data Formats

For TZRec training, select a Hive table with `ON` and partitions with `WHERE`. An `ON` data source specified for export follows the same process. SQLRec resolves the selected table or partitions to storage directories, then selects data files using `*.parquet` to avoid marker files such as `_SUCCESS`. These data files must use Parquet format and have the `.parquet` suffix. SQLRec handles file selection automatically; users specify tables and partitions as usual.

Configure evaluation data through `eval_input_path`, which accepts file paths or glob patterns, including comma-separated paths. A trailing `/*` is automatically converted to `/*.parquet`. Explicit file paths and existing `*.parquet` patterns are used as specified.

Each row is one sample containing the model's feature columns and the labels specified by `label_columns`. Column names and actual Parquet types must match their declarations. Labels are numeric scalars, not strings, booleans or arrays. All target labels must be non-null and finite after conversion to float32, without NaN, Infinity or overflow. Every MMoE row must contain all targets; missing-target masks are not supported. See below for feature types and encoding requirements.

**The training entry does not pre-scan training or evaluation labels or synchronize label validation across ranks.** Prepare samples according to these requirements before training. TZRec's native trainer reads the data directly; incompatible samples may fail during training or affect losses and metrics.

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

### TZRec Features and Checkpoint Compatibility

SQL prediction requests preserve explicit nulls, allowing feature defaults even when the whole batch is null.

Fine-tuning and export require the MODEL network, features and label contract to match the checkpoint. Raw defaults and normalizers compare by numeric meaning; ID defaults and separators compare exactly. Structure options in TRAIN/EXPORT are ignored. Create and train a new model to change its structure.

Continuous scalar/vector defaults and numeric normalizer parameters are compared at float32 precision; vector element order and dimensions must match. Normalizer parameter order does not affect comparison. Omitted `log10` parameters use `threshold=1` and `default=0`, matching the original feature generator. Accepted equivalent configurations retain the original saved checkpoint configuration. These rules also apply to MMoE feature comparisons; tasks and network structure must still match.

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

Plain raw, MLP, and AutoDis features enter the Deep group. ID and bucketized raw features enter both Wide and Deep; DeepFM's FM group contains sparse features with equal embedding dimensions. WideAndDeep/DeepFM require a sparse feature; DSSM, MMoE, and RocketLaunching support continuous-only features. RocketLaunching generates a single Deep group.

`tzrec.wide_and_deep` uses WideAndDeep and `tzrec.deepfm` uses DeepFM. Export and fine-tuning require the checkpoint architecture to match the declared model. Checkpoints containing label features must be retrained with labels excluded. New FROM tasks preserve the model contract and accept new training settings. Optimizer state is restored by default; changing learning rates requires explicit `restore_optimizer=false`, which restores weights and resets all optimizer state. Create a new model for feature or architecture changes; TRAIN/EXPORT cannot switch model types.

TZRec stages have independent options; MODEL training/resource options are not inherited. Exports use a private staging directory, SHA256 manifest `model_meta.json`, and `_SUCCESS` before publishing. Existing targets are never overwritten. Startup requires the manifest and completion marker and verifies file checksums for every model. HDFS/JuiceFS publication relies on directory rename semantics.

Local builds and image publication run `bin/verify_tzrec_image.sh`, including native operator/output checks and real train/export/HTTP Python-C++ comparisons for WideAndDeep, DeepFM, DSSM, MMoE, and RocketLaunching. DSSM coverage includes softmax retrieval training, fine-tuning, and both tower exports; MMoE coverage includes mixed tasks, regression-only tasks, continuous features, fine-tuning, and two-process training. RocketLaunching coverage includes gradients, light-only inference, shared layers, continuous features, fine-tuning, and two-process training. ARM FG also has unit tests and a native x86 differential gate against the original pyfg wheel.

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

Training options (set in TRAIN MODEL WITH):

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `sparse_lr` | Double | 0.001 | Sparse feature learning rate |
| `dense_lr` | Double | 0.001 | Dense feature learning rate |
| `num_epochs` | Integer | 1 | Number of training epochs |
| `batch_size` | Integer | 8192 | Batch size |
| `num_workers` | Integer | 8 | Data loader worker process count |
| `mixed_precision` | String | - | Mixed precision training mode, `BF16` or `FP16`, disabled by default |

**Distributed Training Parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `nnodes` | Integer | 1 | Training node count |
| `nproc_per_node` | Integer | 1 | Processes per node |
| `master_port` | Integer | 29500 | Distributed training master port |

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
    'model' = 'tzrec.wide_and_deep',
    'label_columns' = 'label',
    'embedding_dim' = '32',
    'hidden_units' = '512,256,128',
    'column.user_id.embedding_dim' = '64',
    'column.item_id.embedding_dim' = '64'
);
```

### 2.1 DeepFM Model

**Model Name**: `tzrec.deepfm`. It shares Wide & Deep training and feature options, adds FM pairwise interactions, and requires equal sparse embedding dimensions. Its output is `probs`.

### 2.2 MMoE Multi-target Ranking

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

Binary outputs are `probs_<label> FLOAT`; regression outputs are `y_<label> FLOAT`. `call_service` appends these columns to the input table. Combine predictions in SQL, for example `0.6 * probs_click + 0.4 * probs_like`; training loss weights and serving fusion weights are separate. Normalize regression predictions to the appropriate business scale before fusion.

One `<checkpoint>_export` and one service contain all tasks. MODEL defines targets, task types, networks and features, which must match the checkpoint. New FROM training may change task weights/metrics, epochs, batch size, learning rates and evaluation inputs. Task aliases, multiclass targets, missing-label masks and conditional task sample spaces are not supported; every target must have valid labels over the same sample space.

### 2.3 RocketLaunching Pre-ranking {#rocket-launching}

**Model name**: `tzrec.rocket_launching`. User/item features are modeled jointly. Booster and light learn exposure click labels together, while light also receives logit and hidden-feature distillation. Serving runs only the light path. The internal booster cannot load an existing ranking checkpoint as an external teacher. The native TZRec implementation is used without source changes.

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

Declare one numeric label in the model schema and prepare both training and evaluation exposure pairs with numeric 0/1 labels. Users ensure the values; SQLRec does not scan, filter or generate negatives. Unclicked exposures can be negative examples; unexposed candidates cannot automatically be labeled zero. BCE and num_class=1 are fixed. Labels are excluded from features and are not needed online. Native light inputs are detached; shared embedding/share_mlp parameters receive booster supervision, rather than light supervision/distillation gradients.

Evaluate `auc_light`, `normalized_entropy_light`, `accuracy_light`, and `binary_cross_entropy_light`. Native logs may retain unused booster metrics, including NaN; these are not valid evaluation results. Also evaluate candidate Top K retention, retention of full-ranking Top M, final quality, and total serving cost; AUC alone is insufficient.

Use the normal [model lifecycle](../../guides/model-lifecycle.md). One `<checkpoint>_export` returns public **`probs_light FLOAT`**, with native `logits_light` retained as an extra output. Publication validates finite scalar light outputs and probability ranges; startup requires a matching output manifest. TRAIN/EXPORT/SERVICE cannot change network, features, labels, or distillation settings. Fine-tuning may change learning rates, batch, epochs, and evaluation paths.

Candidate requests must satisfy the 4096-row, 16 MiB, and feature-value limits. The two-table call reduces repeated user data transmission, but does not guarantee a single user-network computation per request.

### 3. DSSM Model

DSSM defaults to in-batch negative sampling and also supports item-pool sampling. It requires positive-only pairs for both training and evaluation, with numeric labels preferably set to 1. Labels do not filter pairs or act as sample weights. The training entry does not reject or filter zero or negative labels; filter positive pairs when preparing the datasets. See “TZRec Sample Data Formats” above.

DSSM (Deep Structured Semantic Models) is a two-tower retrieval model implemented based on the tzrec framework, supporting complete training, export, and service deployment workflow.

SQLRec DSSM uses a fixed `softmax_cross_entropy` objective with Recall@1, Recall@5, Recall@10, and Recall@20 evaluation metrics. It does not offer a choice of objectives; supplied loss options are ignored. Restoring or exporting a checkpoint saved with another objective is rejected. Users prepare both training and evaluation samples beforehand; SQLRec does not filter them automatically.

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
| `embedding_dim` | Integer | 16 | Embedding dimension |
| `num_buckets` | Integer | 1000000 | Integer feature bucket count |
| `hidden_units` | String | "512,256,128" | Deep network hidden layer unit count |
| `user_hidden_units` | String | "512,256,128" | User tower hidden layer unit count |
| `item_hidden_units` | String | "512,256,128" | Item tower hidden layer unit count |
| `output_dim` | Integer | 64 | Output embedding dimension |

Training options (set in TRAIN MODEL WITH):

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `sparse_lr` | Double | 0.001 | Sparse feature learning rate |
| `dense_lr` | Double | 0.001 | Dense feature learning rate |
| `num_epochs` | Integer | 1 | Number of training epochs |
| `batch_size` | Integer | 8192 | Batch size |
| `num_workers` | Integer | 8 | Data loader worker process count |
| `negative_sampling` | String | `in_batch` | `in_batch` or `pool` |
| `negative_sample_count` | Integer | 256 | Shared negatives per batch in pool mode |
| `negative_pool_size` | Integer | 4096 | Maximum distinct cached items; must exceed `batch_size` |
| `item_id_column` | String | — | Required in pool mode: scalar STRING/INT/BIGINT ID in `item_features` |
| `mixed_precision` | String | - | Mixed precision training mode, `BF16` or `FP16`, disabled by default |

**Distributed Training Parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `nnodes` | Integer | 1 | Training node count |
| `nproc_per_node` | Integer | 1 | Processes per node |
| `master_port` | Integer | 29500 | Distributed training master port |

**Column-level Configuration Parameters**:

Can configure parameters separately for each feature column:

| Parameter Format | Description |
|-----------------|-------------|
| `column.{feature_name}.bucket_size` | Feature bucket count |
| `column.{feature_name}.embedding_dim` | Feature embedding dimension |

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
-- v1.0_export/item and v1.0_export/user
CREATE SERVICE `dssm-item-service`
    ON MODEL dssm_model
    CHECKPOINT = 'v1.0_export/item';

CREATE SERVICE `dssm-user-service`
    ON MODEL dssm_model
    CHECKPOINT = 'v1.0_export/user';
```

User and item vectors for retrieval must come from the same checkpoint. Item vectors can be computed offline to build an ANN index. When updating a model, update the user tower service, item vectors, and index together to avoid mixing versions.

### 4. LightGBM Model

The LightGBM model is based on the GBDT (Gradient Boosting Decision Tree) framework, supporting the full train/export/serve lifecycle. Training data and artifacts use the configured storage; export produces ONNX format for online inference.

LightGBM training features are converted to float32 to match ONNX and serving precision. NaN missing values are retained; Infinity and float32 overflow are rejected. Models previously trained on double values require retraining to guarantee this precision consistency.

**Model name**: `gbdt.lightgbm`

GBDT TRAIN, EXPORT, and SERVICE may override runtime and resource settings, but cannot change the declared model type or `label_columns`. Explicitly repeating the declared value is allowed; a different value raises an error. Create a new model to change its type or label. This rule applies to LightGBM, XGBoost, and CatBoost.

GBDT serving converts missing numeric fields and `null` internally to float32 `NaN` for the model's missing-value handling, matching training; `0` remains a valid number. Clients should send `null` or omit the field; do not send `NaN` in JSON. Numeric strings must be complete, finite float32 decimal numbers; invalid values return HTTP 400. Services inherit model image, version and resource settings, with service parameters taking precedence. Traffic starts after model download/loading and a successful `/health` probe. This also applies to XGBoost and CatBoost.

`POST /predict` accepts a nonempty array of row objects or a column map. Column values must be arrays with matching lengths; length-one columns can broadcast. JSON parsing failures, non-object rows, non-array columns, incompatible column lengths, and empty batches return HTTP 400. Model runtime exceptions return HTTP 500. Error responses use JSON with an `error` field.

**Features**:
- LightGBM-based gradient boosting decision tree
- Float/double numerical features only (no categorical support; use CatBoost for categorical/integer features)
- Parquet training data on distributed storage
- Model artifacts persisted to distributed storage
- Exports ONNX format for serving (via onnxmltools)
- C++ ONNX Runtime inference server

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

### 5. XGBoost Model

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

### 6. CatBoost Model

The CatBoost model is based on the GBDT framework with native categorical feature handling. It supports the full train/export/serve lifecycle. Training data and artifacts use the configured storage; export produces native `.cbm` format for online inference.

**Model name**: `gbdt.catboost`

Training and serving convert integer categories to decimal strings and null categories to `""`, preserving large integer ID precision. Online categories must be integers, strings or `null`; floats, booleans and arrays return HTTP 400.

**Features**:
- CatBoost-based gradient boosting decision tree
- Native categorical feature handling (no manual encoding needed); INT/INTEGER/BIGINT/STRING columns are automatically treated as categorical features, FLOAT/DOUBLE columns as numeric features. INT and INTEGER are aliases for the same integer type
- Parquet training data on distributed storage
- Model artifacts persisted to distributed storage
- Exports native .cbm format for serving (loaded via CatBoost C API, supports categorical features)
- C++ CatBoost native inference server

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

### 7. Hugging Face Transformers Model

**Model name**: `huggingface.transformers`

For this backend, `TRAIN MODEL` downloads a selected Hugging Face Hub revision and stores it through the Hadoop CLI as an origin checkpoint that can be served directly. `EXPORT MODEL` is not supported yet.

The initial tasks are `text-classification`, `text-generation`, `embedding`, and `image-embedding`. Text generation accepts plain prompts only; image embedding accepts HTTP/HTTPS URLs only.

Text classification follows the checkpoint's `problem_type`: single-label models with multiple outputs use softmax, while multi-label and single-output models use sigmoid. All return the highest-scoring label/score. Checkpoints explicitly declaring `problem_type=regression` are rejected at service load; single-output models without a declared type retain sigmoid compatibility. Input column options are case-insensitive and runtime configuration uses the declared SQL field names. Classification, text embedding and generation require non-null STRING text inputs, including configured paired text. Nulls, missing fields and non-string values return HTTP 400; empty strings remain valid inputs.

#### Hugging Face Task Options

`task` determines the SQL output fields and cannot be changed through training or service overrides; create a new model to switch tasks. Serving also checks that the checkpoint task matches the declared service task at startup.

`task` and `repo_id` are required. Declare the input column as `STRING` in the Model and select the column option for the task:

| task | Required input option | Output fields |
| --- | --- | --- |
| `text-classification` | `text_column` | `label` (STRING), `score` (FLOAT) |
| `text-generation` | `prompt_column` | `generated_text` (STRING) |
| `embedding` | `text_column` | `embedding` (ARRAY&lt;FLOAT&gt;) |
| `image-embedding` | `image_column` | `embedding` (ARRAY&lt;FLOAT&gt;) |

`repo_id` identifies the Hub repository. `revision` selects a branch, tag, or commit and defaults to `main`; pin a commit for reproducible downloads. This example defines a text embedding model:

```sql
CREATE MODEL text_embedding_model (
    text STRING
) WITH (
    'model' = 'huggingface.transformers',
    'task' = 'embedding',
    'repo_id' = 'intfloat/multilingual-e5-small',
    'text_column' = 'text',
    'pooling' = 'mean',
    'normalize' = 'true'
);

TRAIN MODEL text_embedding_model CHECKPOINT = 'v1' WITH (
    'revision' = 'main'
);

CREATE SERVICE `text-embedding-service`
    ON MODEL text_embedding_model
    CHECKPOINT = 'v1'
    WITH (
        'device' = 'auto',
        'inference_batch_size' = '32'
    );
```

Private repositories can reference a Kubernetes Secret with `hf_token_secret` and `hf_token_secret_key` in the TRAIN parameters. Serving reads only from the checkpoint and does not contact the Hub.

#### Hugging Face Service Options

Ordinary inference parameters use this precedence: explicit `CREATE SERVICE ... WITH` values, parameters explicitly declared in the Model, training parameters saved in the checkpoint, then built-in defaults. For example, if the Model declares `pooling=mean` and TRAIN overrides it to `cls`, a service without an explicit override still uses `mean`; set `pooling=cls` in SERVICE WITH to use `cls`. Parameters set only in TRAIN and absent from both the Model and SERVICE are inherited from the checkpoint. The `task` must match the Model declaration and checkpoint, and `trust_remote_code` uses the value recorded in the checkpoint.

Set these as needed in `CREATE SERVICE ... WITH (...)`:

| Parameter | Default | Usage |
| --- | --- | --- |
| `device` | `auto` | `auto`, `cpu`, or `cuda` |
| `inference_batch_size` | `8` | Internal inference batch size |
| `dtype` | `auto` | `auto`, `float32`, `float16`, or `bfloat16` |
| `pod_gpu` | `0` | GPU count; request a positive number and provide available GPU nodes for GPU inference |
| `pod_gpu_resource` | `nvidia.com/gpu` | GPU resource name in the cluster |

For GPU inference, set `'device' = 'cuda', 'pod_gpu' = '1'`. The cluster also needs compatible GPU drivers, a device plugin, and a runtime.

::: details Task-Specific Options
Text embeddings support `pooling` (default `mean`) and `normalize` (default `true`). Text classification can use `text_pair_column` for a second text column. Text generation supports `max_new_tokens` (default `128`), `do_sample` (default `false`), `temperature`, and `top_p`.

For image embeddings, `image_column` must contain HTTP/HTTPS URLs. Limit downloads with `image_url_allowed_hosts`, `image_download_timeout_ms` (default `5000`), `image_max_bytes` (default 10 MiB), and `image_max_pixels` (default 20 million).

Image downloads do not perform an additional public-IP DNS check. DNS resolution and proxy routing are handled by the HTTP client and network environment, allowing Fake-IP proxies and private image URLs. `image_url_allowed_hosts` defaults to empty, allowing any HTTP/HTTPS host. When configured, hosts must match exactly and every redirect is checked again. Download size, pixel count, timeout and redirect limits still apply. Rebuild and use the updated Transformers runtime image after changing the Python implementation.
:::
