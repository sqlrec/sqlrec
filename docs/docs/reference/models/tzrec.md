# TZRec 模型

WideAndDeep、DeepFM 用于排序，DSSM 用于召回，MMoE 用于多目标预测，RocketLaunching 用于粗排。执行步骤见[模型指南](../../guides/model-lifecycle.md)，Pod 参数见[通用资源配置](./builtin-models.md#通用流程与资源配置)。

## TZRec 配置阶段

| 位置 | 参数职责 |
| --- | --- |
| CREATE MODEL WITH | 模型类型、路径、字段和标签、特征编码、网络结构、双塔分组、MMoE 任务类型及塔尺寸；也可声明训练及资源默认值 |
| TRAIN MODEL WITH | 训练预算、优化器、读取并发、评估数据、负采样、任务权重和指标、观测及保存频率、训练拓扑 |
| EXPORT MODEL WITH | `batch_size`（默认 8192）及导出任务资源；固定单进程、一个读取 worker |
| CREATE SERVICE WITH | `replicas`、推理环境及服务资源 |

训练及资源参数按“本次操作 `WITH` > Model `WITH` > 内置默认值”取值。网络结构、特征和标签放在 `CREATE MODEL` 中，修改这些定义需要新建模型；TRAIN、EXPORT、SERVICE 可覆盖对应操作使用的默认参数。

下面是常用训练参数，可在 MODEL 中声明默认值，在 TRAIN WITH 中覆盖：

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| `batch_size` | 8192 | 每个 rank 的正样本 batch；全局规模约为 batch × nnodes × nproc_per_node，负采样候选另算 |
| `num_workers` | 8 | 每个 rank 的数据读取 worker，0 表示在训练进程读取 |
| `num_epochs` | 1 | 新任务的训练轮数；同名恢复时为累计目标轮数 |
| `sparse_lr` / `dense_lr` | 0.001 / 0.001 | Adagrad / Adam 的学习率，当前使用常数学习率 |
| `random_seed` | 不设置 | Torch 与 NumPy 的非负种子 |
| `mixed_precision` | 不设置 | 可选 BF16 或 FP16 |
| `restore_optimizer` | true | 新 FROM 任务是否恢复优化器；不同学习率需显式 false，重置全部优化器状态 |
| `eval_input_path` | 不设置 | 验证数据路径 |
| `nnodes` / `nproc_per_node` | 1 / 1 | torchrun 节点代理数及每节点进程数 |
| `master_port` | 29500 | 分布式训练端口 |

DSSM 的负采样模式、数量、池容量及 `item_id_column` 可在 MODEL 中声明，在 TRAIN 中覆盖；池是每个读取 worker 的本地缓存。MMoE 的 `task.<label>.weight/metrics` 同样支持 MODEL 默认值和 TRAIN 覆盖，任务类型及塔尺寸保留在 MODEL。新 FROM 可调整采样数量和容量、任务权重及指标，负采样模式保持与源 checkpoint 一致。

同名运行中或成功的 TRAIN 任务复用已有 checkpoint。需要新一轮训练时使用新名称；失败重试时重新提供训练表和参数，`num_epochs` 表示这次恢复训练的累计目标轮数。

## TZRec 在线推理

TZRec 服务默认使用 C++ CPU 推理。在推理 Deployment 中设置 `TZREC_SERVING_BACKEND=python` 并重启可切换到 Python，`cpp` 切回 C++；非法值会导致启动失败。模型加载和预热完成后才接收流量，可通过服务副本扩容。

已有 Kubernetes 服务可通过环境变量切换，例如：

```sh
kubectl -n sqlrec set env deployment/rank-service TZREC_SERVING_BACKEND=python
# 切回默认 C++ 实现
kubectl -n sqlrec set env deployment/rank-service TZREC_SERVING_BACKEND=cpp
```

使用 SQLRec 导出的模型。支持无权重的整数或字符串 ID、字符串数组、按分隔符拆分的字符串、连续标量、固定维度浮点向量及分桶数值特征；不支持 sequence、weighted、vocab 和 DAG 特征。

`POST /predict` 接收行数组或列式 JSON，列式数据允许长度为 1 的列广播。整列缺失返回 400；单行缺字段、null、空字符串和空数组按特征默认值处理，稀疏特征默认值为空时编码为空特征，稠密特征必须配置固定维度的数值默认值。额外的非特征列不参与推理。输入错误返回 400，模型执行错误返回 500。

推理容器需配置 Hadoop 文件系统连接，并能读取模型导出目录。`LOCAL_CACHE_DIR` 指定模型下载缓存，默认 `/tmp/tzrec_model_cache`；下载失败或导出文件无效时，服务不会启动。

## TZRec 训练曲线

默认记录 loss、学习率、可用梯度总范数及当前任务的训练指标。需要验证曲线时设置 `eval_input_path`。在 `TRAIN MODEL WITH` 中配置：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `use_tensorboard` | `true` | 控制训练期间的 TensorBoard 写入，训练日志及验证结果文件仍保留 |
| `tensorboard_summaries` | `loss,learning_rate,global_gradient_norm` | 逗号分隔的原生 summary 类型，见下文；不允许空项、重复项或未知名称 |
| `is_profiling` | `false` | 开启原生训练性能分析 trace；仅支持本地或挂载路径，远程 URI 会在训练前报错 |
| `log_step_count_steps` | `100` | 训练日志和曲线的 step 间隔，须为正整数 |
| `save_checkpoints_steps` | `1000` | 保存 checkpoint 并运行验证的 step 间隔；须为非负整数，`0` 关闭按 step 周期保存 |
| `save_checkpoints_epochs` | 不设置 | 按 epoch 保存并验证的间隔；须为非负整数，`0` 关闭按 epoch 周期保存；不能与显式设置的正数 `save_checkpoints_steps` 同时为正 |
| `enable_train_metrics` | `true` | 开启当前模型支持的训练指标；不包含仅支持验证的 NE |
| `train_metric_decay_step` | `100` | TZRec 原生默认更新周期；开启训练指标时须为日志间隔的正整数倍 |
| `train_metric_decay_rate` | `0.9` | 历史训练指标的衰减率，须为 `[0,1)` 内的有限数值 |

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

仅在训练结束时保存 checkpoint，可设置 `save_checkpoints_steps=0`，并保持 `save_checkpoints_epochs` 未设置或为 0。改用按 epoch 保存时，先将已显式配置的正数 step 间隔设为 0。

训练 event 位于 origin checkpoint 目录，验证 event 位于其 `eval_val/` 下。将目录同步或挂载到本地后运行：

```bash
tensorboard --logdir /local/rank_model/curves_v1 --port 6006
```

`tensorboard_summaries` 支持 `loss`、`learning_rate`、`global_gradient_norm`、`gradient_norm`、`parameter`、`gradient`。基础 loss 和学习率始终保留；参数和梯度直方图会增加开销，建议排障时开启。总梯度范数不包含全部分片 Embedding 梯度。

WideAndDeep/DeepFM 记录 AUC、Accuracy，验证另含 normalized entropy；MMoE 按任务记录指标；RocketLaunching 验证查看 light 分支。DSSM 的 Recall@1/5/10/20 在当前 batch 或采样候选内计算，不等于全量索引召回率。

训练指标采用历史衰减统计，不能与固定验证集指标直接比较。AUC 标签须为 0/1；WideAndDeep/DeepFM 使用软标签训练时应关闭训练指标。checkpoint 目录须放在持久存储上，以保留曲线。

`is_profiling=true` 只支持本地或挂载的模型目录，不支持 `jfs://`、`hdfs://`、`s3://` 等远程 URI。性能分析文件位于 `train_eval_trace/`，与普通标量曲线分开查看。

## TZRec 样本数据格式

TZRec 训练时，通过 `ON` 指定 Hive 表，通过 `WHERE` 选择分区；导出时指定的 `ON` 数据源也按此方式处理。SQLRec 将所选表或分区解析为存储目录，再用 `*.parquet` 筛选目录中的数据文件，避开 `_SUCCESS` 等标记文件。因此这些数据文件需采用 Parquet 格式并以 `.parquet` 结尾。文件筛选由 SQLRec 自动完成，用户按原有方式指定表和分区即可。

评估数据通过 `eval_input_path` 配置，该参数接收文件路径或通配符，支持逗号分隔的多个路径。末尾的 `/*` 会自动转换为 `/*.parquet`；直接指定文件或使用已有的 `*.parquet` 模式时，按指定路径读取。

每行是一条样本，包含模型使用的特征列及 `label_columns` 指定的标签列；列名和实际 Parquet 类型须与声明一致。标签使用数值标量，不使用字符串、布尔值或数组。所有目标标签须非空，且转换为 float32 后仍为有限数值，不能包含 NaN、Infinity 或溢出值。MMoE 每行须同时包含所有目标，不支持缺失目标标签 mask。特征类型和编码要求见下文。

样本生成流程应保证下表的标签要求；不符合要求的数据可能导致训练失败或指标失真。

| 模型 | 样本列示例（SQL 类型） | 训练和验证标签要求 |
|------|----------------------|--------------------|
| `tzrec.wide_and_deep` | `user_id BIGINT, item_id BIGINT, label FLOAT` | 训练标签在 `[0,1]`，可使用软标签；AUC 验证集使用 0/1。硬标签也可存为 INT/BIGINT |
| `tzrec.deepfm` | `user_id BIGINT, item_id BIGINT, label FLOAT` | 同 WideAndDeep：训练支持 `[0,1]` 软标签，AUC 验证集使用 0/1 |
| `tzrec.dssm` | `user_id BIGINT, item_id BIGINT, label INT` | 每行须是正样本用户/物品对，标签建议统一为 1；训练和验证均只保留正样本 |
| `tzrec.rocket_launching` | `user_id BIGINT, item_id BIGINT, click INT` | 真实曝光配对的数值 0/1 标签；训练和验证均保留正负样本，由用户提前准备 |
| `tzrec.mmoe` | `user_id BIGINT, item_id BIGINT, click INT, watch_time FLOAT` | binary 目标使用 0/1 数值标量；regression 目标声明为 FLOAT/DOUBLE，实际 Parquet 列也须为浮点类型 |

以下 SQL 展示如何从已有样本表生成所需列；输出仍需写成上述 Parquet 格式，特征列按具体模型补充：

```sql
-- WideAndDeep / DeepFM：保留正负样本；AUC 验证集也使用 0/1。
SELECT user_id, item_id, CAST(clicked AS INT) AS label
FROM source_sample;

-- DSSM：先筛选正样本，再生成数值标签列；批内其他物品提供负例。
SELECT user_id, item_id, CAST(1 AS INT) AS label
FROM source_sample WHERE clicked = 1;

-- MMoE：同一行提供所有目标，回归标签在写入 Parquet 前转换为浮点。
SELECT user_id, item_id, CAST(clicked AS INT) AS click,
       CAST(watch_time AS FLOAT) AS watch_time
FROM source_sample;
```

DSSM 原生损失把每行用户和物品的配对视为正例，标签值不用于筛选正负样本，也不作为样本权重。含 0 或负标签的行不会被训练入口拒绝或过滤，仍会被当作正样本对；应在生成训练集和验证集时完成筛选。

## TZRec 特征与模型兼容性

SQL 推理请求会保留已有字段的显式 null，全批次为空的特征仍可使用默认值。

续训和导出要求 Model 的网络、特征和标签与 checkpoint 一致。结构参数放在 MODEL 中，TRAIN/EXPORT 不能修改这些定义。

WideAndDeep、DeepFM 和 DSSM 的 `label_columns` 必须是单个标签列。标签可以声明在 Model 字段中，也可以只存在于训练表中；生成特征、Wide/Deep/FM 分组和 DSSM 推断塔时都会排除标签。训练表仍需包含标签，在线请求无需提供标签。手工指定的 DSSM 塔不能包含标签或未知列；只指定一塔时，另一塔使用剩余的非标签特征，两塔均须非空。

| SQL 类型 | 处理方式 |
|----------|----------|
| INT / INTEGER / BIGINT | 保留原始 ID，必须在 `[0, bucket_size)` 范围内；INT 与 INTEGER 是同一整数类型 |
| `VARCHAR` / `STRING` / `ARRAY<STRING>` | FarmHash ID，可使用字符串数组或按 separator 分隔的字符串 |
| FLOAT / DOUBLE | 连续 RawFeature，推理使用 float32 |
| `ARRAY<FLOAT>` / `ARRAY<DOUBLE>` | 固定维度连续向量，必须指定 `column.{name}.value_dim` |

`ARRAY<INT>` / `ARRAY<BIGINT>` 不支持多值 ID，请转换为 `ARRAY<STRING>`。布尔值、NaN/Infinity、超出 float32 范围的连续值和向量维度错误都会返回 400。每个 HTTP 请求最多 16 MiB、4096 行和 65536 个特征值；大批次请拆分。

| 列级参数 | 适用范围与说明 |
|----------|----------------|
| `column.{name}.bucket_size` | ID 桶数量，正整数；字符串列表示哈希桶数量 |
| `column.{name}.embedding_dim` | ID/分桶特征的嵌入维度须为正数且为 4 的倍数；MLP/AutoDis 须为正数 |
| `column.{name}.default_value` | ID 默认空，非空 ID 默认值始终是单个 token；连续特征默认 0，向量默认各维为 0，向量默认值用 separator 分隔 |
| `column.{name}.separator` | 单个 ASCII 字符，默认 `\035`（ASCII 29）；用于分隔多值字符串与向量默认值 |
| `column.{name}.value_dim` | 连续向量维度；连续标量必须为 1 |
| `column.{name}.normalizer` | 连续特征支持 zscore、minmax、log10，见下例 |
| `column.{name}.boundaries` | 连续特征的逗号分隔、严格递增有限边界；相等值进入右侧桶 |
| `column.{name}.embedding` | 连续特征 `none`（直接使用）、`mlp`（可学习线性投影）或 `autodis`（仅标量）；不可与 boundaries 同时使用 |
| `column.{name}.autodis.num_channels` | AutoDis 通道数，默认 3 |

Normalizer 示例：`method=zscore,mean=10,standard_deviation=2`、`method=minmax,min=0,max=100`、`method=log10,threshold=0.0001,default=-4`。`log10` 省略的 threshold、default 分别为 1、0。统计量需从训练数据预先计算；默认值位于**归一化后的空间**，缺失值不会再次执行归一化。标量分桶特征使用空默认值时，在线缺失值按未归一化的零分桶；向量分桶特征保留空行。DOUBLE 会转换为 float32，不适合要求精确小数或完整 double 精度的特征。

`ARRAY<FLOAT/DOUBLE>` 的 `value_dim=1` 分桶特征必须配置非空默认值：在线将这一维输入作为数值标量处理，空默认值的训练列表与在线标量缺失语义不同。

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

普通连续特征、MLP/AutoDis 特征进入 Deep 分组，ID 和 boundaries 分桶特征同时进入 Wide 与 Deep；DeepFM 的 FM 分组只接收稀疏特征且嵌入维度须一致。WideAndDeep/DeepFM 至少需要一个 ID 或 boundaries 分桶特征；DSSM、MMoE、RocketLaunching 可使用纯连续特征，RocketLaunching 只生成一个 Deep 分组。

`tzrec.wide_and_deep` 使用 WideAndDeep，`tzrec.deepfm` 使用 DeepFM。导出和继续训练要求 checkpoint 的架构与声明的模型一致，并保留 checkpoint 内保存的结构及特征。包含标签特征的 checkpoint 必须排除标签后重训。新 checkpoint FROM 可以设置新的学习率、轮数和批次。默认恢复优化器状态；修改学习率时必须显式设置 `restore_optimizer=false`，只恢复权重并重置全部优化器状态。改变特征或网络结构请新建并重训模型，TRAIN/EXPORT 不允许切换模型类型。

Model 中的训练及资源默认值可由对应的 TRAIN、EXPORT、SERVICE 参数覆盖。导出使用 `batch_size`，默认 8192。服务使用已完成的 export checkpoint，并须保留完整导出目录。

## Wide & Deep 模型

Wide & Deep 模型是基于 tzrec 框架实现的推荐模型，支持完整的训练、导出和服务部署流程。

**模型名称**：`tzrec.wide_and_deep`

**输出字段**：

| 字段名 | 类型 | 说明 |
|--------|------|------|
| `probs` | FLOAT | 预测概率值 |

**必需参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `label_columns` | String | 标签列名 |

**训练配置参数**：

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `embedding_dim` | Integer | 16 | 嵌入维度 |
| `num_buckets` | Integer | 1000000 | 整数特征分桶数 |
| `hidden_units` | String | "512,256,128" | 深度网络隐藏层单元数 |

训练、分布式及列级参数见本页的 TZRec 通用配置。

**使用示例**：

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

## DeepFM 模型

**模型名称**：`tzrec.deepfm`。与 Wide & Deep 使用相同的训练/特征参数，额外包含 FM 二阶交互；全部稀疏特征的 embedding_dim 必须一致。输出为 `probs`。

## MMoE 多目标排序模型

**模型名称**：`tzrec.mmoe`。共享专家网络、每个目标独立任务塔，一次推理返回全部目标。支持二分类和标量回归，也支持纯连续特征。

```sql
CREATE MODEL multi_rank (
    user_id BIGINT,
    item_id BIGINT,
    price DOUBLE,
    click INT,
    `like` INT,
    watch_time DOUBLE
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

`label_columns` 按顺序定义至少两个目标；标签必须是字段列表中不重复的数值标量列，任务名直接使用标签名，限定为字母或下划线开头的字母、数字、下划线组合。所有目标使用同一组非标签特征。线上输入只需真实特征，不必填标签。

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `num_expert` | `3` | 共享专家数量，正整数 |
| `expert_hidden_units` | `256,128` | 专家 MLP，各层维度为正整数 |
| `task_hidden_units` | `64,32` | 默认任务塔 MLP |
| `task.<label>.hidden_units` | 继承任务塔默认值 | 单个任务塔 MLP |
| `task.<label>.type` | `binary` | `binary` 使用 BCE，`regression` 使用 L2/MSE |

训练参数（在 TRAIN MODEL WITH 中设置）：

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `task.<label>.weight` | `1.0` | 有限且大于零的 float32 训练损失权重 |
| `task.<label>.metrics` | binary 为 `auc,normalized_entropy,accuracy`，regression 为 `mean_squared_error,mean_absolute_error` | 逗号分隔，可选当前任务默认列表中的子集；NE 仅用于验证，其余同步用于训练指标 |
| `eval_input_path` | 不设置 | 独立 Parquet 验证集路径，可在 TRAIN 的 WITH 中覆盖 |

资源、batch、epoch、学习率和 `column.*` 特征参数沿用 TZRec 通用配置。MMoE 使用独立的专家/任务塔参数，不能使用单目标 `hidden_units` 或 DSSM 塔参数。未知目标和未知 `task.*` 参数会在创建模型时报错。

训练和验证数据须包含每个目标的有效标签：二分类为 0/1，回归须声明为 FLOAT/DOUBLE，且 Parquet 列也须为浮点类型，数值转换为 float32 后须有限。不允许空值或缺失列。整数回归标签请在生成训练数据前转为 FLOAT/DOUBLE，避免 TZRec 的 L2 反向传播发生 dtype 错误。样本生成流程须保证这些要求，训练入口不额外扫描标签。未指定验证集时只进行训练，不自动在训练集计算验证指标。

输出为 `probs_<label> FLOAT`（二分类）或 `y_<label> FLOAT`（回归）。例如上述模型返回 `probs_click`、`probs_like`、`y_watch_time`；通过 `call_service` 追加到输入表。最终排序可在 SQL 中融合：

```sql
CACHE TABLE scored AS CALL call_service('multi-rank-service', rank_feature);
SELECT item_id, 0.6 * probs_click + 0.4 * probs_like AS rank_score
FROM scored ORDER BY rank_score DESC;
```

训练权重与线上融合系数分别配置；回归值需按业务尺度处理后再融合。所有目标共用一个 `<checkpoint>_export` 和一个服务。目标集合、类型、网络和特征由 MODEL 定义，并与 checkpoint 保持一致。新 FROM 训练可以调整任务权重、指标、epoch、batch、学习率和验证集路径。

当前不支持任务别名、多分类、缺失目标标签 mask 或任务样本空间配置。各目标须在同一训练样本空间上定义；仅点击样本定义的 CVR 需要另行设计损失与指标口径。

## RocketLaunching 粗排模型 {#rocket-launching}

**模型名称**：`tzrec.rocket_launching`。用户/物品特征联合输入，训练时 booster 和 light 同时学习曝光点击标签，light 同时接受 logit 及中间层蒸馏；线上只执行 light 路径。booster 是内部联合训练网络，不能直接加载已有精排 checkpoint 作为 teacher。

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

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `booster_hidden_units` | `256,128,64` | 内部 booster 的非空正整数层宽列表 |
| `light_hidden_units` | `64,32` | 线上 light 的非空正整数层宽列表 |
| `share_hidden_units` | 不设置 | 可选共享 MLP，设置时必须为非空正整数列表，会增加线上计算 |

booster/light 至少有一个同宽层。特征蒸馏固定开启并使用 COSINE；显式配置 false 或其他度量会报错。当前不支持关闭蒸馏、选择 loss、外部 teacher、旧 `hidden_units`、DSSM 塔参数或 MMoE 任务参数。资源、batch、epoch、学习率及 `column.*` 沿用 TZRec 通用参数。

模型字段中声明单个数值标签，训练和验证准备 0/1 曝光标签；未点击曝光可作为负样本，不能自动将未曝光候选标为 0。线上推理无需标签。

验证使用 `auc_light`、`normalized_entropy_light`、`accuracy_light`、`binary_cross_entropy_light`。原生日志可能包含未更新的 booster 评估指标（包括 NaN），不要将它们当作有效验证结果。粗排效果还须在固定候选池评估 Top K 的正样本/精排 Top M 保留率、最终指标及端到端成本，不能仅根据 AUC 决定上线。

按[模型生命周期](../../guides/model-lifecycle.md)训练、导出和部署。单个 `<checkpoint>_export` 提供 **`probs_light FLOAT`**，可能另有 `logits_light` 输出。续训、导出及创建服务时，网络、特征、标签和蒸馏配置须保持不变。

当前服务调用整批发送候选，请同时满足 4096 行、16 MiB 及特征值数量限制。双表调用减少用户特征传输重复，但不保证用户网络只计算一次。

## DSSM 模型

DSSM（Deep Structured Semantic Models）模型是基于 tzrec 框架实现的双塔召回模型，支持完整的训练、导出和服务部署流程。

DSSM 默认使用批内负采样，也支持物品池负采样。训练和验证数据均须只包含正样本对，数值标签建议统一为 1。标签值不参与正负样本筛选或样本加权；训练入口不拒绝或过滤零、负标签行，请在生成样本时先筛选正样本。具体格式见上文「TZRec 样本数据格式」。

当前 SQLRec DSSM 固定使用 `softmax_cross_entropy`，生成 Recall@1、Recall@5、Recall@10、Recall@20 验证指标；不提供训练目标选择，传入的 loss 选项会被忽略。恢复、导出保存了其他目标的 checkpoint 时会报错。训练和验证样本由用户提前准备，SQLRec 不自动过滤。

**模型名称**：`tzrec.dssm`

**输出字段**：

| 字段名 | 类型 | 说明 |
|--------|------|------|
| `user_tower_emb` | ARRAY\<FLOAT\> | 用户塔嵌入向量 |
| `item_tower_emb` | ARRAY\<FLOAT\> | 物品塔嵌入向量 |

**必需参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `user_features` | String | 用户特征列名，多个特征用逗号分隔 |
| `item_features` | String | 物品特征列名，多个特征用逗号分隔 |

**注意**：`user_features` 和 `item_features` 至少需要配置其中一个。

**训练配置参数**：

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `embedding_dim` | Integer | 16 | 嵌入维度 |
| `num_buckets` | Integer | 1000000 | 整数特征分桶数 |
| `hidden_units` | String | "512,256,128" | 深度网络隐藏层单元数 |
| `user_hidden_units` | String | "512,256,128" | 用户塔隐藏层单元数 |
| `item_hidden_units` | String | "512,256,128" | 物品塔隐藏层单元数 |
| `output_dim` | Integer | 64 | 输出嵌入维度 |

除通用参数外，DSSM 支持以下负采样参数：

| 参数 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `negative_sampling` | String | `in_batch` | `in_batch` 批内负例，或 `pool` 物品池采样 |
| `negative_sample_count` | Integer | 256 | 池采样每批共享的负例数 |
| `negative_pool_size` | Integer | 4096 | 不同物品的容量上限，须大于 `batch_size` |
| `item_id_column` | String | — | 池模式必填：`item_features` 中的标量 STRING/INT/BIGINT 物品 ID |

**负采样方式**：

DSSM 训练时需要用负例作为对照，学习让正例物品获得更高的匹配分数。通过 `negative_sampling` 选择负例的来源：

- `in_batch`（默认）：将同一批次中其他样本的物品作为负例，负例数量随批次大小变化。
- `pool`（池采样）：将训练过程中出现的物品组成候选池，每批从池中随机抽取负例。候选物品可以来自其他批次，因此使用较小的训练批次时，也能获得更多负例。

池采样直接使用现有训练数据，无需额外准备物品表。`negative_sample_count` 控制每批抽取的负例数，`negative_pool_size` 控制池中最多保留多少个不同物品，`item_id_column` 指定用于区分物品的 ID 列。池采样要求 `user_features` 和 `item_features` 不重叠，采样器只扩展物品塔字段。池只包含训练中出现的物品，不代表完整物品目录；增大池容量也不一定提升效果，应以验证结果为准。

**使用示例**：

下面启用池采样，训练批次大小为 256，每批抽取 256 个负例，物品池最多容纳 4096 个不同物品：

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

-- 按模型指南完成训练和导出后，DSSM 生成两个 tower checkpoint：
-- v1_export/item 和 v1_export/user
CREATE SERVICE `dssm-item-service`
    ON MODEL dssm_model
    CHECKPOINT = 'v1_export/item';

CREATE SERVICE `dssm-user-service`
    ON MODEL dssm_model
    CHECKPOINT = 'v1_export/user';
```

召回时，用户塔与物品塔向量应来自同一 checkpoint。物品向量可离线计算并建立 ANN 索引；更新模型后应同步更新用户塔服务、物品向量及索引，避免混用不同版本。
