# 内置模型

本文档介绍 SQLRec 内置的模型类型及其使用方法。

## 通用流程与资源配置

Model、训练、导出和 Service 的 `WITH` 选项使用字符串键和值，例如 `'batch_size' = '8192'`；SQLRec 会按参数类型解析值。

训练型模型按[模型指南](../../guides/model-lifecycle.md#训练型模型的完整流程)执行创建、训练、导出和服务部署。训练型模型的示例以 Model 定义为主，`training_sample` 等训练表需另行准备。TZRec 的模型结构放在 MODEL，训练参数放在 TRAIN，导出及服务资源分别在对应操作中设置。

下表的资源配置适用于 tzrec、GBDT 和 Hugging Face 自托管任务或服务；`replicas` 用于在线服务。

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `pod_cpu_cores` | Integer | 1 | Pod CPU 核数 |
| `pod_memory` | String | "2Gi" | Pod 内存 |
| `pod_cpu_limit` | String | - | Pod CPU 上限 |
| `pod_memory_limit` | String | - | Pod 内存上限 |
| `replicas` | Integer | 1 | 服务副本数 |

镜像默认值：tzrec 使用 `sqlrec/tzrec`，GBDT 使用 `sqlrec/gbdt`，版本为 `${SQLREC_VERSION}-cpu`；Hugging Face 使用 `sqlrec/transformers:${SQLREC_VERSION}`。可用 `image`、`version` 覆盖。

### TZRec 配置阶段

| 位置 | 参数职责 |
| --- | --- |
| CREATE MODEL WITH | 模型类型、路径、字段和标签、特征编码、网络结构、双塔分组、MMoE 任务类型及塔尺寸；也可声明训练及资源默认值 |
| TRAIN MODEL WITH | 训练预算、优化器、读取并发、评估数据、负采样、任务权重和指标、观测及保存频率、训练拓扑 |
| EXPORT MODEL WITH | `batch_size`（默认 8192）及导出任务资源；固定单进程、一个读取 worker |
| CREATE SERVICE WITH | `replicas`、推理环境及服务资源 |

训练及运行参数按本次操作 WITH > MODEL WITH > ConfigOption 默认值取值。`batch_size`、`num_workers`、学习率等可以在 MODEL 中作为默认值，在 TRAIN 中覆盖；EXPORT 和 SERVICE 同样可以覆盖模型声明的资源默认值。不读取的参数会被忽略；实际使用的参数仍须通过数值与相互约束校验。由系统生成的节点数、rank、seed 和哈希环境变量不能通过 `kubernetes.env.*` 覆盖。

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

同名运行中或成功任务直接返回已有 checkpoint。失败重试保留存储目录，按本次 TRAIN WITH、MODEL 默认值及 ConfigOption 默认值重新生成配置，需要重新提供 ON 和本次训练所需参数，不会自动恢复原 SQL 参数或检查是否与上次一致。已有保存点时由 TZRec 恢复模型、优化器状态及数据进度。`num_epochs` 表示同名恢复的累计目标轮数；新 FROM 任务使用新的训练轮数预算。运行中或成功任务不会因新的参数重新启动。

### TZRec 在线推理

TZRec 服务默认使用 C++ CPU 推理；设置环境变量 `TZREC_SERVING_BACKEND=python` 可切换为 Python serving，设置为 `cpp` 或不设置则使用 C++。切换在进程重启后生效，非法值会导致启动失败。两种后端共用 `--scripted_model_dir`、`--host`、`--port` 参数以及远程模型下载缓存。初始化完成后 `/health` 返回成功，Kubernetes startup/readiness probe 才会允许接收请求；C++ 后端还会在监听前执行预热。DSSM 用户塔和物品塔分别使用各自导出子目录。

已有 Kubernetes 服务可通过环境变量切换，例如：

```sh
kubectl -n sqlrec set env deployment/rank-service TZREC_SERVING_BACKEND=python
# 切回默认 C++ 实现
kubectl -n sqlrec set env deployment/rank-service TZREC_SERVING_BACKEND=cpp
```

导出目录必须包含 `scripted_model.pt`、`fg.json` 和 `pipeline.config`。预处理以导出的 `fg.json` 为准：`num_buckets` 保留原始整数 ID，要求 ID 在桶范围内；`hash_bucket_size` 使用 FarmHash Fingerprint64。当前支持普通无权重 ID 特征、整数或字符串标量、字符串数组以及按 `separator` 分隔的多值字符串；还支持连续 raw 标量、固定维度浮点向量及分桶 raw 特征；sequence、weighted、vocab 和 DAG 配置会在启动时明确报错。

`POST /predict` 接收行数组或列式 JSON，列式数据允许长度为 1 的列广播。整列缺失返回 400；单行缺字段、null、空字符串和空数组按特征默认值处理，稀疏特征默认值为空时编码为空特征，稠密特征必须配置固定维度的数值默认值。额外的非特征列不参与推理。输入错误返回 400，模型执行错误返回 500。

`server.sh` 使用 `hadoop fs -get` 下载远程导出目录，优先使用 `$HADOOP_HOME/bin/hadoop`，否则使用 PATH 中的 `hadoop`。容器中的 Hadoop 客户端需要配置好对应文件系统（例如 HDFS 或 JuiceFS）的连接和认证；本地目录直接使用。`LOCAL_CACHE_DIR` 可指定远程模型缓存的父目录，默认 `/tmp/tzrec_model_cache`；每次启动使用独立子目录，下载失败或缺少必要文件时清理该目录并退出。两种后端统一通过 `server.sh` 启动，由脚本调用一次 `validate_export.py` 检查清单、完成标记、文件校验和及 pipeline 契约；校验通过后使用 `exec` 启动所选 serving，校验失败会清理本次下载的独立缓存目录。`server.py` 负责模型加载与推理预热，不重复执行导出文件校验。Python 后端使用 Torch 和 Python FG 进行特征处理与推理；ARM Python FG 兼容包支持与 SQLRec 生成配置一致的 ID/raw 特征范围。两种后端均在预热成功后就绪，串行执行推理；通过服务副本扩容。

### TZRec 训练曲线

SQLRec 仅生成 TZRec 原生的 `train_config`、`metrics` 和 `train_metrics` 配置，采集和写入由 TZRec 自身完成，无须修改 TZRec 源码。默认保存 TensorBoard loss、学习率、可用梯度的总范数和适合当前任务的训练指标曲线；配置 `eval_input_path` 后同时保存验证指标。下列参数适用于所有内置 TZRec 模型，在 `TRAIN MODEL WITH` 中设置：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `use_tensorboard` | `true` | 控制训练期间的 TensorBoard 写入，训练日志及验证结果文件仍保留 |
| `tensorboard_summaries` | `loss,learning_rate,global_gradient_norm` | 逗号分隔的原生 summary 类型，详见下表；不允许空项、重复项或未知名称 |
| `is_profiling` | `false` | 开启原生训练性能分析 trace；仅支持本地或挂载路径，远程 URI 会在训练前报错 |
| `log_step_count_steps` | `100` | 训练日志和曲线的 step 间隔，须为正整数 |
| `save_checkpoints_steps` | `1000` | 保存 checkpoint 并运行验证的 step 间隔；须为非负整数，`0` 关闭按 step 周期保存 |
| `save_checkpoints_epochs` | 不设置 | 按 epoch 保存并验证的间隔；须为非负整数，`0` 关闭按 epoch 周期保存；不能与显式设置的正数 `save_checkpoints_steps` 同时为正 |
| `enable_train_metrics` | `true` | 开启当前模型支持的训练指标；不包含仅支持验证的 NE |
| `train_metric_decay_step` | `100` | TZRec 原生默认更新周期；开启训练指标时须为日志间隔的正整数倍 |
| `train_metric_decay_rate` | `0.9` | 历史训练指标的衰减率，须为 `[0,1)` 内的有限数值 |

仅在训练结束时保存 checkpoint 并验证，可设置 `save_checkpoints_steps=0`，同时保持 `save_checkpoints_epochs` 未设置或为 `0`。切换已声明的保存策略时，在 `TRAIN MODEL WITH` 中将原间隔设为 `0`，再设置另一种正数间隔。只设置正数 epoch 间隔时，会自动关闭默认的 step 间隔；epoch 间隔为 `0` 时保留 step 间隔。

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

沿用当前 TZRec 版本的原生目录：训练 event 直接保存在本次 origin 的 `model_dir` 下，训练中的验证 event 保存在 `model_dir/eval_val/`，独立评估 event 保存在 `model_dir/eval/`，性能分析 trace 位于 `model_dir/train_eval_trace/`。当前版本没有独立的日志目录配置。`pipeline.config`、`model.ckpt-*` 和逐行 JSON 格式的 `train_eval_result_v2.txt` 也保存在训练目录。将本次训练目录挂载或同步到 `/local/rank_model/curves_v1` 后运行：

```bash
tensorboard --logdir /local/rank_model/curves_v1 --port 6006
```

`tensorboard_summaries` 支持以下六种原生类型，采集频率均为 `log_step_count_steps`。TZRec 始终保留基础 loss 和学习率，`train_metrics` 单独控制训练指标；例如只填写 `gradient_norm` 时仍有 loss、学习率，但不再采集总梯度范数。

| 类型 | TensorBoard 内容 | 默认开启 |
|------|-------------------|----------|
| `loss` | `loss/total` 及各项 `loss/*`，包括 MMoE 任务 loss 和 RocketLaunching 蒸馏 loss | 是 |
| `learning_rate` | 优化器参数组的 `lr/g*` | 是 |
| `global_gradient_norm` | `global_gradient_norm` 标量，统计当前可见梯度的 L2 总范数 | 是 |
| `gradient_norm` | `<参数名>/gradient_norm`，逐参数梯度 L2 范数 | 否 |
| `parameter` | 参数值直方图，位于 Histograms / Distributions | 否 |
| `gradient` | `<参数名>/gradient` 梯度直方图 | 否 |

梯度和参数 summary 只由 rank 0 写入。总梯度范数排除 `ShardedTensor`，且仅统计 `.grad` 非空的参数；分片 Embedding / fused optimizer 的梯度可能不在其中，不能将其解释为全卡、全参数范数。分片参数直方图只记录本地 shard。参数和梯度直方图可能明显增加采集开销及 event 大小，建议排障时开启；当前原生接口没有独立的直方图频率或参数白名单。

```sql
-- 排障时开启全部原生曲线与直方图，训练指标开关保持独立。
TRAIN MODEL rank_model CHECKPOINT = 'diagnostics_v1'
ON training_sample
WITH (
  'tensorboard_summaries' = 'loss,learning_rate,global_gradient_norm,gradient_norm,parameter,gradient',
  'log_step_count_steps' = '100'
);
```

仅保留基础 loss 和学习率时，设置 `'tensorboard_summaries' = 'loss,learning_rate'`。`use_tensorboard=false` 会关闭所有训练期间的 summary 写入，但不会关闭 `is_profiling`。

`is_profiling=true` 仅支持本地文件系统或容器内挂载路径的 `model_dir`，不支持 `jfs://`、`hdfs://`、`s3://` 等远程 URI。原生 Profiler 使用 C++ Kineto 导出 trace，不经过 Python 的 fsspec，JuiceFS 实测会报 `No logger registered for the jfs protocol prefix`。SQLRec 在提交训练任务及训练入口校验该限制；远程目录的普通 TensorBoard 曲线、梯度范数及直方图不受影响。

Profiler 使用当前固定 TZRec 版本的原生采集窗口：等待 1 步、预热 1 步、记录 10 步，单次采集输入 shape、内存及调用栈等信息，输出到 `model_dir/train_eval_trace/`。当前 SQLRec 没有透出其他版本中的 `profiler_config`。trace 是独立文件，下载时需连同 `train_eval_trace/` 保存，并使用兼容的性能分析工具查看，不能作为普通 Scalars 曲线打开。

当前固定的 TZRec 版本支持以下 10 类验证指标，其中 6 类也支持训练采集；模型本身还会限制可用指标。完整配置见 [TZRec 官方指标文档](https://torcheasyrec.readthedocs.io/zh-cn/latest/models/evaluation_metrics.html)。SQLRec 按输出类型选择默认值：

| 原生指标 | 支持训练 | SQLRec 默认选择及用途 |
|----------|----------|----------------------|
| `auc` | 是 | 二分类默认开启，衡量正负样本排序能力，越高越好 |
| `normalized_entropy` | 否 | 二分类验证默认开启，衡量相对于标签均值基线的交叉熵，越低越好；小于 1 表示优于基线 |
| `accuracy` | 是 | 二分类默认开启，原生阈值为 0.5；只作为辅助观察，类别不平衡时不能代替 AUC |
| `mean_squared_error` | 是 | MMoE 回归任务默认开启，观察平方误差，越低越好 |
| `mean_absolute_error` | 是 | MMoE 回归任务默认开启，观察与标签同量纲的绝对误差，越低越好 |
| `recall_at_k` | 是 | DSSM 默认开启 K=1、5、10、20，越高越好 |
| `multiclass_auc` | 否 | 原生支持多分类；SQLRec 当前排序任务为标量二分类或回归，不默认配置 |
| `grouped_auc` | 否 | 需要明确的 `grouping_key` 及对应特征，不自动猜测用户分组 |
| `xauc` | 是 | 连续值目标的成对排序指标，需要按数据规模选择采样和样本对上限，不默认开启 |
| `grouped_xauc` | 否 | 需要分组字段和组内样本对配置，不默认开启 |

训练曲线包含 `loss/total`、各项 `loss/*` 和 `lr/g*`。WideAndDeep/DeepFM 默认记录 `metric/auc`、`metric/accuracy`，验证另有 `metric/normalized_entropy`；DSSM 默认记录 `metric/recall@1`、`metric/recall@5`、`metric/recall@10`、`metric/recall@20`。MMoE 按各任务的 `task.<label>.metrics` 记录带任务名后缀的指标，如 `metric/auc_label`、`metric/normalized_entropy_label`、`metric/mean_absolute_error_watch_time`；NE 仅出现在验证中。RocketLaunching 训练时记录 booster 和 light 的 AUC、Accuracy；验证只执行 light 分支，应查看 `metric/auc_light`、`metric/accuracy_light` 和 `metric/normalized_entropy_light`，原生结果中可能包含未更新的 booster 占位指标。

验证 loss 位于 `metric/*`，如 `metric/binary_cross_entropy`、`metric/l2_loss_watch_time`，由原生流程自动记录，无须重复配置 logloss。DSSM 的 Recall 在当前 batch/采样候选集合上计算，不等同于全量索引召回；K 不小于候选数时会饱和。需要配置独立的 `eval_input_path` 才会产生验证曲线，验证采样频率与 checkpoint 保存频率一致；短任务可能只有结束时的一个验证点。

训练 loss 是日志采样时的 batch loss，分布式时由 rank 0 记录，并非全局 epoch 平均值。训练指标采用历史衰减统计，新建指标在首个更新周期完成前为 0，与固定验证集上的指标口径不同。未显式设置衰减参数时，SQLRec 使用 TZRec 的默认值 `decay_step=100`、`decay_rate=0.9`。AUC 的训练与验证标签均须为 0/1；使用软标签训练 WideAndDeep/DeepFM 时，应关闭训练指标。

训练频率、训练指标和验证指标可以在从旧 checkpoint 开始的新一轮训练中覆盖，网络、特征及任务结构仍由 checkpoint 保持。writer 的生命周期、缓冲和训练指标恢复均沿用所固定 TZRec 版本的原生行为：fine-tune 可能继承 checkpoint 中已有的训练指标统计；训练期间只由 rank 0 写入 event。当前版本的独立 `tzrec.eval` 命令会独立创建 `eval/` writer，不受训练开关控制；该命令未显式关闭 writer，异步 summary 可能未落盘，独立评估以 `eval_result.txt` 为准。SQLRec 训练曲线优先查看 `eval_val/`。

`model_dir` 须位于持久存储或持久挂载上，否则容器删除后曲线会丢失。远程 URI 需要训练端和 TensorBoard 查看端均具备对应文件系统依赖、连接和认证；`USE_FSSPEC=1` 本身不保证 TensorBoard 远程读写可用，可先同步到本地查看。日志随 origin 训练目录保留，导出模型不负责归档训练曲线。

### TZRec 样本数据格式

TZRec 训练时，通过 `ON` 指定 Hive 表，通过 `WHERE` 选择分区；导出时指定的 `ON` 数据源也按此方式处理。SQLRec 将所选表或分区解析为存储目录，再用 `*.parquet` 筛选目录中的数据文件，避开 `_SUCCESS` 等标记文件。因此这些数据文件需采用 Parquet 格式并以 `.parquet` 结尾。文件筛选由 SQLRec 自动完成，用户按原有方式指定表和分区即可。

评估数据通过 `eval_input_path` 配置，该参数接收文件路径或通配符，支持逗号分隔的多个路径。末尾的 `/*` 会自动转换为 `/*.parquet`；直接指定文件或使用已有的 `*.parquet` 模式时，按指定路径读取。

每行是一条样本，包含模型使用的特征列及 `label_columns` 指定的标签列；列名和实际 Parquet 类型须与声明一致。标签使用数值标量，不使用字符串、布尔值或数组。所有目标标签须非空，且转换为 float32 后仍为有限数值，不能包含 NaN、Infinity 或溢出值。MMoE 每行须同时包含所有目标，不支持缺失目标标签 mask。特征类型和编码要求见下文。

**训练入口不额外扫描训练集或验证集的标签，也不进行分布式标签校验同步。** 下列格式要求由样本生成流程保证；数据会直接交给 TZRec 原生训练流程读取，不符合要求的样本可能导致训练报错或影响损失和指标。

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

### TZRec 特征与模型兼容性

SQL 推理请求会保留已有字段的显式 null，全批次为空的特征仍可使用默认值。

续训和导出会检查 MODEL 声明的网络、特征和标签契约与 checkpoint 一致。连续特征默认值及 normalizer 按数值含义比较；ID 默认值和分隔符严格比较。TRAIN/EXPORT 中的结构参数被忽略，修改结构需新建模型并重新训练。

连续特征的标量/向量默认值和 normalizer 数值参数按 float32 精度比较，向量元素顺序和维度必须一致。normalizer 参数顺序不影响比较；`log10` 未指定的 `threshold`、`default` 分别按 `1`、`0` 比较，与原版特征生成器一致。等价配置通过后仍沿用 checkpoint 中保存的原始配置。此规则同样适用于 MMoE 的特征比较；任务及网络结构仍须一致。

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
| `column.{name}.default_value` | ID 默认空；连续特征默认 0，向量默认各维为 0；多值默认用 separator 分隔 |
| `column.{name}.separator` | 单个 ASCII 字符，默认 `\035`（ASCII 29）；用于分隔多值字符串与向量默认值 |
| `column.{name}.value_dim` | 连续向量维度；连续标量必须为 1 |
| `column.{name}.normalizer` | 连续特征支持 zscore、minmax、log10，见下例 |
| `column.{name}.boundaries` | 连续特征的逗号分隔、严格递增有限边界；相等值进入右侧桶 |
| `column.{name}.embedding` | 连续特征 `none`（直接使用）、`mlp`（可学习线性投影）或 `autodis`（仅标量）；不可与 boundaries 同时使用 |
| `column.{name}.autodis.num_channels` | AutoDis 通道数，默认 3 |

Normalizer 示例：`method=zscore,mean=10,standard_deviation=2`、`method=minmax,min=0,max=100`、`method=log10,threshold=0.0001,default=-4`。统计量需从训练数据预先计算；默认值位于**归一化后的空间**，缺失值不会再次执行归一化。DOUBLE 会转换为 float32，不适合要求精确小数或完整 double 精度的特征。

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

TZRec 的训练和资源参数支持 MODEL 默认值及操作 WITH 覆盖；导出使用相同的 `batch_size` 参数名，固定单进程及一个读取 worker。新导出先写独立 staging 目录，生成 SHA256 清单 `model_meta.json` 和 `_SUCCESS` 后发布到目标目录；目标目录已存在时拒绝覆盖。服务启动时所有模型都必须具备清单和完成标记，并通过文件校验和检查。HDFS/JuiceFS 上依赖目录 rename 的发布语义。

本地构建和镜像发布均执行 `bin/verify_tzrec_image.sh`：检查依赖/动态库、C++ 算子加载、输出类型，并真实训练导出 WideAndDeep、DeepFM、DSSM、MMoE、RocketLaunching，再对照 Python/C++ 特征张量及 HTTP 预测。DSSM 覆盖 softmax 召回训练、续训及双塔导出；MMoE 覆盖混合任务、纯回归、连续特征、续训和双进程训练；RocketLaunching 覆盖梯度、仅 light 推理、共享层、连续特征、续训和双进程训练。ARM 兼容包另有单元测试及原生 x86 官方 pyfg 差分检查。

## 内置模型类型

SQLRec 内置了以下模型类型：

### 1. 外部模型

外部模型用于对接已有的外部模型服务，不支持训练和导出操作。

**模型名称**：`external`

**特性**：
- 连接外部已有的模型推理服务
- 不支持训练（`TRAIN MODEL`）
- 不支持导出（`EXPORT MODEL`）
- 通过 URL 直接访问服务

**配置参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `output_columns` | String | 输出列定义，格式：`name1:type1,name2:type2` |

`output_columns` 是 Model 配置；`url` 是 Service 配置，用于指定实际调用的推理地址。

**使用示例**：

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

### 2. Wide & Deep 模型

Wide & Deep 模型是基于 tzrec 框架实现的推荐模型，支持完整的训练、导出和服务部署流程。

**模型名称**：`tzrec.wide_and_deep`

**特性**：
- 支持 Wide & Deep 架构的推荐模型
- 支持分布式训练（PyTorch Distributed）
- 支持 Parquet 格式的训练数据
- 自动生成 Kubernetes 训练和服务 YAML
- 支持稀疏特征和稠密特征

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

训练参数（在 TRAIN MODEL WITH 中设置）：

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `sparse_lr` | Double | 0.001 | 稀疏特征学习率 |
| `dense_lr` | Double | 0.001 | 稠密特征学习率 |
| `num_epochs` | Integer | 1 | 训练轮数 |
| `batch_size` | Integer | 8192 | 批次大小 |
| `num_workers` | Integer | 8 | 数据加载工作进程数 |
| `mixed_precision` | String | - | 混合精度训练模式，可选 `BF16`/`FP16`，默认不开启 |

**分布式训练参数**：

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `nnodes` | Integer | 1 | 训练节点数 |
| `nproc_per_node` | Integer | 1 | 每节点进程数 |
| `master_port` | Integer | 29500 | 分布式训练主端口 |

**列级配置参数**：

可以为每个特征列单独配置参数：

| 参数格式 | 说明 |
|----------|------|
| `column.{feature_name}.bucket_size` | 特征的分桶数量 |
| `column.{feature_name}.embedding_dim` | 特征的嵌入维度 |

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

### 2.1 DeepFM 模型

**模型名称**：`tzrec.deepfm`。与 Wide & Deep 使用相同的训练/特征参数，额外包含 FM 二阶交互；全部稀疏特征的 embedding_dim 必须一致。输出为 `probs`。

### 2.2 MMoE 多目标排序模型

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

### 2.3 RocketLaunching 粗排模型 {#rocket-launching}

**模型名称**：`tzrec.rocket_launching`。用户/物品特征联合输入，训练时 booster 和 light 同时学习曝光点击标签，light 同时接受 logit 及中间层蒸馏；线上只执行 light 路径。booster 是内部联合训练网络，不能直接加载已有精排 checkpoint 作为 teacher。沿用原生实现，不修改第三方源码。

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

单个数值标签须声明在模型字段中，训练/验证文件中的值由用户保证为 0/1；不自动扫描、过滤或生成负例。已曝光未点击记录可作为负样本，未曝光候选不能直接标为 0。固定 BCE 和 num_class=1；标签不进入特征，线上无需提供标签。原生 light 输入被 detach，共享 embedding/share_mlp 由 booster 的监督梯度更新，light 的监督与蒸馏梯度不更新共享输入部分。

验证使用 `auc_light`、`normalized_entropy_light`、`accuracy_light`、`binary_cross_entropy_light`。原生日志可能包含未更新的 booster 评估指标（包括 NaN），不要将它们当作有效验证结果。粗排效果还须在固定候选池评估 Top K 的正样本/精排 Top M 保留率、最终指标及端到端成本，不能仅根据 AUC 决定上线。

训练、导出及服务部署沿用[模型生命周期](../../guides/model-lifecycle.md)。只生成 `<checkpoint>_export`，公共输出为 **`probs_light FLOAT`**；原生 `logits_light` 作为额外输出保留。导出前验证 light 输出形状、范围与有限性，产物要求匹配的 output manifest。TRAIN/EXPORT/SERVICE 不能改变网络、特征、标签或蒸馏配置；续训可调整学习率、batch、epoch 和验证集路径。

当前服务调用整批发送候选，请同时满足 4096 行、16 MiB 及特征值数量限制。双表调用减少用户特征传输重复，但不保证用户网络只计算一次。

### 3. DSSM 模型

DSSM（Deep Structured Semantic Models）模型是基于 tzrec 框架实现的双塔召回模型，支持完整的训练、导出和服务部署流程。

DSSM 默认使用批内负采样，也支持物品池负采样。训练和验证数据均须只包含正样本对，数值标签建议统一为 1。标签值不参与正负样本筛选或样本加权；训练入口不拒绝或过滤零、负标签行，请在生成样本时先筛选正样本。具体格式见上文「TZRec 样本数据格式」。

当前 SQLRec DSSM 固定使用 `softmax_cross_entropy`，生成 Recall@1、Recall@5、Recall@10、Recall@20 验证指标；不提供训练目标选择，传入的 loss 选项会被忽略。恢复、导出保存了其他目标的 checkpoint 时会报错。训练和验证样本由用户提前准备，SQLRec 不自动过滤。

**模型名称**：`tzrec.dssm`

**特性**：
- 支持双塔架构的召回模型
- 用户塔和物品塔分别生成嵌入向量
- 支持分布式训练（PyTorch Distributed）
- 支持 Parquet 格式的训练数据
- 自动生成 Kubernetes 训练和服务 YAML
- 支持稀疏特征和稠密特征

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

训练参数（在 TRAIN MODEL WITH 中设置）：

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `sparse_lr` | Double | 0.001 | 稀疏特征学习率 |
| `dense_lr` | Double | 0.001 | 稠密特征学习率 |
| `num_epochs` | Integer | 1 | 训练轮数 |
| `batch_size` | Integer | 8192 | 批次大小 |
| `num_workers` | Integer | 8 | 数据加载工作进程数 |
| `negative_sampling` | String | `in_batch` | `in_batch` 批内负例，或 `pool` 物品池采样 |
| `negative_sample_count` | Integer | 256 | 池采样每批共享的负例数 |
| `negative_pool_size` | Integer | 4096 | 不同物品的容量上限，须大于 `batch_size` |
| `item_id_column` | String | — | 池模式必填：`item_features` 中的标量 STRING/INT/BIGINT 物品 ID |
| `mixed_precision` | String | - | 混合精度训练模式，可选 `BF16`/`FP16`，默认不开启 |

**分布式训练参数**：

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `nnodes` | Integer | 1 | 训练节点数 |
| `nproc_per_node` | Integer | 1 | 每节点进程数 |
| `master_port` | Integer | 29500 | 分布式训练主端口 |

**列级配置参数**：

可以为每个特征列单独配置参数：

| 参数格式 | 说明 |
|----------|------|
| `column.{feature_name}.bucket_size` | 特征的分桶数量 |
| `column.{feature_name}.embedding_dim` | 特征的嵌入维度 |

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
-- v1.0_export/item 和 v1.0_export/user
CREATE SERVICE `dssm-item-service`
    ON MODEL dssm_model
    CHECKPOINT = 'v1.0_export/item';

CREATE SERVICE `dssm-user-service`
    ON MODEL dssm_model
    CHECKPOINT = 'v1.0_export/user';
```

召回时，用户塔与物品塔向量应来自同一 checkpoint。物品向量可离线计算并建立 ANN 索引；更新模型后应同步更新用户塔服务、物品向量及索引，避免混用不同版本。

### 4. LightGBM 模型

LightGBM 模型是基于 GBDT（梯度提升决策树）框架实现的模型，支持完整的训练、导出和服务部署流程。训练数据和模型文件使用配置的存储，导出时转换为 ONNX 格式用于在线推理。

LightGBM 训练特征先转换为 float32，与 ONNX 和在线服务使用同样的精度；保留 NaN 缺失值，拒绝 Infinity 或 float32 溢出。旧版使用 double 训练的模型需要重新训练，才能保证这项精度一致性。

**模型名称**：`gbdt.lightgbm`

GBDT 的 TRAIN、EXPORT 和 SERVICE 可覆盖运行参数及资源配置，但不能改变 Model 声明的模型类型或 `label_columns`；显式传入相同值允许执行，传入不同值会报错。改变模型类型或标签须新建模型。这项规则适用于 LightGBM、XGBoost 和 CatBoost。

GBDT 在线推理的缺失数值字段和 `null` 在服务内部转换为 float32 `NaN`，供模型按缺失值处理，与训练时一致；`0` 是有效数值。客户端传 `null` 或省略字段即可，JSON 中不要发送 `NaN`。数值字符串须是完整的有限 float32 十进制数，非法值返回 HTTP 400。服务继承模型声明中的镜像、版本和资源配置，服务参数可覆盖这些配置；模型下载、加载完成且 `/health` 探针成功后才接收流量。此行为也适用于 XGBoost 和 CatBoost。

`POST /predict` 接收非空的对象行数组或列式 JSON；列式值必须为数组，长度须一致，长度为 1 的列可广播。JSON 解析失败、行元素不是对象、列值不是数组、列长度不兼容及空批次均返回 HTTP 400；模型执行抛出的运行时异常返回 HTTP 500。错误响应使用 JSON 格式，包含 `error` 字段。

**特性**：
- 基于 LightGBM 框架的梯度提升树模型
- 仅支持浮点数值特征（float/double），不支持类别特征（如需类别/整数特征请使用 CatBoost）
- 支持 Parquet 格式训练数据（存储在分布式存储）
- 模型文件持久化到分布式存储
- 导出 ONNX 格式用于 serving（通过 onnxmltools 转换）
- C++ ONNX Runtime 推理服务

**输出字段**：

| 字段名 | 类型 | 说明 |
|--------|------|------|
| `probs` | FLOAT | 二分类正类概率；回归预测值 |

**必需参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `label_columns` | String | 单个标签列名，不能包含逗号或首尾空白 |

**训练配置参数**：

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `objective` | String | "binary" | 学习目标（binary, regression） |
| `metric` | String | "auc" | 评估指标（auc, logloss, rmse） |
| `num_iterations` | Integer | 300 | boosting 迭代次数 |
| `learning_rate` | Double | 0.1 | 学习率 |
| `num_leaves` | Integer | 63 | 每棵树最大叶子数 |
| `max_depth` | Integer | 6 | 最大树深度 |
| `feature_fraction` | Double | 0.8 | 每棵树使用的特征比例 |
| `bagging_fraction` | Double | 0.8 | 每棵树使用的数据比例 |
| `bagging_freq` | Integer | 5 | bagging 频率 |
| `min_data_in_leaf` | Integer | 20 | 叶子节点最小样本数 |
| `l2_regularization` | Double | 1.0 | L2 正则化系数 |

**使用示例**：

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

### 5. XGBoost 模型

XGBoost 模型是基于 GBDT（梯度提升决策树）框架实现的模型，支持完整的训练、导出和服务部署流程。训练数据和模型文件均存储在分布式存储上，导出时转换为 ONNX 格式用于在线推理。

**模型名称**：`gbdt.xgboost`

**特性**：
- 基于 XGBoost 框架的梯度提升树模型
- 仅支持浮点数值特征（float/double）
- 支持 Parquet 格式训练数据（存储在分布式存储）
- 模型文件持久化到分布式存储
- 导出 ONNX 格式用于 serving（通过 onnxmltools 转换）
- C++ ONNX Runtime 推理服务

**输出字段**：

| 字段名 | 类型 | 说明 |
|--------|------|------|
| `probs` | FLOAT | 二分类正类概率；回归预测值 |

**必需参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `label_columns` | String | 单个标签列名，不能包含逗号或首尾空白 |

**训练配置参数**：

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `objective` | String | "binary" | 学习目标（binary, regression） |
| `metric` | String | "auc" | 评估指标（auc, logloss, rmse） |
| `num_iterations` | Integer | 300 | boosting 迭代次数 |
| `learning_rate` | Double | 0.1 | 学习率 |
| `max_depth` | Integer | 6 | 最大树深度 |
| `feature_fraction` | Double | 0.8 | 每棵树使用的特征比例（对应 XGBoost colsample_bytree） |
| `bagging_fraction` | Double | 0.8 | 每棵树使用的数据比例（对应 XGBoost subsample） |
| `min_child_weight` | Integer | 1 | 子节点最小权重和 |
| `l2_regularization` | Double | 1.0 | L2 正则化系数（对应 XGBoost reg_lambda） |

**使用示例**：

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

### 6. CatBoost 模型

CatBoost 模型是基于 GBDT 框架实现的模型，原生支持类别特征处理，支持完整的训练、导出和服务部署流程。训练数据和模型文件使用配置的存储，导出原生 `.cbm` 格式用于在线推理。

**模型名称**：`gbdt.catboost`

训练和在线推理统一将整数类别值转换为十进制字符串，将空类别值转换为 `""`，保留大整数 ID 的精度。在线类别值须为整数、字符串或 `null`；浮点数、布尔值和数组返回 HTTP 400。

**特性**：
- 基于 CatBoost 框架的梯度提升树模型
- 原生支持类别特征处理（无需手动编码）；INT/INTEGER/BIGINT/STRING 类型的列自动作为类别特征，FLOAT/DOUBLE 类型的列作为数值特征；INT 与 INTEGER 是同一整数类型
- 支持 Parquet 格式训练数据（存储在分布式存储）
- 模型文件持久化到分布式存储
- 导出原生 .cbm 格式用于 serving（通过 CatBoost C API 直接加载，支持类别特征）
- C++ CatBoost 原生推理服务

**输出字段**：

| 字段名 | 类型 | 说明 |
|--------|------|------|
| `probs` | FLOAT | 二分类正类概率；回归预测值 |

**必需参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `label_columns` | String | 单个标签列名，不能包含逗号或首尾空白 |

**训练配置参数**：

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `objective` | String | "binary" | 学习目标（binary, regression） |
| `metric` | String | "auc" | 评估指标（auc, logloss, rmse） |
| `cb_iterations` | Integer | 1000 | CatBoost 迭代次数 |
| `cb_depth` | Integer | 6 | CatBoost 树深度 |
| `cb_l2_leaf_reg` | Double | 3.0 | L2 叶子正则化系数 |
| `learning_rate` | Double | 0.1 | 学习率 |

**使用示例**：

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

### 7. Hugging Face Transformers 模型

**模型名称**：`huggingface.transformers`

该模型类型把 `TRAIN MODEL` 定义为从 Hugging Face Hub 下载指定 revision，并通过 Hadoop CLI 将快照保存为可直接部署的 origin checkpoint。暂不支持 `EXPORT MODEL`。

首期任务包括 `text-classification`、`text-generation`、`embedding` 和 `image-embedding`。文本生成仅接受普通 prompt；图片 embedding 仅接受 HTTP/HTTPS URL。

文本分类根据 checkpoint 的 `problem_type` 计算分数：单标签多输出分类使用 softmax，多标签分类和单输出模型使用 sigmoid，均返回得分最高的一个 label/score。明确声明 `problem_type=regression` 的 checkpoint 在服务加载时被拒绝；未声明类型的单输出模型保留 sigmoid 兼容行为。输入列参数不区分大小写，运行配置使用 SQL 声明的字段名。分类、文本 embedding 和生成任务的文本及配置的配对文本字段必须为非空值的 STRING，null、缺字段或非字符串返回 HTTP 400；空字符串仍是有效输入。

#### Hugging Face 任务配置

`task` 决定 SQL 输出字段，训练和服务参数不能改变它；如需切换任务，请新建模型。服务启动时也会检查 checkpoint 中的任务是否与服务声明一致。

`task` 和 `repo_id` 必填。输入列必须在 Model 字段列表中声明为 `STRING`，并按任务选择列名配置：

| task | 必填输入配置 | 输出字段 |
| --- | --- | --- |
| `text-classification` | `text_column` | `label`（STRING）、`score`（FLOAT） |
| `text-generation` | `prompt_column` | `generated_text`（STRING） |
| `embedding` | `text_column` | `embedding`（ARRAY&lt;FLOAT&gt;） |
| `image-embedding` | `image_column` | `embedding`（ARRAY&lt;FLOAT&gt;） |

`repo_id` 是 Hub 仓库名称；`revision` 可指定分支、标签或 commit，默认 `main`。固定 commit 可让下载版本更容易复现。下面定义一个文本 embedding 模型：

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

私有仓库可在 TRAIN 参数中通过 `hf_token_secret` 和 `hf_token_secret_key` 引用 Kubernetes Secret。服务仅从 checkpoint 加载模型，不访问 Hub。

#### Hugging Face 服务配置

普通推理参数按 `CREATE SERVICE ... WITH` 显式参数、Model 中显式声明的参数、checkpoint 保存的训练参数、内置默认值的顺序取值，前者优先。例如 Model 声明 `pooling=mean`、TRAIN 覆盖为 `cls` 时，未覆盖该参数的服务仍使用 `mean`；如需使用 `cls`，在 SERVICE 的 WITH 中显式设置。仅在 TRAIN 中设置且 Model、SERVICE 均未声明的参数会从 checkpoint 继承。`task` 必须与模型声明和 checkpoint 一致；`trust_remote_code` 使用 checkpoint 记录的值。

在 `CREATE SERVICE ... WITH (...)` 中按需设置：

| 参数 | 默认值 | 用法 |
| --- | --- | --- |
| `device` | `auto` | 可选 `auto`、`cpu`、`cuda` |
| `inference_batch_size` | `8` | 服务内部推理批次大小 |
| `dtype` | `auto` | 可选 `auto`、`float32`、`float16`、`bfloat16` |
| `pod_gpu` | `0` | 请求的 GPU 数量；GPU 推理需设为正数，并准备可用 GPU 节点 |
| `pod_gpu_resource` | `nvidia.com/gpu` | 集群的 GPU 资源名称 |

GPU 推理可设置 `'device' = 'cuda', 'pod_gpu' = '1'`；还需要集群提供匹配的 GPU 驱动、设备插件和运行环境。

::: details 按任务调整
文本 embedding 可配置 `pooling`（默认 `mean`）和 `normalize`（默认 `true`）；文本分类可用 `text_pair_column` 指定第二个文本列。文本生成可配置 `max_new_tokens`（默认 `128`）、`do_sample`（默认 `false`）、`temperature` 和 `top_p`。

图片 embedding 的 `image_column` 必须包含 HTTP/HTTPS URL。需要限制下载来源或大小时，可设置 `image_url_allowed_hosts`、`image_download_timeout_ms`（默认 `5000`）、`image_max_bytes`（默认 10 MiB）和 `image_max_pixels`（默认 2000 万）。
:::
