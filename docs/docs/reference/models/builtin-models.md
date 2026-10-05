# 内置模型

本文档介绍 SQLRec 内置的模型类型及其使用方法。

## 通用流程与资源配置

Model、训练、导出和 Service 的 `WITH` 选项使用字符串键和值，例如 `'batch_size' = '8192'`；SQLRec 会按参数类型解析值。

训练型模型按[模型指南](../../guides/model-lifecycle.md#训练型模型的完整流程)执行创建、训练、导出和服务部署。训练型模型的示例以 Model 定义为主，`training_sample` 等训练表需另行准备。训练配置可作为 Model 默认值，并在 `TRAIN MODEL ... WITH (...)` 中覆盖。

下表的资源配置适用于 tzrec、GBDT 和 Hugging Face 自托管任务或服务；`replicas` 用于在线服务。

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `pod_cpu_cores` | Integer | 1 | Pod CPU 核数 |
| `pod_memory` | String | "2Gi" | Pod 内存 |
| `pod_cpu_limit` | String | - | Pod CPU 上限 |
| `pod_memory_limit` | String | - | Pod 内存上限 |
| `replicas` | Integer | 1 | 服务副本数 |

镜像默认值：tzrec 使用 `sqlrec/tzrec`，GBDT 使用 `sqlrec/gbdt`，版本为 `${SQLREC_VERSION}-cpu`；Hugging Face 使用 `sqlrec/transformers:${SQLREC_VERSION}`。可用 `image`、`version` 覆盖。

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

`server.sh` 使用 `hadoop fs -get` 下载远程导出目录，优先使用 `$HADOOP_HOME/bin/hadoop`，否则使用 PATH 中的 `hadoop`。容器中的 Hadoop 客户端需要配置好对应文件系统（例如 HDFS 或 JuiceFS）的连接和认证；本地目录直接使用。`LOCAL_CACHE_DIR` 可指定远程模型缓存的父目录，默认 `/tmp/tzrec_model_cache`；每次启动使用独立子目录，下载失败或缺少必要文件时清理该目录并退出。校验通过后，shell 使用 `exec` 启动所选 serving。Python 后端使用 Torch 和 Python FG 进行特征处理与推理；ARM Python FG 兼容包支持与 SQLRec 生成配置一致的 ID/raw 特征范围。两种后端均在预热成功后就绪，串行执行推理；通过服务副本扩容。

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

续训和导出时，WITH 显式覆盖的网络或特征结构参数会逐项与 checkpoint 比较，不一致时在执行前报错。连续特征默认值和 normalizer 按解析后的数值含义比较，等价数字写法或 normalizer 参数顺序变化不会被误判为结构改变；ID 默认值和分隔符仍严格比较。未覆盖的结构参数保留 checkpoint 的值，不参与默认值比较；列级参数仍优先于全局 `embedding_dim`、`num_buckets`。改变结构需新建模型并重新训练。

连续特征的标量/向量默认值和 normalizer 数值参数按 float32 精度比较，向量元素顺序和维度必须一致。normalizer 参数顺序不影响比较；`log10` 未指定的 `threshold`、`default` 分别按 `1e-10`、`-10` 比较。等价配置通过后仍沿用 checkpoint 中保存的原始配置。此规则同样适用于 MMoE 的特征比较；任务及网络结构仍须一致。

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
| `column.{name}.separator` | 默认 `\035`（ASCII 29）；用于分隔多值字符串与向量默认值 |
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

普通连续特征、MLP/AutoDis 特征进入 Deep 分组，ID 和 boundaries 分桶特征同时进入 Wide 与 Deep；DeepFM 的 FM 分组只接收稀疏特征且嵌入维度须一致。当前排序模型至少需要一个 ID 或 boundaries 分桶特征，DSSM 可使用纯连续特征。

新建 `tzrec.wide_and_deep` 正确使用 WideAndDeep；旧版本同名模型实际生成的 DeepFM 权重，在导出和继续训练时按 checkpoint 内保存的 `pipeline.config` 保留结构及特征。新建 DeepFM 请使用 `tzrec.deepfm`。历史训练时被忽略的浮点列，需要重新训练才能生效；包含标签特征的 checkpoint 必须排除标签后重训。继续训练可以覆盖学习率、轮数、批次等运行设置；修改学习率时不会恢复旧 optimizer 状态。改变特征或网络结构请新建并重训模型，TRAIN/EXPORT 不允许切换模型类型。

Model 的 WITH 参数由 TRAIN、EXPORT 和 SERVICE 继承，对应操作的 WITH 参数优先。新导出先写独立 staging 目录，生成 SHA256 清单 `model_meta.json` 和 `_SUCCESS` 后发布到目标目录；目标目录已存在时拒绝覆盖。服务启动校验清单；旧版没有清单的完整导出目录仍可加载。HDFS/JuiceFS 上依赖目录 rename 的发布语义。

本地构建和镜像发布均执行 `bin/verify_tzrec_image.sh`：检查依赖/动态库、C++ 算子加载、输出类型，并真实训练导出 WideAndDeep、DeepFM、DSSM、MMoE，再对照 Python/C++ 特征张量及 HTTP 预测。MMoE 覆盖混合任务、纯回归、连续特征、续训和双进程训练。ARM 兼容包另有单元测试及原生 x86 官方 pyfg 差分检查。

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

CREATE SERVICE external_service
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
| `sparse_lr` | Double | 0.001 | 稀疏特征学习率 |
| `dense_lr` | Double | 0.001 | 稠密特征学习率 |
| `num_epochs` | Integer | 1 | 训练轮数 |
| `batch_size` | Integer | 8192 | 批次大小 |
| `num_workers` | Integer | 8 | 数据加载工作进程数 |
| `embedding_dim` | Integer | 16 | 嵌入维度 |
| `num_buckets` | Integer | 1000000 | 整数特征分桶数 |
| `hidden_units` | String | "512,256,128" | 深度网络隐藏层单元数 |
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
    'task.like.weight' = '2.0',
    'task.watch_time.type' = 'regression',
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
| `task.<label>.weight` | `1.0` | 有限且大于零的 float32 训练损失权重 |
| `task.<label>.metrics` | binary：`auc`；regression：`mean_squared_error` | binary 支持 `auc,accuracy`，regression 支持 `mean_squared_error,mean_absolute_error` |
| `eval_input_path` | 不设置 | 独立 Parquet 验证集路径，可在 TRAIN 的 WITH 中覆盖 |

资源、batch、epoch、学习率和 `column.*` 特征参数沿用 TZRec 通用配置。MMoE 使用独立的专家/任务塔参数，不能使用单目标 `hidden_units` 或 DSSM 塔参数。未知目标和未知 `task.*` 参数会在创建模型时报错。

训练和验证数据须包含每个目标的有效标签：二分类为 0/1，回归须声明为 FLOAT/DOUBLE，且 Parquet 列也须为浮点类型，数值转换为 float32 后须有限。不允许空值或缺失列。整数回归标签请在生成训练数据前转为 FLOAT/DOUBLE，避免 TZRec 的 L2 反向传播发生 dtype 错误。样本生成流程须保证这些要求，训练入口不额外扫描标签。未指定验证集时只进行训练，不自动在训练集计算验证指标。

输出为 `probs_<label> FLOAT`（二分类）或 `y_<label> FLOAT`（回归）。例如上述模型返回 `probs_click`、`probs_like`、`y_watch_time`；通过 `call_service` 追加到输入表。最终排序可在 SQL 中融合：

```sql
CACHE TABLE scored AS CALL call_service('multi_rank_service', rank_feature);
SELECT item_id, 0.6 * probs_click + 0.4 * probs_like AS rank_score
FROM scored ORDER BY rank_score DESC;
```

训练权重与线上融合系数分别配置；回归值需按业务尺度处理后再融合。所有目标共用一个 `<checkpoint>_export` 和一个服务。目标集合、类型、训练权重、网络和特征配置在 TRAIN/EXPORT/服务部署时不能改变；改变这些配置须新建模型。继续训练仍可覆盖 epoch、batch、学习率和验证集路径。

当前不支持任务别名、多分类、缺失目标标签 mask 或任务样本空间配置。各目标须在同一训练样本空间上定义；仅点击样本定义的 CVR 需要另行设计损失与指标口径。

### 3. DSSM 模型

DSSM（Deep Structured Semantic Models）模型是基于 tzrec 框架实现的双塔召回模型，支持完整的训练、导出和服务部署流程。

DSSM 使用批内负采样，训练和验证数据均须只包含正样本对，数值标签建议统一为 1。标签值不参与正负样本筛选或样本加权；训练入口不拒绝或过滤零、负标签行，请在生成样本时先筛选正样本。具体格式见上文「TZRec 样本数据格式」。

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
| `sparse_lr` | Double | 0.001 | 稀疏特征学习率 |
| `dense_lr` | Double | 0.001 | 稠密特征学习率 |
| `num_epochs` | Integer | 1 | 训练轮数 |
| `batch_size` | Integer | 8192 | 批次大小 |
| `num_workers` | Integer | 8 | 数据加载工作进程数 |
| `embedding_dim` | Integer | 16 | 嵌入维度 |
| `num_buckets` | Integer | 1000000 | 整数特征分桶数 |
| `hidden_units` | String | "512,256,128" | 深度网络隐藏层单元数 |
| `user_hidden_units` | String | "512,256,128" | 用户塔隐藏层单元数 |
| `item_hidden_units` | String | "512,256,128" | 物品塔隐藏层单元数 |
| `output_dim` | Integer | 64 | 输出嵌入维度 |
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

-- 按模型指南完成训练和导出后，DSSM 生成两个 tower checkpoint：
-- v1.0_export/item 和 v1.0_export/user
CREATE SERVICE dssm_item_service
    ON MODEL dssm_model
    CHECKPOINT = 'v1.0_export/item';

CREATE SERVICE dssm_user_service
    ON MODEL dssm_model
    CHECKPOINT = 'v1.0_export/user';
```

### 4. LightGBM 模型

LightGBM 模型是基于 GBDT（梯度提升决策树）框架实现的模型，支持完整的训练、导出和服务部署流程。训练数据和模型文件使用配置的存储，导出时转换为 ONNX 格式用于在线推理。

LightGBM 训练特征先转换为 float32，与 ONNX 和在线服务使用同样的精度；保留 NaN 缺失值，拒绝 Infinity 或 float32 溢出。旧版使用 double 训练的模型需要重新训练，才能保证这项精度一致性。

**模型名称**：`gbdt.lightgbm`

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

CREATE SERVICE text_embedding_service
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
