# 模型效果测试

本页展示 SQLRec 内置模型在四个公开数据集上的推荐效果，覆盖物品召回、二分类排序和多任务预测。你可以查看各模型的验证结果，并使用仓库中的 `benchmark/model_quality/` 脚本复现。SQL 请求的吞吐与延迟见 [SQL性能测试](./sql-benchmark.md)。

## 测试结果

全部 28 个数据集/模型组合均使用完整训练集训练、完整验证集评估，结果已独立复算。

### 排序

每格为 **AUC / LogLoss**。AUC 越高，说明模型区分正负样本的能力越强；LogLoss 越低，说明预测概率的误差越小。加粗标出六种单任务排序模型在同一数据集上的最优值，MMoE 的两个任务分别列出。

| 模型 / 任务 | MovieLens | KuaiRand | KuaiRec | Criteo 1M |
| --- | ---: | ---: | ---: | ---: |
| Wide & Deep | 0.7763 / 0.5622 | 0.6875 / 0.6259 | 0.8741 / 0.1383 | 0.7866 / 0.4601 |
| DeepFM | **0.7862** / **0.5553** | 0.6884 / 0.6266 | **0.8764** / **0.1374** | **0.7881** / **0.4590** |
| Rocket Launching | 0.7751 / 0.5642 | 0.6940 / 0.6527 | 0.8716 / 0.1400 | 0.7837 / 0.4662 |
| CatBoost | 0.7522 / 0.5831 | **0.7072** / 0.6010 | 0.8678 / 0.1419 | 0.7759 / 0.4694 |
| LightGBM | 0.7408 / 0.6040 | 0.7021 / 0.5993 | 0.8691 / 0.1404 | 0.7283 / 0.5009 |
| XGBoost | 0.7357 / 0.5987 | 0.7017 / **0.5988** | 0.8649 / 0.1419 | 0.7286 / 0.5005 |
| MMoE / click | — | 0.6852 / 0.6578 | — | — |
| MMoE / long_view | — | 0.7003 / 0.4833 | — | — |

AUC 和 LogLoss 的最优模型可能不同，例如 KuaiRand 上 CatBoost 的 AUC 最高，XGBoost 的 LogLoss 最低。选择模型时应结合业务对排序和概率预测的需求。

### 召回

DSSM 按用户向量与物品向量的内积返回 Top-50。Recall@50 表示找回了多少比例的正例；NDCG@50 同时考虑命中位置，正例排得越靠前，得分越高。两者均在有正例的查询上取平均，越高越好。

| 数据集 / DSSM | Recall@50 | NDCG@50 | 全部 / 有正例查询 | 候选范围 |
| --- | ---: | ---: | ---: | --- |
| MovieLens | 34.26% | 19.15% | 926 / 873 | 完整 1,682 项目录，排除 train 已见物品 |
| KuaiRand | 11.05% | 4.45% | 38,079 / 24,950 | 完整 7,583 项可用目录，允许重复观看 |
| KuaiRec | 57.82% | 51.47% | 1,411 / 1,381 | 每个查询在 valid 中的全部已观测正负例，排除 train 已见视频 |

KuaiRec 只在每个用户已观测的候选物品中召回，另外两个数据集使用完整目录，因此不能按召回率高低直接比较这三个任务的难度。

## 数据与测试设置

### 数据集

| 数据集 | 评估目标与正例定义 | 训练行数 | 验证行数 | 划分方式 |
| --- | --- | ---: | ---: | --- |
| MovieLens-100K | 评分偏好：电影评分 ≥ 4 | 80,140 | 9,877 | 用户—物品交互留出，约 80% / 10% / 10% |
| KuaiRand-Pure | 点击：`is_click=1`；MMoE 另预测长观看 | 1,124,319 | 408,405 | 按时间先后划分 |
| KuaiRec small_matrix | 观看偏好：`watch_ratio > 2` | 3,741,059 | 467,747 | 用户—视频交互留出，约 80% / 10% / 10% |
| Criteo 1M | 广告点击：`target=1` | 799,561 | 100,266 | 按特征哈希留出，约 80% / 10% / 10% |

KuaiRand 使用标准曝光训练，验证包含标准和随机曝光。MovieLens、KuaiRec 评估已知用户和物品之间的偏好，不属于严格的未来时间预测。排序训练使用真实正负例，DSSM 只使用训练正例；KuaiRec 未观测的用户—视频组合不会被当作负例。

Criteo 使用固定的 1M 样本版本，相同特征记录始终分到同一组，划分不依赖标签。表中为该样本的留出验证成绩，未使用预留测试集调参。Criteo 只测试单任务排序，不测试 DSSM 和 MMoE。

### 模型与特征

| 模型 | 用途与主要输入 |
| --- | --- |
| DSSM | 召回；使用用户/物品 ID、类目等特征，MovieLens 和 KuaiRec 还使用历史统计，KuaiRand 使用场景和作者 |
| Wide & Deep、DeepFM、Rocket Launching | 排序；使用类别和数值特征，推荐数据集还使用 ID、历史统计及可用的视频时长；Rocket 评估 Light 分支 |
| CatBoost、LightGBM、XGBoost | 排序；前三个数据集使用数值历史统计和可用的视频时长；Criteo 的输入见下文 |
| MMoE | 多任务排序；使用 KuaiRand 排序特征，同时预测点击和长观看 |

Criteo 包含 13 个数值特征和 26 个匿名类别特征。CatBoost 和三个神经排序模型使用全部 39 个特征；LightGBM、XGBoost 使用 13 个数值特征，符合 SQLRec 当前训练与导出链路的输入要求。因此 Criteo 各模型的成绩对应不同特征配置。

部分排序模型还使用用户和物品的平滑历史正例率。具体字段和模型参数见[内置模型](../reference/models/builtin-models.md)及 [benchmark README](https://github.com/sqlrec/sqlrec/blob/main/benchmark/model_quality/README.md)。

### 主要训练配置

| 数据集 / 模型 | Embedding 维度 | Batch size | 训练轮数 |
| --- | ---: | ---: | ---: |
| MovieLens / DSSM | 16 | 256 | 3 |
| KuaiRand / DSSM | 16 | 256 | 6 |
| KuaiRec / DSSM | 8 | 64 | 1 |
| MovieLens / 三个神经排序模型 | 16 | 256 | 6 |
| KuaiRec / 三个神经排序模型 | 16 | 256 | 1 |
| KuaiRand / Wide & Deep、DeepFM、MMoE | 8 | 64 | 1 |
| KuaiRand / Rocket Launching | 8 | 256 | 1 |
| Criteo / 三个神经排序模型 | 8 | 256 | 1 |

Criteo 数值缺失填 0，类别缺失使用固定占位值。神经排序模型对数值特征做对数缩放，训练与推理保持一致；树模型保留原数值。

树模型训练 100 轮，MovieLens LightGBM 为 300 轮。DSSM 统一使用 `negative_sampling=pool`，负例数 256、池容量 4096；向量维度为 MovieLens/KuaiRand 32、KuaiRec 16。训练资源为 2 CPU / 4 GiB，推理服务为 1 CPU / 2 GiB。

## 复现测试

先按[服务部署](../operations/deployment.md)准备 SQLRec、Kyuubi、共享存储和 Milvus，服务与模型镜像使用当前源码构建。在仓库根目录执行：

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r benchmark/model_quality/requirements.txt
source deploy/env.sh

# 每个数据版本准备一次，后续运行复用。
.venv/bin/python -m benchmark.model_quality.prepare --dataset all --upload

# 先运行一个数据集和模型。
.venv/bin/python -m benchmark.model_quality --dataset movielens100k --model deepfm --full-eval

# 运行全部 28 个组合。
.venv/bin/python -m benchmark.model_quality --model all --full-eval
```

仅测试 Criteo 时，将数据集参数改为 `--dataset criteo1m`；搭配 `--model all` 可运行其支持的六个模型，且不需要 Milvus。

以上命令使用完整验证集；省略 `--full-eval` 时，默认最多评估 20,000 条排序记录或 1,000 个召回查询，训练数据不变。

每次运行后，在 `benchmark/model_quality/runs/` 的对应目录查看 `summary.md`，其中包含指标和完成状态；`config.json` 记录各模型的实际配置。需要检查原始预测时，增加 `--save-predictions`。

## 调整配置

- 数据字段、标签和特征视图：`benchmark/model_quality/configs/datasets/*.yaml`。
- 模型与训练参数：`benchmark/model_quality/config.py`，支持按数据集、模型分别配置。
- 参数含义与 SQL 用法：见[内置模型](../reference/models/builtin-models.md)；完整测试配置及运行选项见 [benchmark README](https://github.com/sqlrec/sqlrec/blob/main/benchmark/model_quality/README.md)。

调整后应使用相同验证数据和候选范围比较结果。修改数据准备规则或特征定义时，需要重新运行数据准备命令。
