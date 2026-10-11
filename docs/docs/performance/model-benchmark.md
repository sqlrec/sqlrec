# 模型效果测试

`benchmark/model_quality/` 通过 SQLRec 的公开 SQL 接口训练、导出和部署模型，再调用 SQL API 计算推荐效果。本页介绍测试配置和经过数据与指标核验的结果，覆盖 22 个数据集/模型组合。SQL 请求的吞吐与延迟测试见 [SQL性能测试](./sql-benchmark.md)。

## 数据集

三个数据集使用不同的标签和划分方式。排序训练保留真实正负例，DSSM 使用训练集中的全部正例；KuaiRec 未观测的用户—视频组合不会被当作负例。

| 数据集 | 正例定义 | 划分方式 | Train 行数 | Valid 行数 | Test 行数 |
| --- | --- | --- | ---: | ---: | ---: |
| MovieLens-100K | 电影评分 `rating >= 4` | 用户—物品 pair 固定哈希划分，约 80% / 10% / 10% | 80,140 | 9,877 | 9,983 |
| KuaiRand-Pure | 日志中的 `is_click = 1`；MMoE 另预测 `long_view` | 按时间划分 | 1,124,319 | 408,405 | 1,064,442 |
| KuaiRec small_matrix | 观看比例 `watch_ratio > 2` | 用户—视频 pair 固定哈希划分，约 80% / 10% / 10% | 3,741,059 | 467,747 | 467,764 |

KuaiRand 使用 2022-04-08 至 04-21 的标准曝光训练，04-22 至 04-28 验证，04-29 至 05-08 测试；验证和测试都包含标准及随机曝光。日期边界处重叠的时间戳已清除，确保 train、valid、test 严格按时间先后排列。召回目录只保留验证期开始前已可用的视频。

MovieLens、KuaiRec 的划分用于评估已知用户和物品之间的偏好，不属于严格的未来时间预测。三个任务的标签含义和难度不同，AUC 不宜跨数据集直接比较。

## 特征与模型配置

数据集字段和特征视图定义在 `benchmark/model_quality/configs/datasets/*.yaml`；模型及训练参数集中在 `benchmark/model_quality/config.py`。

神经排序模型使用用户/物品 ID、类目，以及用户和物品的历史交互数、正例数。KuaiRand 还使用作者、场景 `tab` 和视频时长；KuaiRec 使用类目和视频时长，作者缺失时使用固定占位值。树模型只使用四个历史统计数值，视频数据集再加时长。

统计特征统一使用 `log1p(count)`。valid/test 的统计值只来自 train；KuaiRand 训练行只使用更早时间戳的历史，排除同一时间戳反馈；MovieLens、KuaiRec 训练行的统计排除当前交互。评分、观看比例、点击和长观看标签均不进入模型特征或预测请求。

| 模型 | 用途 | 主要结构 |
| --- | --- | --- |
| DSSM | Top-50 召回 | 用户塔和物品塔分别生成向量，按内积排序；默认塔层为 32/16，输出 16 维 |
| Wide & Deep | 二分类排序 | Wide 与 Deep 分支，Deep 层为 64/32 |
| DeepFM | 二分类排序 | FM 特征交叉与 64/32 层的 Deep 分支 |
| Rocket Launching | 二分类排序 | Booster 辅助训练 Light 分支；评估使用 Light 的预测概率 |
| CatBoost | 二分类排序 | 梯度提升树，默认深度 6 |
| LightGBM | 二分类排序 | 梯度提升树，默认最多 31 个叶子 |
| XGBoost | 二分类排序 | 梯度提升树，默认深度 6 |
| MMoE | 多任务排序，仅 KuaiRand | 4 个专家，每个专家层为 64/32，任务层为 16；分别预测点击和长观看 |

字符串和类目特征使用哈希 embedding。每个字段的 bucket 数按训练类别数的 20 倍向上取 2 的幂，最大 4,194,304；DSSM 的物品字段还覆盖完整服务目录。每次运行的 `config.json` 保存实际 bucket 数及全部参数。

DSSM 的输入按数据集分别配置：

| 数据集 | 用户塔 | 物品塔 | 召回评分 |
| --- | --- | --- | --- |
| MovieLens | 用户 ID、用户历史统计 | 物品 ID、类目、物品历史统计 | 向量内积 |
| KuaiRand | 用户 ID、`tab` | 视频 ID、类目、作者 | 向量内积 + `log1p(train_click_count)` |
| KuaiRec | 用户 ID、用户历史统计 | 视频 ID、类目、作者占位值、物品历史统计、时长 | 向量内积 |

KuaiRand 的热度项只使用训练点击数，权重为 1；该模型的两个塔不输入历史统计或视频时长。

## 训练配置

所有模型使用完整训练集和固定 seed=123。神经模型的默认配置为 embedding 8 维、batch 64、1 epoch、1 个数据加载 worker，稀疏和稠密学习率均为 0.001；树模型默认训练 100 轮，学习率 0.05。各数据集采用以下固定覆盖项，测试入口不会自动搜索参数：

| 数据集 / 模型 | 覆盖默认值的配置 |
| --- | --- |
| MovieLens / DSSM | embedding 16，batch 256，3 epochs，稀疏学习率 0.01；两塔层 64/32，输出 32 维 |
| MovieLens / Wide & Deep、DeepFM、Rocket Launching | embedding 16，batch 256，6 epochs，稀疏学习率 0.03 |
| MovieLens / LightGBM | 300 轮，深度 4，15 个叶子，每叶至少 100 行；特征及样本采样比例均为 1，L2 正则 5 |
| KuaiRand / DSSM | embedding 16，batch 256，3 epochs，稀疏学习率 0.01；两塔层 64/32，输出 32 维 |
| KuaiRand / Rocket Launching | batch 256，共享层 64，Booster 层 32/16，Light 层 16；时长取对数，仍为 1 epoch |
| KuaiRec / DeepFM | embedding 16，batch 256，稀疏学习率 0.03；时长取对数，仍为 1 epoch |

时长对数变换为：大于 1 秒时取 `log10(seconds)`，其余映射为 0；训练与推理使用相同变换。未列出的参数保留默认值。

本次运行的模型镜像版本为 `0.1.16-cpu`，训练 Pod 配置为 2 CPU / 4 GiB，推理服务为 1 CPU / 2 GiB。模型配置已根据验证结果选定，因此下表是调参后的完整验证集结果，不是未见过的独立测试集成绩，也不是多 seed 平均值。

## 当前测试结果

### 排序

每格为 **AUC / LogLoss**：AUC 越高越好，LogLoss 越低越好。全部使用上表中的完整 valid，没有按标签筛选样本；MMoE 的两个任务分别列出。

| 模型 / 任务 | MovieLens | KuaiRand | KuaiRec |
| --- | ---: | ---: | ---: |
| Wide & Deep | 0.7763 / 0.5622 | 0.6875 / 0.6259 | 0.8703 / 0.1405 |
| DeepFM | 0.7834 / 0.5555 | 0.6884 / 0.6266 | 0.8764 / 0.1375 |
| Rocket Launching | 0.7745 / 0.5647 | 0.6940 / 0.6527 | 0.8699 / 0.1406 |
| CatBoost | 0.7423 / 0.5970 | 0.7072 / 0.6010 | 0.8678 / 0.1419 |
| LightGBM | 0.7408 / 0.6040 | 0.7021 / 0.5993 | 0.8685 / 0.1408 |
| XGBoost | 0.7268 / 0.6147 | 0.7017 / 0.5988 | 0.8649 / 0.1419 |
| MMoE / click | — | 0.6852 / 0.6578 | — |
| MMoE / long_view | — | 0.7003 / 0.4833 | — |

### 召回

Recall@50 表示命中的正例占该查询全部正例的比例；NDCG@50 同时考虑命中位置，靠前的正例权重更高。两者均在有正例的查询上取宏平均，无正例查询单独计数。

| 数据集 / DSSM | Recall@50 | NDCG@50 | 全部 / 有正例查询 | 候选范围 |
| --- | ---: | ---: | ---: | --- |
| MovieLens | 27.89% | 13.15% | 926 / 873 | 完整 1,682 项目录，排除 train 已见物品 |
| KuaiRand | 13.09% | 5.30% | 38,079 / 24,950 | 完整 7,583 项可用目录，允许重复观看 |
| KuaiRec | 50.42% | 38.62% | 1,411 / 1,381 | 每个查询在 valid 中的全部已观测正负例，排除 train 已见视频 |

KuaiRec 使用已观测候选池，不能把 50.42% 直接与另外两个完整目录任务比较。其底层目录包含 3,327 个视频，按查询筛选候选时不读取标签；召回指标始终保留该查询全部正例作为分母。

本页使用修复后的 KuaiRec 结果：视频时长选择已移除 `watch_ratio` 排序依赖，全部 7 个模型重新训练并评估。数据核验覆盖全部 train/valid/test 标签、分区重叠及历史统计；20 个排序任务和 3 个召回模型的指标均已独立复算通过，30 项本地测试通过。

## 复现

先按[服务部署](../operations/deployment.md)准备 SQLRec、Kyuubi、共享存储和 Milvus，然后在仓库根目录执行：

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r benchmark/model_quality/requirements.txt
source deploy/env.sh

# 数据与训练表每个版本只准备一次，后续运行复用。
.venv/bin/python -m benchmark.model_quality.prepare --dataset all --upload

# 完整训练集 + 完整验证集，运行全部 22 个组合。
.venv/bin/python -m benchmark.model_quality --model all --full-eval

# 也可以只测试一个数据集/模型。
.venv/bin/python -m benchmark.model_quality --dataset kuairand --model dssm --full-eval
```

`--full-eval` 仅移除评估抽样限制，不改变训练数据；默认最多评估 20,000 条排序记录或 1,000 个召回查询。结果写入 `benchmark/model_quality/runs/日期_时间_数据集_模型/`，`summary.md` 查看指标，`results.json` 保存完整精度数值，每个模型的 `config.json` 保存实际配置。运行目录不复制数据集，预测文件仅在指定 `--save-predictions` 时保存。

数据准备版本分别为 MovieLens `9b0c935804e0d6ad`、KuaiRand `d5ace3c58614f9ad`、KuaiRec `932028e12264857a`。
