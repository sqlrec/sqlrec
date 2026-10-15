# Model Quality Test

This page shows the recommendation quality of SQLRec's built-in models on four public datasets, covering retrieval, binary ranking, and multitask prediction. You can compare validation results and reproduce them with `benchmark/model_quality/`. For request throughput and latency, see [SQL Performance Test](./sql-benchmark.md).

## Results

All 28 dataset/model combinations were trained on the complete training splits and evaluated on the complete validation splits. The reported metrics were independently recalculated.

### Ranking

Each cell is **AUC / LogLoss**. Higher AUC means better discrimination between positive and negative examples; lower LogLoss means less error in predicted probabilities. Bold values identify the best of the six single-task ranking models on each dataset. MMoE tasks are listed separately.

| Model / task | MovieLens | KuaiRand | KuaiRec | Criteo 1M |
| --- | ---: | ---: | ---: | ---: |
| Wide & Deep | 0.7763 / 0.5622 | 0.6875 / 0.6259 | 0.8741 / 0.1383 | 0.7866 / 0.4601 |
| DeepFM | **0.7862** / **0.5553** | 0.6884 / 0.6266 | **0.8764** / **0.1374** | **0.7881** / **0.4590** |
| Rocket Launching | 0.7751 / 0.5642 | 0.6940 / 0.6527 | 0.8716 / 0.1400 | 0.7837 / 0.4662 |
| CatBoost | 0.7522 / 0.5831 | **0.7072** / 0.6010 | 0.8678 / 0.1419 | 0.7759 / 0.4694 |
| LightGBM | 0.7408 / 0.6040 | 0.7021 / 0.5993 | 0.8691 / 0.1404 | 0.7283 / 0.5009 |
| XGBoost | 0.7357 / 0.5987 | 0.7017 / **0.5988** | 0.8649 / 0.1419 | 0.7286 / 0.5005 |
| MMoE / click | — | 0.6852 / 0.6578 | — | — |
| MMoE / long_view | — | 0.7003 / 0.4833 | — | — |

The best model can differ by metric: CatBoost has the highest KuaiRand AUC, while XGBoost has the lowest LogLoss. Consider your application's ranking and probability-prediction needs when selecting a model.

### Retrieval

DSSM returns the Top-50 items by the inner product of user and item vectors. Recall@50 measures the fraction of positive targets retrieved. NDCG@50 also rewards placing positive items earlier in the ranking. Both are averaged over queries with positive targets; higher is better.

| Dataset / DSSM | Recall@50 | NDCG@50 | All / positive queries | Candidates |
| --- | ---: | ---: | ---: | --- |
| MovieLens | 34.26% | 19.15% | 926 / 873 | Complete 1,682-item catalog, excluding items seen in train |
| KuaiRand | 11.05% | 4.45% | 38,079 / 24,950 | Complete 7,583-item eligible catalog, repeat viewing allowed |
| KuaiRec | 57.82% | 51.47% | 1,411 / 1,381 | All observed positive/negative validation pairs for that query, excluding videos seen in train |

KuaiRec retrieves from each user's observed candidates, while the other datasets use complete catalogs. Their recall scores therefore cannot be used to compare the difficulty of these three tasks directly.

## Data and Test Settings

### Datasets

| Dataset | Target and positive label | Training rows | Validation rows | Split |
| --- | --- | ---: | ---: | --- |
| MovieLens-100K | Rating preference: movie rating ≥ 4 | 80,140 | 9,877 | User/item interaction holdout, approximately 80% / 10% / 10% |
| KuaiRand-Pure | Clicks: `is_click=1`; MMoE also predicts long views | 1,124,319 | 408,405 | Chronological |
| KuaiRec small_matrix | Watch preference: `watch_ratio > 2` | 3,741,059 | 467,747 | User/video interaction holdout, approximately 80% / 10% / 10% |
| Criteo 1M | Ad clicks: `target=1` | 799,561 | 100,266 | Feature-hash holdout, approximately 80% / 10% / 10% |

KuaiRand trains on standard exposures and validates on both standard and random exposures. MovieLens and KuaiRec evaluate preferences between known users and items, rather than strict future-time prediction. Ranking uses observed positive and negative examples; DSSM uses training positives only. Unobserved KuaiRec user/video pairs are never treated as negatives.

Criteo uses a pinned 1M sample version. Identical feature rows stay in the same split, and labels do not affect the split. Its scores are holdout-validation results on this sample; the reserved test split was not used for tuning. Criteo tests single-task ranking only, without DSSM or MMoE.

### Models and Features

| Model | Purpose and main inputs |
| --- | --- |
| DSSM | Retrieval using user/item IDs, categories, and related features; MovieLens and KuaiRec also use history statistics, while KuaiRand uses context and author |
| Wide & Deep, DeepFM, Rocket Launching | Ranking using categorical and numeric features; recommendation datasets also use IDs, history statistics, and available video duration; Rocket evaluates the Light branch |
| CatBoost, LightGBM, XGBoost | Ranking using numeric history statistics and available video duration on the first three datasets; see below for Criteo inputs |
| MMoE | Multitask ranking using KuaiRand ranking features to predict clicks and long views together |

Criteo contains 13 numeric features and 26 anonymous categorical features. CatBoost and the three neural ranking models use all 39 features. LightGBM/XGBoost use the 13 numeric features, as required by SQLRec's current training/export path. Criteo scores therefore reflect different feature configurations.

Some ranking models also use user/item smoothed positive rates from training history. See [Built-in Models](../reference/models/builtin-models.md) and the [benchmark README](https://github.com/sqlrec/sqlrec/blob/main/benchmark/model_quality/README.md) for specific features and parameters.

### Main Training Settings

| Dataset / model | Embedding dimension | Batch size | Epochs |
| --- | ---: | ---: | ---: |
| MovieLens / DSSM | 16 | 256 | 3 |
| KuaiRand / DSSM | 16 | 256 | 6 |
| KuaiRec / DSSM | 8 | 64 | 1 |
| MovieLens / three neural ranking models | 16 | 256 | 6 |
| KuaiRec / three neural ranking models | 16 | 256 | 1 |
| KuaiRand / Wide & Deep, DeepFM, MMoE | 8 | 64 | 1 |
| KuaiRand / Rocket Launching | 8 | 256 | 1 |
| Criteo / three neural ranking models | 8 | 256 | 1 |

Criteo fills missing numeric values with zero and missing categories with a fixed placeholder. Neural ranking models apply logarithmic scaling to numeric inputs consistently during training and serving; tree models retain the original numeric values.

Tree models use 100 iterations, except MovieLens LightGBM, which uses 300. DSSM uses `negative_sampling=pool`, 256 negatives, and a pool capacity of 4096. Its output vectors have 32 dimensions on MovieLens/KuaiRand and 16 on KuaiRec. Training resources are 2 CPUs / 4 GiB; serving resources are 1 CPU / 2 GiB.

## Reproduce the Test

Follow [Service Deployment](../operations/deployment.md) to prepare SQLRec, Kyuubi, shared storage, and Milvus. Build the service and model images from the current source, then run from the repository root:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r benchmark/model_quality/requirements.txt
source deploy/env.sh

# Prepare each data version once; later runs reuse it.
.venv/bin/python -m benchmark.model_quality.prepare --dataset all --upload

# Start with one dataset and model.
.venv/bin/python -m benchmark.model_quality --dataset movielens100k --model deepfm --full-eval

# Run all 28 combinations.
.venv/bin/python -m benchmark.model_quality --model all --full-eval
```

To test Criteo only, use `--dataset criteo1m`. Combined with `--model all`, this runs its six supported models and does not require Milvus.

These commands evaluate complete validation splits. Without `--full-eval`, evaluation uses up to 20,000 ranking rows or 1,000 retrieval queries; training data is unchanged.

After each run, open `summary.md` in the corresponding directory under `benchmark/model_quality/runs/` for metrics and completion status. Each model's `config.json` records its resolved configuration. Add `--save-predictions` when you need to inspect individual predictions.

## Adjust the Configuration

- Data fields, labels, and feature views: `benchmark/model_quality/configs/datasets/*.yaml`.
- Model and training parameters: `benchmark/model_quality/config.py`, with separate settings for each dataset/model combination.
- Parameter meanings and SQL usage: [Built-in Models](../reference/models/builtin-models.md). For complete test settings and run options, see the [benchmark README](https://github.com/sqlrec/sqlrec/blob/main/benchmark/model_quality/README.md).

Compare changes on the same validation data and candidate scope. Rerun data preparation when changing preparation rules or feature definitions.
