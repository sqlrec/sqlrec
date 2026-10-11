# Model Quality Test

`benchmark/model_quality/` trains, exports, and deploys models through SQLRec's public SQL interface, then measures recommendation quality through its SQL API. This page explains the test configuration and results verified through data and metric checks, covering 22 dataset/model combinations. See [SQL Performance Test](./sql-benchmark.md) for request throughput and latency.

## Datasets

The datasets use different labels and splits. Ranking models train on observed positive and negative examples; DSSM trains on all training positives. Unobserved KuaiRec user/video pairs are never treated as negatives.

| Dataset | Positive label | Split | Train rows | Valid rows | Test rows |
| --- | --- | --- | ---: | ---: | ---: |
| MovieLens-100K | Movie rating `rating >= 4` | Fixed hash of user/item pairs, approximately 80% / 10% / 10% | 80,140 | 9,877 | 9,983 |
| KuaiRand-Pure | Logged `is_click = 1`; MMoE also predicts `long_view` | Chronological | 1,124,319 | 408,405 | 1,064,442 |
| KuaiRec small_matrix | Watch ratio `watch_ratio > 2` | Fixed hash of user/video pairs, approximately 80% / 10% / 10% | 3,741,059 | 467,747 | 467,764 |

KuaiRand trains on standard exposures from 2022-04-08 through 04-21, validates on 04-22 through 04-28, and tests on 04-29 through 05-08. Validation and test include both standard and random exposures. Overlapping timestamps at date boundaries are purged so train, valid, and test are strictly ordered in time. The retrieval catalog contains only videos available before validation starts.

MovieLens and KuaiRec evaluate preferences between known users and items through pair holdouts; they do not measure strict future-time prediction. Labels and task difficulty differ, so AUC should not be compared directly across datasets.

## Features and Models

Dataset fields and feature views are defined in `benchmark/model_quality/configs/datasets/*.yaml`. Model and training parameters are centralized in `benchmark/model_quality/config.py`.

Neural ranking models use user/item IDs, categories, and user/item interaction and positive counts. KuaiRand also uses author, context `tab`, and video duration. KuaiRec uses categories and video duration, with a fixed placeholder for missing authors. Tree models use only the four numeric history counts, plus duration for video datasets.

Counts use `log1p(count)`. Validation/test statistics come exclusively from train. KuaiRand training rows use only earlier timestamps, excluding feedback at the same timestamp. MovieLens and KuaiRec training statistics exclude the current interaction. Ratings, watch ratios, click labels, and long-view labels never become model features or prediction inputs.

| Model | Purpose | Main structure |
| --- | --- | --- |
| DSSM | Top-50 retrieval | Separate user/item towers, ranked by inner product; default tower layers 32/16 and output dimension 16 |
| Wide & Deep | Binary ranking | Wide and Deep branches, with Deep layers 64/32 |
| DeepFM | Binary ranking | FM feature interactions and Deep layers 64/32 |
| Rocket Launching | Binary ranking | A Booster assists the Light branch during training; evaluation uses Light probabilities |
| CatBoost | Binary ranking | Gradient-boosted trees, default depth 6 |
| LightGBM | Binary ranking | Gradient-boosted trees, default maximum 31 leaves |
| XGBoost | Binary ranking | Gradient-boosted trees, default depth 6 |
| MMoE | Multitask ranking, KuaiRand only | Four experts with layers 64/32 and task layers 16; separate click and long-view predictions |

String and category features use hashed embeddings. Each field gets 20 times its training category count, rounded up to a power of two and capped at 4,194,304 buckets. DSSM item fields also cover the complete served catalog. Each run's `config.json` records the resolved bucket sizes and all parameters.

DSSM uses dataset-specific tower inputs:

| Dataset | User tower | Item tower | Retrieval score |
| --- | --- | --- | --- |
| MovieLens | User ID and user history counts | Item ID, categories, and item history counts | Embedding inner product |
| KuaiRand | User ID and `tab` | Video ID, categories, and author | Inner product + `log1p(train_click_count)` |
| KuaiRec | User ID and user history counts | Video ID, categories, author placeholder, item history counts, and duration | Embedding inner product |

KuaiRand's popularity term uses training clicks only, with weight 1. Its towers do not receive history counts or duration.

## Training Configuration

Every model uses the complete training split and seed 123. Neural defaults are embedding dimension 8, batch size 64, one epoch, one data-loading worker, and sparse/dense learning rates of 0.001. Tree defaults are 100 iterations and learning rate 0.05. These fixed dataset-specific overrides are applied without parameter search during evaluation:

| Dataset / model | Overrides |
| --- | --- |
| MovieLens / DSSM | Embedding 16, batch 256, 3 epochs, sparse learning rate 0.01; tower layers 64/32, output dimension 32 |
| MovieLens / Wide & Deep, DeepFM, Rocket Launching | Embedding 16, batch 256, 6 epochs, sparse learning rate 0.03 |
| MovieLens / LightGBM | 300 iterations, depth 4, 15 leaves, minimum 100 rows per leaf; feature/sample fractions 1, L2 regularization 5 |
| KuaiRand / DSSM | Embedding 16, batch 256, 3 epochs, sparse learning rate 0.01; tower layers 64/32, output dimension 32 |
| KuaiRand / Rocket Launching | Batch 256, shared layer 64, Booster layers 32/16, Light layer 16; logarithmic duration, still one epoch |
| KuaiRec / DeepFM | Embedding 16, batch 256, sparse learning rate 0.03; logarithmic duration, still one epoch |

Logarithmic duration maps values above one second to `log10(seconds)` and other values to zero. Training and serving apply the same transformation. Unlisted parameters retain their defaults.

The runs used model image version `0.1.16-cpu`, training Pods with 2 CPUs / 4 GiB, and serving Pods with 1 CPU / 2 GiB. Configurations were selected using validation results. The numbers below are tuned full-validation results, not unseen test-set estimates or averages across multiple seeds.

## Current Results

### Ranking

Each cell is **AUC / LogLoss**: higher AUC and lower LogLoss are better. Every validation row listed above is included, without label-based sampling. MMoE tasks are reported separately.

| Model / task | MovieLens | KuaiRand | KuaiRec |
| --- | ---: | ---: | ---: |
| Wide & Deep | 0.7763 / 0.5622 | 0.6875 / 0.6259 | 0.8703 / 0.1405 |
| DeepFM | 0.7834 / 0.5555 | 0.6884 / 0.6266 | 0.8764 / 0.1375 |
| Rocket Launching | 0.7745 / 0.5647 | 0.6940 / 0.6527 | 0.8699 / 0.1406 |
| CatBoost | 0.7423 / 0.5970 | 0.7072 / 0.6010 | 0.8678 / 0.1419 |
| LightGBM | 0.7408 / 0.6040 | 0.7021 / 0.5993 | 0.8685 / 0.1408 |
| XGBoost | 0.7268 / 0.6147 | 0.7017 / 0.5988 | 0.8649 / 0.1419 |
| MMoE / click | — | 0.6852 / 0.6578 | — |
| MMoE / long_view | — | 0.7003 / 0.4833 | — |

### Retrieval

Recall@50 is the fraction of a query's positive targets retrieved. NDCG@50 also rewards placing positive items earlier in the ranking. Both metrics are macro-averaged over queries with positive targets; queries without positives are counted separately.

| Dataset / DSSM | Recall@50 | NDCG@50 | All / positive queries | Candidates |
| --- | ---: | ---: | ---: | --- |
| MovieLens | 27.89% | 13.15% | 926 / 873 | Complete 1,682-item catalog, excluding items seen in train |
| KuaiRand | 13.09% | 5.30% | 38,079 / 24,950 | Complete 7,583-item eligible catalog, repeat viewing allowed |
| KuaiRec | 50.42% | 38.62% | 1,411 / 1,381 | All observed positive/negative validation pairs for that query, excluding videos seen in train |

KuaiRec uses observed candidates, so its 50.42% cannot be compared directly with the other full-catalog tasks. Its underlying catalog contains 3,327 videos. Per-query candidate selection does not inspect labels, and the metric denominator always includes all positive targets for the query.

This page uses repaired KuaiRec results: duration selection no longer sorts on `watch_ratio`, and all seven models were retrained and evaluated. The audit checked every train/valid/test label, split overlaps, and history counts. All 20 ranking tasks and three retrieval models passed independent metric recalculation, and 30 local tests passed.

## Reproduce

Follow [Service Deployment](../operations/deployment.md) to prepare SQLRec, Kyuubi, shared storage, and Milvus. Then run from the repository root:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r benchmark/model_quality/requirements.txt
source deploy/env.sh

# Prepare each data version and its training tables once; later runs reuse them.
.venv/bin/python -m benchmark.model_quality.prepare --dataset all --upload

# Full training and validation data for all 22 combinations.
.venv/bin/python -m benchmark.model_quality --model all --full-eval

# Alternatively, evaluate one dataset/model.
.venv/bin/python -m benchmark.model_quality --dataset kuairand --model dssm --full-eval
```

`--full-eval` removes evaluation sampling limits without changing training data. Defaults evaluate at most 20,000 ranking rows or 1,000 retrieval queries. Results go to `benchmark/model_quality/runs/date_time_dataset_model/`: `summary.md` shows metrics, `results.json` keeps unrounded values, and each model's `config.json` records its resolved configuration. Runs do not copy datasets. Predictions are saved only with `--save-predictions`.

Preparation versions are MovieLens `9b0c935804e0d6ad`, KuaiRand `d5ace3c58614f9ad`, and KuaiRec `932028e12264857a`.
