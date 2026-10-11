# Model Quality

Train, export, and deploy models through SQLRec's public SQL interface, then predict through its SQL API. Reports contain two core metrics for the current model, with no historical comparisons, tuning, auditing, or interruption recovery.

## Prepare and run

Run from the repository root. Python 3 and an existing SQLRec deployment are required. Dependency installation and local tests are separate from evaluation:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r benchmark/model_quality/requirements.txt
source deploy/env.sh

# Prepare local data and upload/register reusable training tables once per version.
.venv/bin/python -m benchmark.model_quality.prepare --dataset all --upload

# Local data preparation only, without a cluster.
.venv/bin/python -m benchmark.model_quality.prepare --dataset all

# Run one model on all applicable datasets by default.
.venv/bin/python -m benchmark.model_quality --model deepfm

# Run on one dataset.
.venv/bin/python -m benchmark.model_quality --model dssm --dataset kuairand

# Run all 22 applicable combinations; MMoE supports only KuaiRand.
.venv/bin/python -m benchmark.model_quality --model all

# Evaluate the full validation set.
.venv/bin/python -m benchmark.model_quality --model dssm --full-eval

# Evaluate the full validation sets for all models and datasets.
.venv/bin/python -m benchmark.model_quality --model all --full-eval

# Optionally save one compact prediction file per model for inspection.
.venv/bin/python -m benchmark.model_quality --model deepfm --save-predictions

# Run local tests without a cluster.
.venv/bin/python -m unittest discover -s benchmark/model_quality/tests -v
```

`--model` is required; running without arguments does not start training. Supported models are DSSM, Wide & Deep, DeepFM, Rocket Launching, CatBoost, LightGBM, XGBoost, and MMoE.

Training always uses the full training set with seed=123. Defaults are one epoch for neural models and 100 iterations for tree models. Fixed dataset/model overrides are listed below; they are applied after the common and model-specific defaults in `config.py`. Evaluation never searches or selects parameters. By default, ranking evaluation uses a fixed sample of up to 20,000 validation rows, and retrieval uses up to 1,000 queries. Sampling is independent of labels, and models for the same task use the same evaluation samples. `--full-eval` only removes these evaluation limits. Datasets below the limits use the full validation set. Full training times cannot be compared directly with the old smoke run, which trained on only 32 users.

| Dataset / model | Fixed overrides |
| --- | --- |
| MovieLens / DSSM | Embedding 16, batch 256, 3 epochs, sparse learning rate 0.01, tower layers 64/32, output dimension 32 |
| MovieLens / Wide & Deep, DeepFM, Rocket Launching | Embedding 16, batch 256, 6 epochs, sparse learning rate 0.03 |
| MovieLens / LightGBM | 300 iterations, depth 4, 15 leaves, minimum 100 rows per leaf, feature/bagging fractions 1, L2 regularization 5 |
| KuaiRec / DeepFM | Embedding 16, batch 256, sparse learning rate 0.03, logarithmic duration normalization; still 1 epoch |
| KuaiRand / Rocket Launching | Batch 256, shared layer 64, booster layers 32/16, light layer 16, logarithmic duration normalization; still 1 epoch |
| KuaiRand / DSSM | The existing retrieval configuration described below |

For the two ranking overrides with duration normalization, `column.duration_s.normalizer=method=log10,threshold=1,default=0` maps durations above one second to `log10(seconds)` and all other values to zero. The exported feature configuration applies the same transformation during training and serving. Prepared data and reusable training tables do not change. Every run records the resolved parameters in the model's `config.json`.

KuaiRand DSSM uses `user_id, tab` in the user tower and `video_id, tag, author_id` in the item tower. Its fixed parameters are embedding dimension 16, batch size 256, 3 epochs, sparse learning rate 0.01, hidden layers 64/32 in both towers, and output dimension 32. Counts and video duration are excluded from the tower inputs. Retrieval blends personalized similarity with a train-only popularity prior: `score = inner_product(user_embedding, item_embedding) + log1p(train_click_count)`. The existing prepared item click counts supply this prior; validation/test labels never enter it. The SQL recipe retains the complete catalog before filtering and sorting by the blended score. MovieLens and KuaiRec use the original similarity score with popularity weight zero. Parameters and the popularity weight are recorded in each model's `config.json`; change the fixed choices in `config.py`.

Set data and result paths with `--data-root` and `--output`; `BENCH_DATA_ROOT` also sets the data root. The defaults are `data/` and `runs/` in this directory. Changes to preparation rules create a new data version while retaining previous data and runs. Missing, outdated, or corrupted inputs raise errors and are not repaired automatically. File checksums and types are verified after preparation and again at the start of each evaluation.

Run folder names use the machine's local date and time, followed by the dataset and model, for example `2026-10-11_20-30-45_movielens100k_deepfm`. Runs covering multiple datasets use `all-datasets`, and `--model all` uses `all-models`. `--full-eval` adds `_full`; `--save-predictions` adds `_predictions`. If the same name already exists, a numeric suffix such as `_2` keeps the runs separate without overwriting files. The local time matches the timestamps in `run.log`.

Each prepared version includes fixed `training/<view>/split=train/part.parquet` files for ranking, trees, retrieval, and, where applicable, multitask training. Retrieval training includes only positives. `prepare --upload` uploads these views under `<storage>/datasets/<dataset>/<fingerprint>/<view>` and registers external Hive tables. Repeating the command checks and reuses existing tables without uploading again. Evaluation only reads these tables and checks their schema, location, and row count; it never exports or uploads training files. If a table is missing, run the preparation command with `--upload`. Existing tables with mismatched metadata or counts raise an error rather than being overwritten. An interrupted upload that left files without a registered table requires manual inspection before retrying; the command does not overwrite those files.

Connection settings and resources come from `deploy/env.sh`. You can override `NODE_IP`, `SQLREC_THRIFT_PORT`, `SQLREC_REST_PORT`, `KYUUBI_PORT`, `MILVUS_PORT`, `NAMESPACE`, `HADOOP_BIN`, and `KUBECTL_BIN`. Shared storage is read from Hadoop's `core-site.xml`, or configured with `BENCH_STORAGE_ROOT` / `BENCH_FS_URI`. Training defaults to 2 CPUs / 4Gi memory, and serving to 1 CPU / 2Gi memory; override these with `BENCH_TRAIN_CPU`, `BENCH_TRAIN_MEMORY`, `BENCH_SERVICE_CPU`, and `BENCH_SERVICE_MEMORY`. Image versions default to `${SQLREC_VERSION}-cpu`; override them with `BENCH_TZREC_VERSION` and `BENCH_GBDT_VERSION`. If Milvus authentication is enabled, set `SQLREC_BENCH_MILVUS_TOKEN`. Credentials are not written to results.

The two SQL connections serve different purposes: Kyuubi (default port 30009) creates external Hive training tables and registers partitions during `prepare --upload`, and checks existing tables during evaluation; SQLRec (default port 30000) executes model training, export, deployment, and vector SQL. Prediction uses SQLRec HTTP (default port 30001). The evaluation entry point connects to Kyuubi first, then checks training inputs and runs model tasks.

## Read the results

See [Model Quality Test](../../docs/en/docs/performance/model-benchmark.md) for dataset and training configurations, the complete audited result tables, and evaluation scope. Its KuaiRec results supersede the earlier measurements described in the historical notes below.

`summary.md` in each new run directory is the main report:

- Ranking: AUC ↑ and LogLoss ↓. MMoE's click and long_view tasks appear separately.
- Retrieval: Recall@50 ↑ and NDCG@50 ↑. Evaluation preserves the full candidate catalog, all ground-truth targets for selected queries, and filtering of previously seen items.
- Each row represents one dataset/model/task, with sample count, elapsed time, and completion status. One model typically produces 3 rows; the full matrix produces about 23.
- AUC is N/A for a single-class evaluation set. Missing or duplicate prediction IDs raise errors. Empty responses, missing queries, and queries without positives are handled separately.

`COMPLETED` means execution, prediction checks, and service cleanup finished. It does not indicate that a quality threshold was met. Results should not be ranked directly across datasets: MovieLens measures rating preference; KuaiRand measures clicks on mixed standard/random exposures; KuaiRec measures observed watch_ratio preference, with retrieval candidates restricted to each selected query's held-out observed items. Unobserved KuaiRec interactions are not added as negatives. Sampled results estimate performance under the current configuration rather than exact metrics for the full dataset.

`results.json` stores core metrics, necessary coverage information, data versions, and stage timings. Each model directory contains `config.json`, `statements.sql`, and `resources.json`. Predictions are used in memory to calculate metrics and are not saved by default. Runs contain no training data copies, query target/filter sets, or prediction shards.

With `--save-predictions`, each model saves one `predictions.parquet`: ranking stores `row_id` and task probabilities; retrieval stores `query_id`, `item_id`, and `score`. An empty retrieval response is represented by a row with its query ID and null item/score. Input features and labels are not copied into prediction files. API requests still use batches, independently of file output.

On failure or Ctrl-C, the script attempts to release the current model's services and confirms that their Kubernetes Deployments, Services, ReplicaSets, and Pods are gone and training/export Jobs are no longer active. If cleanup cannot be verified, it stops and records an error. The script does not cancel training Jobs or retry writes with uncertain outcomes. After a forced process termination or power loss, use `resources.json` to check for remaining resources manually. Models/checkpoints, APIs, and indexes are retained for inspection and are not reused across runs. Versioned training tables and files are shared across runs and are retained.

## Suggested code reading order

1. [`__main__.py`](__main__.py): selects models/datasets, runs them sequentially, saves results, and cleans up in `finally`.
2. [`config.py`](config.py): fixed model parameters, evaluation budgets, and deployment settings. The three YAML files contain only dataset inputs, fields, and preprocessing definitions.
3. [`ranking.py`](ranking.py): the shared batch prediction path for ranking networks, tree models, and MMoE.
4. [`metrics.py`](metrics.py), [`report.py`](report.py): four metrics and two small tables.
5. [`retrieval.py`](retrieval.py): DSSM's two towers, loading the full item catalog through public SQL, and retrieval for each query.
6. [`data.py`](data.py), [`prepare.py`](prepare.py), `prepare_*.py`: local data validation, sampling, reusable versioned training inputs, and data preparation without leakage.
7. [`client.py`](client.py), [`resources.py`](resources.py): public communication interfaces, resource lifecycle, and cleanup with time limits.

Ranking API batches default to 128 rows. Item SQL batches remain at 32 to avoid Calcite planning overhead from large `UNION ALL` expressions. Short SQL operations poll status every 0.1 seconds; training, export, and service operations poll every second. Training and inference run sequentially, with no plugins, automatic concurrency, recovery, or performance tuning framework.

Before the training-data reuse and output changes, the full run on 2026-10-11 evaluated all 22 combinations with full training and validation data in approximately 68 minutes 48 seconds. All combinations were COMPLETED, and all 25 services were confirmed released. See the [full run report](runs/20261011_103954_4123eb81/summary.md).

The subsequent data/output changes passed 29 local tests, real preparation and reuse checks for all 10 training views, and MovieLens DeepFM/DSSM runs with unchanged metrics and verified service cleanup. DeepFM was tested with default output and with prediction saving; all 9,877 saved probabilities matched the earlier full run exactly. The default DeepFM run contained only 8 files totaling 7,890 bytes. The complete 22-combination matrix was not rerun after these changes.

A batch of 256 returned HTTP 413 under the existing 64 KiB request limit, so 128 was selected after testing. See the [implementation summary](../../docs/reviews/model-quality-implementation.md) for validation scope and measured timings. Request counts alone are not used to infer overall evaluation speedup.

The KuaiRand DSSM update was validated on all 38,079 validation queries and a fixed 1,000-query sample from the later test period. Full-validation Recall@50/NDCG@50 improved from 3.98%/1.62% to 13.09%/5.30%; test-sample results improved from 2.57%/1.34% to 8.03%/3.69%. The full run took about 23 minutes 36 seconds, including the extra test check, versus about 21 minutes 32 seconds previously. All services were released. MovieLens DSSM predictions remained identical, and 29 local tests passed. See the [optimization review](../../docs/reviews/kuairand-recall-optimization.md) for experiment details, scope, and tradeoffs.

The later ranking update validated six dataset/model combinations through real SQL training, export, deployment, and prediction, followed by independent test-set checks. MovieLens DeepFM validation AUC improved from 0.7635 to 0.7834; KuaiRec DeepFM improved from 0.8623 to 0.8669 on the fixed 20,000-row sample, with its sampled run taking about 232 seconds versus 421 seconds for the baseline. KuaiRand Rocket Launching kept validation AUC essentially unchanged and reduced LogLoss from 0.7092 to 0.6495. MMoE and the other tree trials were not adopted where probability quality worsened. The full 22-combination matrix was not rerun, and the existing DSSM configurations were preserved. See the [ranking optimization review](../../docs/reviews/other-model-optimization.md) for all accepted metrics, test results, and timing limits.

A further validation-selected MovieLens DSSM update uses 3 epochs and larger embeddings/towers. Full-validation Recall@50/NDCG@50 improved from 23.95%/11.01% to 27.89%/13.15%; full-test results improved from 23.18%/11.31% to 26.54%/12.68%. The formal CLI reproduced all 46,300 recommendations exactly, 29 local tests passed, and the other 21 parameter sets were unchanged. The eight additional ranking trials and the six-epoch DSSM trial were rejected. See [document-ready results](RESULTS.md) for complete-validation ranking examples and explicit test scopes, and the [latest review](../../docs/reviews/publication-model-optimization.md) for validation details.
