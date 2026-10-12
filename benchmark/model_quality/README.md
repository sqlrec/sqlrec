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
| KuaiRec / Wide & Deep, Rocket Launching | Embedding 16, batch 256, sparse learning rate 0.01, logarithmic duration normalization; still 1 epoch |
| KuaiRec / DeepFM | Embedding 16, batch 256, sparse learning rate 0.03, logarithmic duration normalization; still 1 epoch |
| KuaiRec / DSSM | Embedding 8, batch 64, 1 epoch, tower layers 32/16, output dimension 16; sparse/dense learning rates remain 0.001 |
| KuaiRand / Rocket Launching | Batch 256, shared layer 64, booster layers 32/16, light layer 16, logarithmic duration normalization; still 1 epoch |
| KuaiRand / DSSM | Embedding 16, batch 256, 6 epochs, sparse learning rate 0.01, tower layers 64/32, output dimension 32 |

For ranking overrides with duration normalization, `column.duration_s.normalizer=method=log10,threshold=1,default=0` maps durations above one second to `log10(seconds)` and all other values to zero. The exported feature configuration applies the same transformation during training and serving. Prepared data and reusable training tables do not change. Every run records the resolved parameters in the model's `config.json`.

MovieLens and KuaiRec prepare two additional numeric features: `user_positive_rate` and `item_positive_rate`. Each is `(history_positives + 1) / (history_interactions + 2)`, a smoothed positive rate. They use the same leakage controls as the existing counts: exclude the current training interaction, and use training data only for validation/test. `POSITIVE_RATE_MODELS` in `config.py` enables them for MovieLens DeepFM, Rocket Launching, CatBoost, and XGBoost, and KuaiRec Wide & Deep, DeepFM, Rocket Launching, and LightGBM. Other models retain their previous inputs. Training tables contain the full view schema; each model selects only its configured inputs. Tree models still use numeric history features (and video duration where available), without raw ID features.

All DSSM retrieval scores are strictly `inner_product(user_embedding, item_embedding)`. There is no popularity term, extra vector coordinate, normalization, or reranking score. Milvus uses a FLAT inner-product index over the complete eligible catalog; SQL applies the existing seen/observed-item filters and orders by this score, then item ID for ties. KuaiRand DSSM uses `user_id, tab` in the user tower and `video_id, tag, author_id` in the item tower, without history counts or duration.

DSSM uses native `PoolNegativeSampler` on all three datasets: `negative_sampling=pool`, `negative_sample_count=256`, `negative_pool_size=4096`, and `item_id_column=video_id`. MovieLens trains for 3 epochs, KuaiRand for 6, and KuaiRec for 1. The pool warms up from the existing positive training parquet input; it deduplicates raw item features, uses FIFO eviction, excludes current batch-positive IDs, and samples shared negatives uniformly with replacement. No separate item table is required. The pool covers items observed in training positives, not the full catalog. Validation pools warm up only from training data and do not update from validation rows. Each run records `retrieval.score=inner_product` in `config.json`.

Set data and result paths with `--data-root` and `--output`; `BENCH_DATA_ROOT` also sets the data root. The defaults are `data/` and `runs/` in this directory. Changes to preparation rules create a new data version while retaining previous data and runs. Missing, outdated, or corrupted inputs raise errors and are not repaired automatically. File checksums and types are verified after preparation and again at the start of each evaluation.

Run folder names use the machine's local date and time, followed by the dataset and model, for example `2026-10-11_20-30-45_movielens100k_deepfm`. Runs covering multiple datasets use `all-datasets`, and `--model all` uses `all-models`. `--full-eval` adds `_full`; `--save-predictions` adds `_predictions`. If the same name already exists, a numeric suffix such as `_2` keeps the runs separate without overwriting files. The local time matches the timestamps in `run.log`.

Each prepared version includes fixed `training/<view>/split=train/part.parquet` files for ranking, trees, retrieval, and, where applicable, multitask training. Retrieval training includes only positives. `prepare --upload` uploads these views under `<storage>/datasets/<dataset>/<fingerprint>/<view>` and registers external Hive tables. Repeating the command checks and reuses existing tables without uploading again. Evaluation only reads these tables and checks their schema, location, and row count; it never exports or uploads training files. If a table is missing, run the preparation command with `--upload`. Existing tables with mismatched metadata or counts raise an error rather than being overwritten. An interrupted upload that left files without a registered table requires manual inspection before retrying; the command does not overwrite those files.

Connection settings and resources come from `deploy/env.sh`. You can override `NODE_IP`, `SQLREC_THRIFT_PORT`, `SQLREC_REST_PORT`, `KYUUBI_PORT`, `MILVUS_PORT`, `NAMESPACE`, `HADOOP_BIN`, and `KUBECTL_BIN`. Shared storage is read from Hadoop's `core-site.xml`, or configured with `BENCH_STORAGE_ROOT` / `BENCH_FS_URI`. Training defaults to 2 CPUs / 4Gi memory, and serving to 1 CPU / 2Gi memory; override these with `BENCH_TRAIN_CPU`, `BENCH_TRAIN_MEMORY`, `BENCH_SERVICE_CPU`, and `BENCH_SERVICE_MEMORY`. Image versions default to `${SQLREC_VERSION}-cpu`; override them with `BENCH_TZREC_VERSION` and `BENCH_GBDT_VERSION`. Build and deploy SQLRec and its model images from the current source; the TZRec image must contain native `PoolNegativeSampler` and SQLRec must support its model parameters. If Milvus authentication is enabled, set `SQLREC_BENCH_MILVUS_TOKEN`. Credentials are not written to results.

The two SQL connections serve different purposes: Kyuubi (default port 30009) creates external Hive training tables and registers partitions during `prepare --upload`, and checks existing tables during evaluation; SQLRec (default port 30000) executes model training, export, deployment, and vector SQL. Prediction uses SQLRec HTTP (default port 30001). The evaluation entry point connects to Kyuubi first, then checks training inputs and runs model tasks.

## Read the results

The [Model Quality Test](../../docs/en/docs/performance/model-benchmark.md) documents the current fixed configuration and verified full-validation results for all models. DSSM Recall@50/NDCG@50 are 34.26%/19.15% on MovieLens, 11.05%/4.45% on KuaiRand, and 57.82%/51.47% on KuaiRec. All 22 combinations have been retrained and evaluated through the standard CLI using the complete validation sets. Saved predictions passed independent metric and identity checks; actual native configurations and service release were verified. See the [full run](runs/2026-10-15_01-53-44_all-datasets_all-models_full_predictions/summary.md) and its [verification record](runs/2026-10-15_01-53-44_all-datasets_all-models_full_predictions/verification.json). See the [configuration audit](../../docs/reviews/model-quality-current-status.md) for parameter and feature checks, and the [Pool capacity comparison](../../docs/reviews/pool-negative-sampler-larger-pools.md) for why capacity remains 4096. Removed popularity-blended and logQ experiments do not describe the current recipe.

`summary.md` in each new run directory is the main report:

- Ranking: AUC ↑ and LogLoss ↓. MMoE's click and long_view tasks appear separately.
- Retrieval: Recall@50 ↑ and NDCG@50 ↑. Evaluation preserves the full candidate catalog, all ground-truth targets for selected queries, and filtering of previously seen items.
- Each row represents one dataset/model/task, with sample count, elapsed time, and completion status. One model typically produces 3 rows; the full matrix produces about 23.
- AUC is N/A for a single-class evaluation set. Missing or duplicate prediction IDs raise errors. Empty responses, missing queries, and queries without positives are handled separately.

`COMPLETED` means execution, prediction checks, and service cleanup finished. It does not indicate that a quality threshold was met. Results should not be ranked directly across datasets: MovieLens measures rating preference; KuaiRand measures clicks on mixed standard/random exposures; KuaiRec measures observed watch_ratio preference, with retrieval candidates restricted to each selected query's held-out observed items. Unobserved KuaiRec interactions are not added as negatives. Sampled results estimate performance under the current configuration rather than exact metrics for the full dataset.

`results.json` stores core metrics, necessary coverage information, data versions, and stage timings. Each model directory contains `config.json`, `statements.sql`, and `resources.json`. SQL is generated with indented fields, options, and query clauses; `statements.sql` records the executed SQL for review. Predictions are used in memory to calculate metrics and are not saved by default. Runs contain no training data copies, query target/filter sets, or prediction shards.

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

Ranking API batches contain 512 rows. DSSM loads the complete item catalog in batches of 256 through a SQL function defined once, using typed JSON inputs instead of repeated `UNION ALL` SQL. Item-loading requests write to Milvus and are never automatically retried. Recall uses four concurrent requests, each retaining its own query, filters, and Top-50 result. Each worker owns its HTTP session. Models still run sequentially. Short SQL operations poll every 0.1 seconds; training, export, and service operations poll every second. The fixed execution settings live in `config.py`; there is no performance tuning framework.

The 512-row prediction batches require the current 1 MiB REST request limit (`REST_MAX_CONTENT_LENGTH=1048576` in bytes). Update older deployments with a 64 KiB limit before running the benchmark. See the [execution speed review](../../docs/reviews/model-quality-speed.md) for measured timings and result-equivalence checks.

The quality checks cover data splits, training-only statistics, prediction identities, independent metric recalculation, and pure inner-product recall. The latest ranking configurations were selected on full validation and checked on the existing test splits; small validation gains are not established as stable across seeds. Pool configurations were evaluated on complete validation sets, without new independent test-set evaluation. All choices use seed 123. See the [ranking review](../../docs/reviews/model-quality-pure-ip-optimization.md), [KuaiRand Pool review](../../docs/reviews/tzrec-native-item-pool-result.md), and [other-dataset Pool review](../../docs/reviews/pool-negative-sampler-other-datasets.md) for the validation scope and artifacts.
