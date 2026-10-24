# SQL Performance Test

This page explains how to run the benchmark in `benchmark/sql_performance/` and records one historical result. The numbers apply only to the specified hardware, data, and deployment configuration; they are not a performance guarantee for other environments.

## What the Benchmark Covers

The benchmark calls the `main_rec` API and includes:

- four recall paths: global popularity, user genres, ItemCF, and Milvus vector search;
- exposure deduplication and recall merging;
- item-detail lookup and genre diversification;
- asynchronous recommendation-log writes to Kafka;
- exposure-history writes to Redis.

`init.sh` runs `init_model.sql` to train, export, and deploy the recall and ranking models. Loading item features calls the DSSM item tower Service and writes its embeddings to Milvus. These preparation steps are outside the timed `wrk` run. By default, benchmark requests do not call an online model Service.

## Dataset and Flow

The benchmark uses MovieLens 1M:

| Item | Size |
|------|------|
| Users | 6,040 |
| Items | 3,706 movies |
| Ratings | About 1 million |
| Embedding dimensions | 64 |

The recommendation flow is defined in `benchmark/sql_performance/init_sqlrec_sql.sql`.

| Stage | Default behavior |
|-------|------------------|
| Global popularity | Read 300 rows from `global_hot_item` |
| Genre recall | Query `genre_hot_item` from `user_interest_genre`, limit 300 |
| ItemCF recall | Query `itemcf_i2i` from `user_recent_click_item`, limit 300 |
| Vector recall | Query Milvus with a local random user vector, limit 300 |
| Deduplication | Remove items exposed within the last hour |
| Ranking | `rank_fun_simple` joins item information without inference |
| Diversification | Window 3, at most 1 item per genre, return 10 |

`request.lua` explicitly sets `use_recall_service=false` and `rank_fun=rank_fun_simple` to keep the timed workload stable. To exercise online inference, set request `params` to `{"use_recall_service":"true","rank_fun":"rank_fun"}`. That path calls the DSSM user tower and Wide & Deep ranking Services. Random user vectors in the default benchmark are for performance measurement, not recommendation-quality evaluation.

The models include two `FLOAT` features: `age_years = CAST(age AS FLOAT)` and `genre_count = CAST(number of genres AS FLOAT)`. MovieLens `age` is an age-group code; `age_years` is its numeric representation, not an exact age. Their minmax parameters are `[0,100]` and `[0,18]`, respectively, with missing values defaulting to 0. Both enter the ranking model's deep branch. DSSM uses `age_years` in the user tower and `genre_count` in the item tower. Offline samples, online requests, and item-embedding loading use the same calculations. `rating` remains a label and never becomes an input feature.

`ml_ratings.rating` retains the normalized original score `(score - 1) / 4`. `compute_features.sql` produces binary `FLOAT` labels in `ml_sample.rating`: original scores of 4 or 5 become `1.0`, and scores of 1 through 3 become `0.0`. TZRec training AUC and Accuracy require 0/1 labels and must not use normalized soft labels. `ml_recall_sample` still selects the same 4/5-star positive pairs. Regenerate both sample tables before retraining an existing setup; editing the SQL does not update existing data or checkpoints.

Training SQL explicitly enables training metrics and all six native TensorBoard summary types: loss, learning rate, total gradient norm, per-parameter gradient norm, parameter histograms, and gradient histograms. Ranking records training AUC/Accuracy, and recall records training Recall@1/5/10/20. Histogram collection adds preparation overhead outside the timed `wrk` run. Evaluation curves additionally require `eval_input_path`.

`init.sh` keeps `ml_sample` and `ml_recall_sample` on JuiceFS and creates identical HDFS copies in `ml_sample_hdfs` and `ml_recall_sample_hdfs`. Ranking (`rank_model`) continues to use JuiceFS. DSSM (`recall_model_hdfs`) reads the HDFS recall samples and writes training checkpoints, TensorBoard events, and both tower exports to HDFS. The separate model name prevents `IF NOT EXISTS` from retaining an older JuiceFS definition. Service names remain `rank-service`, `recall-service-user`, and `recall-service-item`; subsequent Milvus item vectors come from the HDFS model.

## Prerequisites

Start the complete environment by following [Service Deployment](../operations/deployment.md). `init.sh` also uses or installs `wrk`, starts Kyuubi, and downloads MovieLens data and Python dependencies.

The historical result below used:

| Item | Configuration |
|------|---------------|
| CPU | AMD Ryzen 5600H |
| Memory | 32 GB DDR4 |
| Operating system | Debian 12 |
| Deployment | Minikube, one SQLRec instance |

## Initialize the Data

From the repository root:

```bash
cd benchmark/sql_performance
bash init.sh
```

The initialization script and `mvn test` share the repository root `.venv`. The script creates it when needed and installs benchmark dependencies. Set `PYTHON_BOOTSTRAP` to choose the base Python interpreter (3.10 or newer).

The script:

1. Deploys Kyuubi and prepares `wrk`.
2. Creates the Milvus `item_embedding` collection and index.
3. Downloads MovieLens 1M, converts it to Parquet, and uploads it to HDFS.
4. Creates offline and online connector tables.
5. Computes popularity, genre, and ItemCF features with Spark SQL.
6. Trains, exports, and deploys the Wide & Deep and DSSM models.
7. Writes features to Redis and model-generated item vectors to Milvus.
8. Registers SQL functions and the `main_rec` API.
9. Calls `main_rec` through Beeline in both default and model-backed modes as basic checks.

`init.sh` changes HDFS, Redis, Milvus, Kafka, and SQLRec metadata in the target environment. Do not run it against a shared or production environment.

## Run the Benchmark

```bash
cd benchmark/sql_performance
bash benchmark.sh
```

The current script uses a 10-second warm-up with one thread and connection, followed by a 30-second run with 10 threads and 10 connections against `/api/v1/main_rec`. Each request chooses a valid MovieLens user ID from 1 to 6040.

Treat the current `benchmark.sh` and `request.lua` as authoritative. Record concurrency and duration with the result whenever you change them.

## Historical Result

The following result was measured with the earlier mock-item-vector setup and a different user-ID range. The current setup imports model-generated item vectors, so the numbers are not directly comparable.

```text
Running 30s test @ http://192.168.49.2:30001/api/v1/main_rec
  10 threads and 10 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     6.73ms    3.16ms  90.29ms   94.46%
    Req/Sec   151.20     16.58   191.00     73.67%
  45231 requests in 30.02s, 87.90MB read
Requests/sec:   1506.47
Transfer/sec:      2.93MB
```

| Metric | Value | Meaning |
|--------|-------|---------|
| Average latency | 6.73 ms | `wrk` request latency |
| Latency standard deviation | 3.16 ms | `wrk` statistic |
| Maximum latency | 90.29 ms | Maximum observed in this run |
| Per-thread requests/sec | 151.20 | Average in `Thread Stats`, not total QPS |
| Total requests | 45,231 | During 30.02 seconds |
| Total QPS | 1,506.47 | `Requests/sec` |
| Transfer rate | 2.93 MB/s | `Transfer/sec` |

When comparing runs, keep at least the dataset, SQLRec version, JVM, concurrency, connector deployment, and model-service usage consistent.
