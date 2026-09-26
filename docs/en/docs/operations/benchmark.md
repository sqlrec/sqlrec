# Benchmark

This page explains how to run the benchmark in `benchmark/movielens/` and records one historical result. The numbers apply only to the specified hardware, data, and deployment configuration; they are not a performance guarantee for other environments.

## What the Benchmark Covers

The benchmark calls the `main_rec` API and includes:

- four recall paths: global popularity, user genres, ItemCF, and Milvus vector search;
- exposure deduplication and recall merging;
- item-detail lookup and genre diversification;
- asynchronous recommendation-log writes to Kafka;
- exposure-history writes to Redis.

::: warning Test scope
The default initialization script does not train or call a model service. It generates random item embeddings during import and calls `random_vec('64')` for each user request. The result therefore excludes real model-inference latency and says nothing about recommendation quality.
:::

Model creation, training, export, and invocation examples are in `benchmark/movielens/init_model.sql`; `init.sh` does not run that file automatically.

## Dataset and Flow

The benchmark uses MovieLens 1M:

| Item | Size |
|------|------|
| Users | 6,040 |
| Items | 3,706 movies |
| Ratings | About 1 million |
| Embedding dimensions | 64 |

The recommendation flow is defined in `benchmark/movielens/init_sqlrec_sql.sql`.

| Stage | Default behavior |
|-------|------------------|
| Global popularity | Read 300 rows from `global_hot_item` |
| Genre recall | Query `genre_hot_item` from `user_interest_genre`, limit 300 |
| ItemCF recall | Query `itemcf_i2i` from `user_recent_click_item`, limit 300 |
| Vector recall | Query Milvus with a random user vector, limit 300 |
| Deduplication | Remove items exposed within the last hour |
| Ranking | `rank_fun_simple` joins item information |
| Diversification | Window 3, at most 1 item per genre, return 10 |

Request `params` can override `recall_fun` or `rank_fun`. A model-based ranking function requires its model and Service to be deployed first.

## Prerequisites

Start the complete environment by following [Service Deployment](./deployment.md). `init.sh` also uses or installs `wrk`, starts Kyuubi, and downloads MovieLens data and Python dependencies.

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
cd benchmark/movielens
bash init.sh
```

The initialization script and `mvn test` share the repository root `.venv`. The script creates it when needed and installs benchmark dependencies. Set `PYTHON_BOOTSTRAP` to choose the base Python interpreter (3.10 or newer).

The script:

1. Deploys Kyuubi and prepares `wrk`.
2. Creates the Milvus `item_embedding` collection and index.
3. Downloads MovieLens 1M, converts it to Parquet, and uploads it to HDFS.
4. Creates offline and online connector tables.
5. Computes popularity, genre, and ItemCF features with Spark SQL.
6. Writes features to Redis and random item vectors to Milvus.
7. Registers SQL functions and the `main_rec` API.
8. Calls `main_rec` once through Beeline as a basic check.

`init.sh` changes HDFS, Redis, Milvus, Kafka, and SQLRec metadata in the target environment. Do not run it against a shared or production environment.

## Run the Benchmark

```bash
cd benchmark/movielens
bash benchmark.sh
```

The current script uses a 10-second warm-up with one thread and connection, followed by a 30-second run with 10 threads and 10 connections against `/api/v1/main_rec`. Each request chooses a user ID from 0 to 5000.

Treat the current `benchmark.sh` and `request.lua` as authoritative. Record concurrency and duration with the result whenever you change them.

## Historical Result

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
