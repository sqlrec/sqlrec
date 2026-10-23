# SQL Performance Benchmark

Measures throughput and latency of the SQLRec `main_rec` HTTP API using a
MovieLens-1M recommendation workflow. The workload covers multi-source recall,
Redis lookups, Milvus vector search, deduplication, diversification, and logging.

This directory measures execution performance. Recommendation-quality metrics
such as AUC and Recall@K are evaluated in [`model_quality`](../model_quality/).

## Run

After deploying the required services, run from the repository root:

```bash
cd benchmark/sql_performance
bash init.sh
bash benchmark.sh
```

`init.sh` prepares data, trains and deploys models, loads online features, and
registers the recommendation SQL functions and API. These preparation steps are
outside the timed benchmark.

`ml_ratings.rating` retains the normalized original MovieLens score `(score - 1) / 4`.
`compute_features.sql` creates binary `FLOAT` labels in `ml_sample.rating`: original
scores of 4 or 5 map to `1.0`, and scores of 1 through 3 map to `0.0`. TZRec training
AUC and Accuracy require 0/1 labels; normalized soft labels must not be used with
these metrics. The `ml_recall_sample` positive-pair filter selects the same 4/5-star
interactions as before. Regenerate both sample tables before retraining an existing
setup; changing the SQL does not update previously generated data or checkpoints.

Model training explicitly enables TensorBoard, training metrics, and all six
native summary types: `loss`, `learning_rate`, `global_gradient_norm`,
`gradient_norm`, `parameter`, and `gradient`. WideAndDeep records training AUC
and Accuracy; DSSM records training Recall@1/5/10/20. Parameter and gradient
histograms add preparation overhead and use the default 100-step log interval.

The generated model configuration also includes evaluation AUC, Accuracy, and
NormalizedEntropy for WideAndDeep, and Recall@1/5/10/20 for DSSM. This workload
does not define a separate validation set; set `eval_input_path` in the training
options to collect evaluation curves. Event files are saved under each origin
checkpoint's `model_dir`, with evaluation events under `eval_val/` when enabled.

`benchmark.sh` uses `wrk` with the workload in `request.lua`. Default requests
disable online model inference and use random user embeddings. Initialization
also checks the inference-enabled workflow.

SQLRec authentication defaults to enabled through `deploy/env.sh`. Both scripts
use `AUTH_ENABLED`, `AUTH_USERNAME`, and `AUTH_PASSWORD`; override them with
environment variables or edit that file. `init.sh` uses `bin/beeline.sh` for
SQLRec connections. HTTP warm-up and timed requests use Basic authentication
through `wrk -H`. Check the non-2xx response count when reviewing results.
Set `AUTH_ENABLED=false` for a server
with authentication disabled. Kyuubi and Milvus keep their own authentication.
Credentials are not written to benchmark files.

See the [English guide](../../docs/en/docs/performance/sql-benchmark.md) or
[中文说明](../../docs/docs/performance/sql-benchmark.md) for deployment requirements,
workload details, and historical results.
