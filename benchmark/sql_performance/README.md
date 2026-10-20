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
