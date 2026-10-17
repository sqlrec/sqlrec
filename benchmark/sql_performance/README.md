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

See the [English guide](../../docs/en/docs/performance/sql-benchmark.md) or
[中文说明](../../docs/docs/performance/sql-benchmark.md) for deployment requirements,
workload details, and historical results.
