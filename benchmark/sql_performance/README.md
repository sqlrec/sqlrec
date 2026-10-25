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

Both WideAndDeep and DSSM train with two nodes (`nnodes=2`), one training
process per node (`nproc_per_node=1`), and one spawned DataLoader worker per
process. Each training Pod requests 2 CPUs and 4 GiB of memory. Both Pods can
run on a single-node Minikube cluster with enough available resources
(4 CPUs and 8 GiB in total); no host affinity is required.
`nnodes=2` counts torchrun node agents, not physical Kubernetes nodes.
Export remains single-process.

This benchmark declares training hyperparameters and DSSM sampling in TRAIN WITH.
MODEL may also provide defaults; operation WITH values override model defaults.
EXPORT reuses batch_size and runs with one process and one loading worker.
Unused options are ignored. Same-name failed retries
preserve stored checkpoints and use the current SQL settings; supply the training
input and options again. New FROM tasks that change learning rates must
set restore_optimizer=false.

Multi-node training fails the entire Job when any worker Pod fails; individual
Pods are not retried against an existing process group. Recreate the whole Job
from its generated YAML to resume the same output directory from its latest
checkpoint. Generated training commands enable `--continue_train`; a fresh
output directory starts normally. Rebuild both the TZRec and SQLRec server
images to pick up the worker sharding and Job-generation fixes.

HDFS and JuiceFS must both be deployed (`deploy_components.sh` includes both).
`ml_sample` and `ml_recall_sample` remain on JuiceFS; feature preparation copies
them into `ml_sample_hdfs` and `ml_recall_sample_hdfs` on `hdfs://myhdfs`.
WideAndDeep (`rank_model`) trains, exports, and serves from JuiceFS. DSSM
(`recall_model_hdfs`) uses the HDFS recall samples and stores checkpoints,
TensorBoard events, and both tower exports on HDFS. The existing recall service
names are retained, so online feature loading and requests use the HDFS model.
The separate DSSM model name also avoids reusing an older JuiceFS definition
through `CREATE MODEL IF NOT EXISTS`.

On ARM, run `bash deploy/hadoop/init.sh` after updating an older deployment,
then `bash deploy/hdfs/deploy.sh`. The Hadoop archive must match
`CONTAINER_ARCH`; Python training requires its native `libhdfs.so`, even when
the Java Hadoop CLI already works. Each initializer run refreshes native libraries
in existing clients while preserving configuration and additional JARs.
The single-DataNode deployment also disables pipeline node replacement so that
PyArrow's default replication request does not break TensorBoard event appends.
Rebuild the TZRec image with `bash bin/build_model_docker.sh tzrec` when upgrading
an existing installation: HDFS export needs the directory-glob fix and the pinned
fsspec version that supports PyTorch checkpoint stream reads.
Use a SQLRec server image built from the current source as well, so generated
service names and training options match these SQL files.

The deployed storage integration tests exercise both backends explicitly:

```bash
bash bin/test.sh -pl sqlrec-core -am \
  -Dtest=RemoteHdfsAccessIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false
```

Run this command from the repository root. `HADOOP_TEST_HDFS_URI` and
`HADOOP_TEST_JUICEFS_URI` override the test endpoints (defaults:
`hdfs://myhdfs` and `jfs://myjfs`); an unavailable backend fails the test.

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
