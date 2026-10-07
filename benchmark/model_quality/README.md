# Model Quality Benchmark

This benchmark follows SQLRec's public application workflow: SQL model definition,
training, export, service deployment, and SQL or SQL API inference. DSSM retrieval
loads embeddings through public SQL into the real Milvus Connector. Python prepares
inputs, collects public returns, validates coverage, and computes metrics.

This is the single maintained guide for this directory. Prepared data and generated
runs are ignored by Git. The broader roadmap remains in the
[framework design](../../docs/model-quality-sql-framework-design.md) and
[dataset portfolio plan](../../docs/model-quality-dataset-portfolio-plan.md).

The default smoke profile exercises the complete public workflow with a small
integration cohort. Its metrics describe that cohort; use the full profile for
full-data model quality evaluation.

## Quick start

With Python 3 and an existing SQLRec deployment built from the current project code,
run one command from the repository:

```bash
# Automatic environment setup, local regressions and all 22 dataset/model runs.
./benchmark/model_quality/run_tests.sh

# The same complete suite with full training and validation data.
./benchmark/model_quality/run_tests.sh --profile full

# Local framework regressions only; no cluster access or data downloads.
./benchmark/model_quality/run_tests.sh unit

# Quality workflows only; skip local regression execution.
./benchmark/model_quality/run_tests.sh quality

# Optional subset; --config can be repeated.
./benchmark/model_quality/run_tests.sh \
  --config benchmark/model_quality/configs/kuairand_multitask.yaml
```

No separate pip, download, preparation or environment-source commands are needed.
The default `all` mode uses the smoke profile and a single training seed, 123.
The script creates/reuses `.venv`, installs missing requirements, sources
`deploy/env.sh`, checks deployment access, prepares complete datasets, then executes
normal public SQL/API workflows sequentially. It also works from another working
directory when invoked by its absolute path. Deployment installation/upgrades and
uncertain mutation retries remain separate from benchmark execution.

## Configuration

Configuration has three owners:

| Owner | Settings |
| --- | --- |
| `deploy/env.sh` and exported overrides | Addresses, ports, namespace, clients, image versions, resource defaults |
| `configs/datasets/*.yaml` | Download sources/checksums, preparation rules, all column types/roles, tasks, views, candidate protocols and dataset metrics |
| `configs/*.yaml` | Explicit dataset-definition reference, view selection, models and experiment parameters |

A compact experiment is sufficient:

```yaml
dataset:
  config: datasets/movielens100k.yaml
  view: recall
models:
  - family: tzrec.dssm
    params:
      output_dim: 16
      batch_size: 64
      num_epochs: 1
evaluation:
  ks: [20, 50, 100]
```

`name` defaults to the config filename stem. Model IDs default to the family suffix.
The resolver expands this into the existing strict schema-v3 contract before build.
Dataset references are resolved relative to the experiment YAML, independently of
the working directory. There is no dataset ID registry or dataset-name branch in
the public runner. There is no YAML inheritance, environment interpolation, or
embedded shell Python.
Existing complete schema-v3 configs remain accepted without deployment defaults.
Legacy frozen v3/v4/v5 plans remain immutable and do not reapply new defaults on resume.

Explicit YAML overrides win over machine defaults. Model hyperparameters come from
the experiment YAML only. Image versions and resource settings come from the
deployment environment. The supplied YAMLs use TZRec settings of embedding
dimension 8, batch size 64, one worker and
one epoch. DSSM uses 32/16 tower widths and output dimension 16. Ranking networks
use small family-specific widths; tree models use 100 iterations and learning
rate 0.05. These are initial iteration settings, not tuned quality claims.
`full` removes sampling limits; it does not silently change hyperparameters.

Training uses one `execution.seed` (default 123), not a seed list. Each configured
model is trained once; reports retain the actual seed and single-run metrics.
There is no mean/standard-deviation aggregation across seeds. Old complete configs
with exactly one `execution.seeds` entry are normalized by the config resolver;
multi-seed configs are rejected. Old frozen plans are read without rewriting them.
Dataset preparation has its own declared fixed seed in dataset YAML.

`serving.batch_size` defaults to 32 for inference and catalog-vector writes.
Typed SQL inputs use `SELECT ... UNION ALL` to preserve array-column aliases.
Large batches can cause excessive Calcite planning time; smaller batches retain
the complete catalog and metric inputs. Explicit experiment overrides are allowed.

### Dataset definitions

[MovieLens](configs/datasets/movielens100k.yaml),
[KuaiRand](configs/datasets/kuairand.yaml), and
[KuaiRec](configs/datasets/kuairec.yaml) contain the dataset-specific declarations.
Each dataset has one YAML for download, preparation and evaluation. `columns`
defines every prepared column's SQL type exactly once, including model features,
labels and audit metadata. A `role` marks a column as a potential model feature;
columns without a role cannot enter a named feature view. `tasks` references label
columns and declares binary/regression semantics, while `views` selects features,
tasks and compatible model families.

For data prepared externally, the minimal definition is:

```yaml
schema_version: 1
id: my_dataset
name: My-Prepared-Dataset
dataset:
  adapter: prepared
  user_key: customer_id
  item_key: product_id
  query_key: query_id
protocol:
  candidate_scope: full_catalog
  exclude_seen: true
evaluation:
  split: valid
  gauc_group: customer_id
columns:
  customer_id: {type: STRING, role: user}
  product_id: {type: STRING, role: item}
  label: {type: INT}
tasks:
  purchased: {column: label, type: binary, description: "observed purchase"}
views:
  retrieval:
    features: [customer_id, product_id]
    tasks: [purchased]
    models: [tzrec.dssm]
    evaluation:
      ks: [20, 50, 100]
```

An experiment selects `dataset.config: datasets/my_dataset.yaml` and
`dataset.view: retrieval`. View names are declared by YAML, not a fixed Python
list. Optional view `models` restricts allowed families; the normal public model
validator still checks features/tasks. A view can override protocol/evaluation
settings. Experiment overrides have the highest precedence.

Dataset keys and audit slices are configurable, including custom scalar audit
columns. Task descriptions may reference simple scalar prepared-manifest fields,
such as `{positive_threshold}`; missing fields fail instead of guessing a value.
The expanded configuration and source definition path/SHA-256 are frozen in the
run. Resume does not reread a modified source YAML.

The bundled definitions also contain two sections:

| Section | Ownership |
| --- | --- |
| `source` | Official archive URL/version/checksum and `files`, a mapping from parser input roles to filenames |
| `preparation` | Adapter module, seed, primary task, identity/time references, query/static-item columns, defaults, array grouping keys and feedback statistics |
| `preparation.parameters` | Adapter-specific thresholds, split percentages/date windows, file-role selection and raw-format metadata |

Preparation selects column names from these declarations; it does not impose a
global feature list. Counts use only training feedback, temporal training excludes
all co-timestamp feedback, and pair training subtracts the pair's own feedback.
`write_frame` uses `columns` for Parquet types; experiment expansion selects those
same definitions for SQL. The prepared manifest records the definition snapshot,
path/SHA-256 and effective seed/threshold/split settings. CLI overrides are audited
in the manifest. Unknown output columns fail instead of creating an implicit schema.

Adding a dataset using supported prepared data, feature types and SQL protocols
requires a new dataset YAML and experiment YAML, without editing public Python.
New raw formats belong in dedicated preparation adapters; new SQL capabilities
are separate framework changes. Add the experiment to the fixed shell suite or
run it directly with `--config`.

### Environment

Quality modes source the existing `deploy/env.sh`; unit mode and `--help` do not.
Manual compact builds must source that file themselves. Environment defaults are
resolved once at build and frozen into `resolved_config.json` and the plan.

| Variable | Default / meaning |
| --- | --- |
| `PYTHON_BIN` | Optional existing interpreter; otherwise create/reuse repository `.venv/bin/python` |
| `NODE_IP`, `SQLREC_THRIFT_PORT`, `SQLREC_REST_PORT`, `KYUUBI_PORT`, `MILVUS_PORT`, `NAMESPACE` | Existing deployment environment |
| `BENCH_DATA_ROOT` | `benchmark/model_quality/data`; raw inputs at `<root>/raw/<dataset-id>/`, prepared versions at `<root>/prepared/<dataset-id>/<cache-key>/` |
| `BENCH_FS_URI` | Hadoop `fs.defaultFS`, read from local `core-site.xml`; must be shared storage |
| `BENCH_STORAGE_ROOT` | `<filesystem>/model_quality` |
| `BENCH_MODEL_BASE_URI` | `<filesystem>/user/sqlrec/models`; must match actual public artifact storage |
| `BENCH_TRAIN_CPU`, `BENCH_TRAIN_MEMORY` | `2`, `4Gi` |
| `BENCH_SERVICE_CPU`, `BENCH_SERVICE_MEMORY` | `1`, `2Gi` |
| `BENCH_TZREC_VERSION`, `BENCH_GBDT_VERSION` | `${SQLREC_VERSION}-cpu`; YAML `params.version` can override |
| `HADOOP_BIN`, `HADOOP_HOME`, `JAVA_HOME` | Existing deployment clients; explicit local reader overrides below |
| `KUBECTL_BIN` | `kubectl`, read-only preflight, runtime audit and resource release verification |
| `SQLREC_BENCH_MILVUS_TOKEN` | Optional Milvus credential; empty when authentication is disabled; never frozen as a value |

If both storage roots are explicitly set, discovering `fs.defaultFS` is unnecessary.
Unknown filesystem defaults fail clearly rather than silently choosing a local
filesystem. Ports/CPU values are converted to numbers and strictly validated.
Default HS2 authentication is NOSASL for SQLRec and NONE for Kyuubi; non-default
authentication/addresses can be set through explicit `sql.endpoints` overrides.
Endpoint password/token/authorization settings name environment variables.

Uploads use `HADOOP_BIN`, then the frozen audit command, then `hadoop` from PATH.
The local client command can change without changing frozen SQL or data locations.
Manual runtime reads can use `HADOOP_BIN` and `JAVA_HOME` on another machine.

### Profiles and categorical capacity

| Profile | Limits |
| --- | --- |
| `smoke` | Defaults to 32 training users, 256 paired-score observations, and 8 DSSM queries |
| `full` | No workflow sampling limits; rejects prepared manifests with `max_users` set |

Limits are selected by stable identities, independently of labels. Explicit smoke
limits can override defaults. A full run rejects limits rather than silently
removing them. Query sampling retains the full served catalog. Paired-score limits
do not shrink selected queries' retrieval targets or observed-candidate sets.
Single-class samples have undefined AUC and are reported accordingly.

The source preparation cohort and the workflow cohort are separate. History
features remain the source dataset's frozen prepared features. Smoke runs validate
integration and support debugging; their metrics do not estimate full-data quality.
Full runs still use the validation split. Test unlocking/model selection is future work.

Compact configs size string/tag hash buckets from distinct training values and,
for retrieval item features, the served catalog. The next power of two covering
20 times cardinality is capped at 4,194,304 buckets. Override with
`dataset.hash_bucket_multiplier`, `dataset.hash_bucket_max`, model `num_buckets`,
or explicit `column.<feature>.bucket_size` parameters. The report records actual
submitted bucket sizes and a uniform-hash shared-bucket estimate. This estimate is
not a measurement of TZRec's hash collisions; runtime audits check submitted sizes.

## Dataset and model suite

The ten configs cover 22 dataset/model combinations:

| Dataset | Recall | Ranking | Trees | Multitask |
| --- | --- | --- | --- | --- |
| MovieLens-100K | DSSM | Wide & Deep, DeepFM, Rocket Launching | CatBoost, LightGBM, XGBoost | Not applicable |
| KuaiRand-Pure | DSSM | Same three models | Same three models | MMoE |
| KuaiRec small_matrix | DSSM with observed candidates | Same three models | Same three models | Not applicable |

Each group has a file named `configs/<dataset-id>_<view>.yaml`, with explicit model
parameters and a shared dataset YAML reference. The shell explicitly
lists these files, runs the three DSSM configs first, then the other groups, and
runs every declared model once, serially, using the fixed execution seed. `--config` replaces the fixed suite.

Dataset semantics differ:

- MovieLens labels ratings >=4 positive and <=3 observed negative. These are rating
  preferences, not CTR. Recommended preparation is per-user chronological splitting;
  all training-rated items are excluded from retrieval.
- KuaiRand uses real `is_click` as `label`, plus real binary `long_view` for MMoE.
  Standard and random exposure metrics are also reported separately. Training uses
  standard exposure history with temporal validation/test cutoffs.
- KuaiRec labels `watch_ratio > 2` positive using the existing pair-hash holdout.
  Retrieval candidates are each query's held-out observed pairs, defined without
  consulting labels. Unobserved pairs are not turned into negative examples.

MMoE requires genuine multiple tasks; MovieLens and KuaiRec do not fabricate a
second task. Its click and long-view metrics remain separate.

Feature views are declared in each dataset YAML. Neural views contain string user/item IDs, tags,
applicable author/tab features, and frozen numeric history features. DSSM has only
user/item roles. Tree configs share explicit FLOAT history features and applicable
static duration; MovieLens omits its constant zero duration. IDs are not cast to
continuous numbers. The shared numeric tree view makes the three tree inputs
consistent; it does not make them equivalent to neural categorical views. A
categorical CatBoost enhancement can be a separate future experiment.

### Dataset preparation

The shell automatically deduplicates selected dataset YAML references, verifies
cached raw-file digests, downloads/extracts missing inputs, and invokes the declared
`preparation.adapter`. Dataset names, URLs, fields, split rules and adapter choices
remain in each dataset YAML. No dataset registry or name-specific branch exists in
the generic setup module.

Prepared caches are keyed by the dataset definition, raw-file hashes and adapter /
shared preparation code hashes. Changing model hyperparameters reuses data;
changing preprocessing rules or code creates a new immutable cache version.
Each reused cache must have current provenance, full-cohort semantics, matching
Parquet types, all split/query/catalog artifacts and valid file checksums. Legacy
directories without this contract are retained and rebuilt into managed caches.
Invalid managed versions are preserved under an `_invalid_...` suffix before repair.
A per-dataset file lock prevents simultaneous writers. Preparation writes a
staging directory and publishes it only after complete validation; interrupted
staging outputs are removed. Download partial archives can be resumed.

Both profiles prepare complete data without `max_users`; smoke sampling occurs
only when the public workflow inputs are frozen. `prepared_inputs.json` selects
verified cache paths automatically for build. Explicit `dataset.path` values are
reused only if they satisfy the declared complete dataset contract and are never
overwritten by automatic setup.

The following commands remain available for optional manual preparation; they are
not required by `run_tests.sh`. Run them from the repository root:

```bash
.venv/bin/python -m benchmark.model_quality.download_data \
  --dataset movielens100k --output benchmark/model_quality/data/raw/movielens100k
.venv/bin/python -m benchmark.model_quality.prepare_movielens \
  --raw benchmark/model_quality/data/raw/movielens100k \
  --output benchmark/model_quality/data/movielens100k --protocol user-time

.venv/bin/python -m benchmark.model_quality.download_data \
  --dataset kuairand --output benchmark/model_quality/data/raw/kuairand
.venv/bin/python -m benchmark.model_quality.prepare_kuairand \
  --raw benchmark/model_quality/data/raw/kuairand \
  --output benchmark/model_quality/data/kuairand

.venv/bin/python -m benchmark.model_quality.download_data \
  --dataset kuairec --output benchmark/model_quality/data/raw/kuairec
.venv/bin/python -m benchmark.model_quality.prepare_kuairec \
  --raw benchmark/model_quality/data/raw/kuairec \
  --output benchmark/model_quality/data/kuairec
```

Existing prepared paths can be selected with YAML `dataset.path`. The definition's
`name` becomes a generic prepared-manifest identity check. Retrieval manifests must
explicitly declare `candidate_scope`; the runner does not infer it from dataset
names or protocol names. Existing frozen runs are unaffected.

The downloader's `--dataset ID` selects `configs/datasets/<ID>.yaml`; it has no
dataset whitelist. `download_data --config FILE` loads an explicit dataset YAML.
Each preparation adapter also accepts `--config FILE`, so a custom definition can
be used consistently for download, preparation and the experiment's
`dataset.config`. Seeds, thresholds and split settings default to that YAML.
MovieLens preparation reads the flattened rating/item inputs extracted by the
downloader. For raw directories containing only an archive, rerun the downloader
to extract the configured inputs before preparation.

Adaptation remains three small layers: raw `prepare_*.py` modules own real data
semantics, `PreparedDatasetAdapter`/`FeatureView` own static input conversion, and
`ProtocolRecipe`/`recipes.py` implement public SQL candidate semantics declared by
the dataset YAML. Explicit experiment `features` can use an optional `source`
for aliases; training and requests share the same type conversion.
Labels and reserved audit identities cannot enter model inputs through aliases.

## Test runner

```bash
./benchmark/model_quality/run_tests.sh quality --profile smoke --run-id review_01
./benchmark/model_quality/run_tests.sh quality --profile full --output /path/to/results
```

Config/output arguments are relative to the invocation directory; dataset paths
are relative to the repository root. Default output is:

```text
runs/<run-id>/
  setup.json
  prepared_inputs.json
  logs/
  summary.json
  summary.md
  metrics.csv
  <config-stem>/
    resolved_config.json
    resolved_plan.json
    execution/
    results/report.json
    results/report.md
    results/metrics.csv
```

Run IDs default to a UTC timestamp plus process ID. The same suffix isolates SQL
object names and training storage paths. An explicit `storage_uri` also gains the
suffix when `--run-id` is supplied. Duplicate config stems and existing suite
paths are rejected. Interrupted runs are resumed individually through the CLI,
not by rerunning the suite. No existing upload directories are overwritten.

Options include `--config`, `--profile`, `--run-id`, `--output`, `--skip-upload`,
`--skip-provision-index`, and `--require-formal`.
Skipped uploads/index creation require data/empty collections at the *resolved*
locations, not an unrelated previous run. Failed independent configs are recorded
and the suite continues only after resource release is verified; uncertain writes are never automatically retried.
A failed local suite in `all` mode prevents any quality run from starting.

Before downloading, preflight opens SQLRec/Kyuubi sessions with `SELECT 1`, checks
SQLRec HTTP and Milvus connection/authentication, verifies Kubernetes read
permissions, and checks shared-filesystem access through the normal Hadoop client.
It does not deploy/upgrade services or create model resources. No-token Milvus
requests omit the Authorization header; authenticated deployments must provide
the credential environment variable. Backend write permissions are also exercised
by the subsequent normal upload/provision workflow.

`logs/` contains dependency, local-test, preflight, preparation and per-config logs.
`setup.json` records stages, interpreter/package versions, deployment checks and
data cache decisions. Failures before model execution still create JSON/Markdown
suite reports; later configs are marked not executed. Ctrl-C/TERM forwards to the
active child, performs owned-service cleanup, and finalizes the suite report.
Arguments/help validation can exit before a suite directory is allocated.

Execution is sequential by **model**, including models sharing one config, with
one fixed training seed:
`define → train → export → deploy → predict/save evidence → release → next model`.
Both DSSM towers are released. The runner's `finally` path and shell `EXIT/INT/TERM`
fallback use the same cleanup implementation, including failed or interrupted
runs. Cleanup does not depend on metrics or report generation. Batch service
retention is intentionally unavailable.

Public `DROP SERVICE` is asynchronous. A read-only Kubernetes barrier verifies
Deployment, Service, ReplicaSet and Pod removal before the next model starts.
Training/export Jobs must also be terminal with no active Pods. Frozen bindings,
initial collision checks, public model/checkpoint/URL checks and saved Kubernetes
UIDs prevent adopting another workload. Unproven operation outcomes, changed UIDs,
read failures or cleanup timeouts **stop subsequent scheduling**. The default
cleanup timeout is 120 seconds (`execution.cleanup_timeout` may override it).
Nothing cancels training/export Jobs or blindly repeats an uncertain DROP.

An interrupted SQL RPC is discarded; cleanup reconnects through a fresh public
session. If cancellation leaves training metadata at `created`, a terminal Job
with the UID captured at submission, plus no active Pods, can prove resource
release. A failed terminal checkpoint can also prove completion for cleanup.
Neither case changes the failed/interrupted training result into a success.

Models, checkpoints, collections, tables and local evidence remain. Reports use
saved predictions and can be generated after services are gone. Resume skips
completed, released models after validating artifact hashes. A partially scored
model whose service was released needs a new isolated run; uncertain creation
requires reconciling the original public operation. `--through` also releases
attempted services and is intended for investigating lifecycle stages, not for
retaining a live service. Legacy frozen v3/v4 plans keep their original stage
execution semantics; the suite fallback still verifies service release.
`cleanup.sql` is a manually reviewed full cleanup script.

Exit code 0 means the requested execution completed; it is not a quality threshold
pass. `--require-formal` additionally requires verified runtime conformance.
The suite summary preserves both report status and orchestration/cleanup failure
status, including `cleanup_failed` and later `not_executed` configurations. Completed
metric inputs from failed workflows are preserved in diagnostic reports.

## Manual workflow

Manual compact builds require the deployment environment; other actions use the
frozen plan and do not re-resolve machine defaults:

```bash
source deploy/env.sh
.venv/bin/python -m benchmark.model_quality.sql_workflow build \
  --config benchmark/model_quality/configs/movielens100k_recall.yaml \
  --run benchmark/model_quality/runs/manual_review \
  --profile smoke --run-id manual_review
.venv/bin/python -m benchmark.model_quality.sql_workflow upload-data --run YOUR_RUN
.venv/bin/python -m benchmark.model_quality.sql_workflow provision-index --run YOUR_RUN
.venv/bin/python -m benchmark.model_quality.sql_workflow run --run YOUR_RUN
.venv/bin/python -m benchmark.model_quality.sql_workflow evaluate --run YOUR_RUN
.venv/bin/python -m benchmark.model_quality.sql_workflow cleanup-services --run YOUR_RUN
```

Uploads use the normal Hadoop client. Kyuubi creates external Hive tables exposing
only the frozen training partition; actual public table counts must match.
DSSM trains on the positive table. Milvus provisioning creates isolated schema and
FLAT/IP indexes only; embedding insertion remains public SQL. Scalar-only runs
make index provisioning a no-op, allowing the same lifecycle command sequence.

`run --through STEP_ID` stops with a partial journal. `check` verifies the frozen
plan and files. `evaluate --diagnostic` writes separate non-formal reports from
complete metric inputs only. `audit-runtime --model SQL_MODEL --artifact PATH`
inspects an unmodified native pipeline. `summarize --inputs RUN_A RUN_B --run SUITE_DIR`
writes the same combined report as the shell runner.

## Artifacts and reports

The plan freezes config, SQL, request paths, input hashes and replay rules. Format
v5 also freezes model-level service/Job bindings, table schemas, connector options
and warehouse partition locations. Compact hashed model names reserve space for
native export Job/headless-Service suffixes and prevent Kubernetes truncation
collisions. Old v3/v4 plans remain readable; v3 is adapted only in memory.

`data_contract.json` keeps labels and audit slice metadata outside prediction inputs.
`sql/` and `requests/` are the actual public statements and label-free payloads.
`infra/` holds index specs/evidence; `execution/` holds the journal, operation IDs,
raw returns and audits. `results/` contains metrics and collected predictions.
Service release SQL is derived from frozen bindings instead of a duplicate SQL
file. `execution/services_cleanup.json` records DROP receipts, owned UIDs and
absence proofs; `execution/journal.json` records per-model completion and release
status. Report `resource_status` is separate from prediction completeness.

Reports distinguish `workflow_status`, `parameters_status`, and
`quality_status=not_assessed`. `formal=true` means the public workflow and supported
runtime parameter checks passed and physical resource release was verified. It does
not mean the model meets a quality target. Legacy plans without release evidence
remain readable but cannot claim verified resource release.
Missing evidence, unsupported parameter checks and runtime mismatches prevent a
formal pass. Reports include dataset, feature view, profile, tasks, sample counts,
candidate protocol, categorical capacity, and per-model results with the actual seed.

Paired DSSM AUC/AP/GAUC uses actual SQL `ip` scores, without probability conversion.
Probability outputs also report LogLoss/Brier; regression supports RMSE/MAE.
GAUC coverage is reported when single-class users cannot contribute. Recall,
micro Recall and NDCG use returned order and preserve all held-out positives.
Missing responses, empty lists and zero-positive queries remain distinct.

KuaiRand reports `source` and `tab` slices for scalar and retrieval metrics. Retrieval
slice truth is formed from the corresponding exposure subset, while already
collected public query responses are reused. MMoE reports each task separately.
JSON/CSV preserve diagnostics; `evaluation.metrics` controls Markdown display.

Full-catalog retrieval uses a fixed candidate limit and public SQL seen filtering,
so truncation/filtering loss remains part of measured quality. Observed-only
retrieval first requests the entire eligible catalog, then joins a label-free
`allowed` input table in public SQL and applies seen filtering/sorting/LIMIT.
Build rejects a candidate limit smaller than the catalog or above Milvus's 16,384
limit. Larger observed-candidate catalogs need another public recipe; they are not
silently approximated. Returned membership is checked against the frozen pool.
Observed-candidate Recall and full-catalog Recall must not be compared as if they
had the same denominator or difficulty.

The default retrieval candidate budget is 16384, capped to the frozen catalog
size (and at least max K). For full-catalog recall, the required budget is
`min(catalog size, max K + maximum seen items in the catalog)` so filtering seen
items does not shorten an otherwise complete top-K. Observed-only protocols must
retrieve the whole catalog before public SQL membership filtering; catalogs over
16384 require another declared retrieval protocol. An explicitly smaller budget
is reported as `candidate_budget.candidate_limited`; it is rejected when
`allow_shortlist=false`. This diagnostic is retained in `data_contract.json` and
the report and does not change target denominators.

## Reproduction and recovery

Normal Beeline/JDBC clients can execute frozen SQL in dependency order. A function
must compile from CREATE through RETURN in one session; request tables and CALL
also share a session. SQL API payloads are stored as JSON. SQLRec uses the public
Flink/default dialect. Python does not privately load weights, fit models, rank
items, inject samplers, or rewrite native training configs.

TRAIN/EXPORT require public succeeded checkpoint/type proof. Services require exact
model/checkpoint binding and readiness. After disconnection, non-idempotent stages
are reconciled using public state and never blindly resubmitted. Function groups
can replay from CREATE on a fresh session. Vector INSERT receipts and embeddings
are separated by result roles; completed inserts are not repeated.

`run --retry-failed-create STEP_ID` is an operator-selected retry for a definitively
failed CREATE TABLE only; it does not retry uncertain writes, TRAIN or EXPORT.
Existing Connector schema/options and external-table schema/location must match
the frozen contract. CREATE and warehouse partition registration are separate
stages; recovery cannot mistake a partially registered table for completed data
access. Both SQLRec `name/type` and warehouse `col_name/data_type` DESCRIBE layouts
are supported. Label SQL types come from the dataset's `columns` declaration and
are preserved in Parquet and model definitions. Initial execution also checks
public object and Kubernetes resource name collisions.

## Runtime parameter audit

Audits read normal native pipeline configs and capture submitted Kubernetes Job /
ConfigMap evidence through read-only commands. Submitted configuration takes
precedence over filesystem artifacts, before export can change them. Polling uses
one bounded Job/ConfigMap budget. No alternative trainer or config patch is used.

TZRec checks declared feature bindings, task labels, applicable network structure,
explicit batch/workers/epochs, buckets/embedding sizes, image and training seed
environment. MMoE checks task towers and loss types; Rocket Launching checks its
booster/light/shared widths. GBDT checks JSON model type, features/categoricals,
labels, explicit parameters, image and pipeline random seed. CatBoost-specific
parameter overrides follow the public generator's precedence and are recorded.
Unsupported additional parameters remain unverified. Missing Job evidence cannot
verify the expected training image, even with a manually supplied pipeline.

## Code structure and extension points

| Module | Responsibility |
| --- | --- |
| `sql_workflow.py` | Preflight/data setup, public lifecycle CLI and suite summaries |
| `bootstrap.py` | Standard-library Python dependency setup, stage records and early-failure reports |
| `user_workflow/data_setup.py` | Deployment preflight, declared preparation adapters and verified versioned caches |
| `run_tests.sh`, `configs/` | Fixed sequential suite and small experiment definitions |
| `user_workflow/configuration.py` | Generic dataset YAML loading and environment/default expansion |
| `configs/datasets/*.yaml` | One definition per dataset: sources, preparation, columns, tasks, views and protocol |
| `download_data.py`, `prepare.py`, `prepare_*.py` | Pinned download/extraction, configured preparation algorithms and raw-format adapters |
| `user_workflow/specs.py` | Validation and feature/task/model/protocol/plan contracts |
| `configs/datasets/` | Dataset-specific fields, labels, views, protocols and evaluation settings |
| `user_workflow/datasets.py` | Generic prepared adapter, static feature views, frozen cohorts and requests |
| `user_workflow/recipes.py` | Public SQL generation including candidate recipes |
| `user_workflow/sql_client.py`, `infrastructure.py` | Public SQL/API clients and index administration |
| `user_workflow/runner.py`, `runtime_audit.py` | Execution/recovery, runtime evidence and owned-service release |
| `metrics.py`, `user_workflow/results.py` | Metric mathematics, coverage and JSON/CSV/Markdown reports |
| `common.py`, `tests/` | Shared file/identity contracts and local tests |

New datasets declare shared dataset YAML against the prepared contract; different feature/candidate
semantics require publicly reproducible recipes. Models declare real public
capabilities and outputs; metrics consume collected returns. External datasets,
automatic selection/test unlocking, integrated bootstrap intervals, ANN performance
matrices, streaming execution and private model lifecycles remain out of scope.
The suite report is a review aid, not a cross-dataset quality leaderboard.

## Validation

On 2026-10-07, `bash benchmark/model_quality/run_tests.sh all` completed with exit
code 0: all 95 framework regression tests and all 10 configurations (22
dataset/model pairs) passed. Every workflow report verified runtime parameters and
resource release; all models used the single seed 123. The automatic environment
checks and prepared-data cache reuse also passed.

The Milvus connector's 102 Java regression tests passed, including bounded SQL bulk
writes. The normal TZRec image build and verification passed, including native
health probes with an idle persistent prediction connection, real training/export,
and comparisons between the C++ and Python serving backends. The old native image
reproduced the health-probe timeout covered by the new regression.

This validation used the default smoke profile. Full-data training and evaluation
were not run, and no model-quality acceptance thresholds were configured
(`quality_status: not_assessed`). Metrics describe the integration cohort. Earlier
failed attempts remain in ignored run directories as diagnostic evidence; each new
suite writes its own logs, reports and resource proofs under `runs/<run-id>/`.
