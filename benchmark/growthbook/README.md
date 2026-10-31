# GrowthBook UDF integration benchmark

Deploy GrowthBook and its MongoDB through the repository's Kubernetes scripts,
create real A/B experiments through GrowthBook's APIs, and verify
`GetGrowthbookFeaturesFunction` through SQLRec's public SQL/HTTP APIs.
The workflow targets the configured `growthbook/growthbook:4.4.0` deployment.
It does not seed MongoDB directly or serve a mocked SDK feature endpoint.

## Run

From the repository root, with a running Kubernetes cluster and SQLRec server:

```bash
bash benchmark/growthbook/test.sh
```

This deploys GrowthBook, waits for its API, installs the shared Python client
dependencies in `.venv`, provisions experiments and features, verifies the UDF,
and cleans up this run's resources. Python 3.11+ is required. Reports are saved
under `benchmark/growthbook/runs/<timestamp>_<run-id>/` and ignored by Git.

For an existing deployment with dependencies already installed:

```bash
source deploy/env.sh
.venv/bin/python -m benchmark.growthbook
```

`bin/test.sh -all` deploys GrowthBook before Maven tests and runs this benchmark
after the SQL performance benchmark, before the Hugging Face and model quality
benchmarks. The normal component deployment still treats GrowthBook as optional.

## Bootstrap and configuration

An empty GrowthBook installation is initialized through `/auth/firsttime` with
a dedicated benchmark administrator and organization. Later runs log in through
`/auth/login`. The development defaults are:

- `GROWTHBOOK_BENCH_EMAIL=sqlrec-benchmark@example.invalid`
- `GROWTHBOOK_BENCH_PASSWORD=SqlrecBenchmark123!`

For an existing installation, supply administrator credentials with these
environment variables. Set `GROWTHBOOK_BENCH_ORGANIZATION` when the account
belongs to more than one organization. The runner creates a temporary API key
through `/keys` for public resource APIs and revokes it during cleanup. Passwords,
session tokens and management API keys are not written to reports.

| Variable | Default / purpose |
| --- | --- |
| `NODE_IP` | Discovered by `deploy/env.sh`; SQLRec and GrowthBook NodePort address |
| `NAMESPACE` | Deployment namespace, normally `sqlrec` |
| `GROWTHBOOK_VERSION` | Deployment image tag, normally `4.4.0` |
| `GROWTHBOOK_API_PORT` | `30032` |
| `GROWTHBOOK_BENCH_API_HOST` | Override the GrowthBook root API origin; omit `/api` |
| `GROWTHBOOK_BENCH_SDK_HOST` | Origin reachable by the SQLRec server; defaults to the API origin |
| `GROWTHBOOK_BENCH_ENVIRONMENT` | `production`; must exist in the selected organization |
| `AUTH_ENABLED`, `AUTH_USERNAME`, `AUTH_PASSWORD` | SQLRec authentication from `deploy/env.sh` |
| `DEPLOY_TIMEOUT` | Deployment readiness timeout |
| `GROWTHBOOK_BENCH_TIMEOUT` | API readiness / SDK publication timeout in seconds, default `120` |

Each run creates its own project, three running experiments, four features and
one Java SDK connection. The feature flags use `experiment-ref` rules referencing
the actual experiment and variation IDs returned by the API. No datasource is
needed: these checks evaluate assignment and UDF behavior rather than statistical
experiment analysis. The SDK payload must contain the published experiment
definitions before the Java client is initialized.

## Correctness checks

- A 50/50 experiment checks deterministic hash-v2 assignment to both variants.
- A targeted experiment checks country, numeric age and boolean attributes,
  including NULL values, with 100% weight on the second variation.
- A 50% coverage experiment checks both variants and users excluded from traffic.
  Fixtures deliberately cover all three classes without probabilistic thresholds.
- Compare every tracking row's `experiment_id`, numeric-index `variation_id`, and
  `user_id` (the raw attribute value used for hashing), plus feature execution
  variables read by SQL `get()`.
- A plain default feature and an unknown feature must not write variables or
  produce experiment tracking rows: the current UDF exposes experiment results only.
- Check repeated and concurrent requests, including different attributes for the
  same user ID, and reject zero-row and multiple-row user tables.
- A valid request after invalid input must still return the expected result.

The reference implements the documented FNV-1a/hash-v2 algorithm independently
of the Java SDK. Fixtures use ASCII IDs/seeds and string feature values. SQL
reads the tracking table before reading variables and returns a separate state
row even when the tracking table is empty.

On success, failure or Ctrl-C, cleanup attempts to remove the run's SQL API and
function, SDK connection, features (archived before deletion), experiments,
project and temporary API key.
The GrowthBook/MongoDB deployment and bootstrap administrator remain reusable.
Cleanup errors make the run fail and are recorded in the report. Writes are
never automatically retried if their response is lost; inspect the unique run
prefix and `resources.json` after an uncertain write or forced termination.

`summary.md` and `results.json` record status, request timing and cleanup results.
`sdk-payload.json`, `statements.sql`, `config.json`, `predictions.json` and
`resources.json` preserve the published configuration and verification evidence.
The request timings include correctness fixtures and initial SDK initialization;
they are not a production throughput measurement.

Local contract tests do not deploy or contact services:

```bash
.venv/bin/python -m unittest discover -s benchmark/growthbook/tests -v
```

## SDK client isolation

The UDF uses GrowthBook Java SDK 0.10.10's feature repository directly. Each
API host / SDK key has its own repository, with an atomically published feature
snapshot and a separate evaluation context for every request. This avoids the
multi-user facade's static repository and isolates concurrent user attributes.
The bounded cache expires idle clients and stops their background polling.
Disk caching is disabled because the SDK's default cache file is shared across
keys. A fresh client must receive a feature payload successfully before use.

Run this benchmark twice against the same SQLRec process to verify that new SDK
keys can initialize after the previous run's resources have been removed.

The runner sends `IGNORE_UNION_EXCEPTION=false` so UDF errors fail the request
instead of being recovered as an empty tracking table. A failed run still
cleans up its GrowthBook resources and SQL function/API.

API behavior was checked against the
[GrowthBook 4.4.0 source](https://github.com/growthbook/growthbook/tree/v4.4.0/packages/back-end/src/api),
the [API introduction](https://docs.growthbook.io/api/introduction), and the
[SDK hashing specification](https://docs.growthbook.io/lib/build-your-own#hashseed-string-value-string-version-integer-floatnull).
