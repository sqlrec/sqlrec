# Full test report

Run from the repository root:

```bash
bash bin/test.sh -all
# Maven arguments are still passed through unchanged.
bash bin/test.sh --all -pl sqlrec-core -am
```

The command retains its existing test scope and execution order: cache images,
recreate Minikube, deploy dependencies, run Maven tests, then run SQL performance,
GrowthBook, Hugging Face and full model quality benchmarks. It stops at the first
failure. It does not add the separate local Python contract tests or change the
Maven lifecycle from `test` to `verify`.

Each invocation creates `benchmark/runs/<timestamp>_<id>/`. The terminal prints
the report path at startup and on exit. Set `TEST_REPORT_ROOT` to change the
parent directory:

```bash
TEST_REPORT_ROOT=/tmp/sqlrec-test-reports bash bin/test.sh -all
```

Open **`summary.md`** for the complete report. It contains:

- Overall status, run parameters, timestamps and elapsed time.
- Every deployment/test stage, its exit code and log link. Failed stages include
  the last 40 log lines directly in the report. Unexecuted stages state why they
  did not run.
- Maven suite counts and all failed test names, messages and stack traces.
  Only XML files created or updated during this invocation are copied into
  `maven/`; old reports are excluded. No new XML is reported as N/A, including
  when compilation fails or the supplied Maven arguments skip tests.
- The formal SQL performance measurement: concurrency, duration, requests/sec,
  mean latency, P50/P75/P90/P99, HTTP errors and socket errors. Warm-up metrics
  are excluded. Throughput includes error responses, following wrk's definition.
- All GrowthBook checks and cleanup results, every Hugging Face case, and all
  model quality rows (29 task rows for the full 28-combination matrix), including
  coverage, timings and cleanup errors. Unavailable measurements are N/A.

`results.json` contains the same run's structured stage and benchmark results.
Raw stage logs live in `logs/`; each Python benchmark's original artifacts live
in `growthbook/`, `huggingface/`, or `model_quality/`. No historical run directory
is searched to select results. The benchmark `--run-dir` option selects an exact
new directory for this integration; standalone defaults and `--output` remain
available.

Reports are updated after each stage and finalized on success, failure, SIGINT
or SIGTERM. Interruptions preserve partial results and mark the active stage as
interrupted. The original nonzero command exit code is preserved. Ignored Maven
test failures, failed benchmark cases or missing/corrupt benchmark reports also
prevent an overall PASSED result, even if a command exits zero. A forced kill or
power loss leaves the last saved report, without finalization.

SQL performance status follows the existing command exit code; this reporting
change does not introduce an error-rate or performance threshold. Model quality
COMPLETED means execution finished, not that a quality threshold was met.
