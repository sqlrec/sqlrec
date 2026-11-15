# Development and Testing

This page is for source and extension developers. See [Service Deployment](../operations/deployment.md) to deploy a service and [Client Connections](../guides/clients.md) for everyday SQL use. Run commands from the repository root.

## Build and Run Locally

Build the service and required modules:

```bash
mvn package -pl sqlrec-frontend -am -DskipTests
SQL_SCHEMA_DIR="$PWD/sqlrec-demo/src/main/sql" bash bin/cli.sh
```

SQL-file mode does not require HMS or Minikube. Queries still need the relevant Connector and UDF JARs. CLI logs default to `sqlrec-frontend/target/logs`.

For remote metadata, configure `HIVE_METASTORE_URI`, `META_DB_URL`, and database credentials. Without configured endpoints, the source CLI tries development defaults from `DEFAULT_TEST_IP`, `NODE_IP`, or Minikube discovery. Explicit settings, including empty strings, are preserved.

## Test an Existing Environment

```bash
bash bin/test.sh -pl sqlrec-core -am
```

The script runs Maven integration tests and passes other arguments to Maven unchanged. Addresses prefer `DEFAULT_TEST_IP`, then `NODE_IP`, then Minikube discovery. Override individual component endpoints or `HADOOP_CONF_DIR` as needed. Maven uses the current shell's JDK; Hive/Hadoop clients use their configured Java environment.

## Rebuild and Run All Tests

```bash
bash bin/test.sh -all
```

`--all` and `-all` are equivalent. This mode needs an accessible existing Minikube environment to save an image cache, then **deletes and rebuilds the cluster named `minikube`, including its node-local dynamic volume data**. Host-side image caches and client directories remain; failed image caching prevents cluster deletion.

Stages include dependency deployment, readiness checks, Maven integration tests, and SQL performance, GrowthBook, Hugging Face, and model quality benchmarks. Any failed stage stops the run. The script uses configured release images without building local source images; build and select your images before testing source changes. The run requires network access, deployment permissions, and sufficient compute and storage.

## Read Reports

The full run writes its report and stage logs under `benchmark/runs/` and prints the run directory on completion. Failed runs also produce reports. Override the parent directory with `TEST_REPORT_ROOT`.

- `summary.md`: stage status, test results, and benchmark results.
- `logs/`: command output for each stage.
- `growthbook/`, `huggingface/`, `model_quality/`: detailed benchmark artifacts.

See individual benchmark READMEs for standalone options:

- [SQL performance](https://github.com/sqlrec/sqlrec/blob/master/benchmark/sql_performance/README.md)
- [GrowthBook](https://github.com/sqlrec/sqlrec/blob/master/benchmark/growthbook/README.md)
- [Hugging Face](https://github.com/sqlrec/sqlrec/blob/master/benchmark/huggingface/README.md)
- [Model quality](https://github.com/sqlrec/sqlrec/blob/master/benchmark/model_quality/README.md)

## Preview and Check Documentation

```bash
npm ci
npm run docs:dev
npm run docs:build
```

Documentation sources are in `docs/docs/` and `docs/en/docs/`. When moving pages, update both languages and `docs/.vitepress/config.mts`. Do not edit generated `.vitepress/dist` or cache files.
