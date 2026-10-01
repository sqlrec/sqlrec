<h1 align="center">SQLRec</h1>

<p align="center">
  English | <a href="README_zh.md">中文</a>
</p>

<p align="center">
  <a href="https://github.com/sqlrec/sqlrec/blob/main/LICENSE">
    <img src="https://img.shields.io/github/license/sqlrec/sqlrec" alt="License">
  </a>
  <a href="https://github.com/sqlrec/sqlrec/stargazers">
    <img src="https://img.shields.io/github/stars/sqlrec/sqlrec" alt="Stars">
  </a>
  <a href="https://github.com/sqlrec/sqlrec/network/members">
    <img src="https://img.shields.io/github/forks/sqlrec/sqlrec" alt="Forks">
  </a>
  <a href="https://github.com/sqlrec/sqlrec/commits">
    <img src="https://img.shields.io/github/last-commit/sqlrec/sqlrec" alt="Last Commit">
  </a>
</p>

## About SQLRec

SQLRec is an engine for building recommendation systems in SQL. Developers familiar with SQL can write recall, deduplication, ranking, and diversification logic, then publish the flow as an HTTP API. The engine handles data-source access, model training, and inference so users can focus on recommendation logic.

The current release is beta. Production use is not recommended, and interface compatibility is not guaranteed.

## Why SQLRec

- **Develop in SQL:** Write recall, ranking, and other recommendation logic, then publish it as an API.
- **Execute online flows:** Run SQL with Calcite, with caching, parallel calls, timeouts, and fallbacks.
- **Reuse your big data ecosystem:** Use existing HMS tables and data on HDFS directly.
- **Manage models through SQL:** Train, deploy, and call models, or connect existing model services.
- **Deploy on Kubernetes:** Manage training and inference with Kubernetes and provided deployment scripts.
- **Extend as needed:** Add custom functions, data sources, and model backends.
- **Troubleshoot easily:** Inspect flows and diagnose issues with the UI, metrics, and traces.

## System Architecture

![SQLRec system architecture](docs/public/sqlrec_arch.svg)

See [Architecture](https://sqlrec.github.io/sqlrec/en/docs/reference/architecture) for component responsibilities and execution details.

## Try It

The Docker demo includes sample data, a recommendation flow, and an API solely for trying the features. It requires no external services. To integrate SQLRec into your application, define your own tables connected to business data, SQL recommendation flows, and APIs, then configure model services and deployment as needed.

Start the demo with Docker:

```bash
docker run --rm -d --name sqlrec-demo \
  -p 30000:30000 \
  -p 30001:30001 \
  sqlrec/sqlrec-demo:latest
```

Call the built-in recommendation API:

```bash
curl -X POST http://localhost:30001/api/v1/demo_rec \
  -H "Content-Type: application/json" \
  -d '{"data":{"user_info":[{"user_id":1000001}]}}'
```

The first call usually returns two items with fields such as `item_id` and `rec_reason`. Use user IDs `1000001` through `1000005`. Repeated calls exclude previously recommended items; restart the container when the candidates are exhausted.

Open the [SQLRec UI](http://localhost:30001/ui/static/index.html) to inspect tables, functions, APIs, and execution DAGs.

### Optional: Call It with SQL

```bash
docker exec -it sqlrec-demo /app/cli.sh
```

```sql
cache table quick_start_user as
select cast(1000001 as bigint) as user_id;

call demo_rec(quick_start_user);
```

End each statement with a semicolon and press `Ctrl+D` to exit. The CLI and HTTP service keep separate in-memory data; CLI changes do not affect the API.

Stop the container when finished; `--rm` removes it automatically:

```bash
docker stop sqlrec-demo
```

## Build Your Own Recommendation Flow

- [Connect Data Sources](https://sqlrec.github.io/sqlrec/en/docs/guides/data-sources): define tables connected to your business stores.
- [Write a Recommendation Flow](https://sqlrec.github.io/sqlrec/en/docs/guides/recommendation-flow): define inputs and SQL functions to compose your business logic.
- [Publish and Call an API](https://sqlrec.github.io/sqlrec/en/docs/guides/api): publish your recommendation function for application calls.
- [Model Training and Online Inference](https://sqlrec.github.io/sqlrec/en/docs/guides/model-lifecycle): connect an existing model service or train a model as needed.
- [Service Deployment](https://sqlrec.github.io/sqlrec/en/docs/operations/deployment): prepare the runtime environment. See the [Docker Quick Start](https://sqlrec.github.io/sqlrec/en/docs/getting-started/docker#managing-local-sql-definitions) for an example of loading custom SQL locally.

See the [SQLRec User Manual](https://sqlrec.github.io/sqlrec/en/) for more.
