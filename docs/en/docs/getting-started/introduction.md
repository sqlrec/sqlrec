# SQLRec

SQLRec is an engine for recommendation flows written in SQL. Connect data sources, organize recall and ranking, call model services, and publish results as HTTP APIs.

## Why SQLRec

- **Develop in SQL:** Write recall, ranking, and other recommendation logic, then publish it as an API.
- **Execute online flows:** Run SQL with Calcite, with caching, parallel calls, timeouts, and fallbacks.
- **Reuse your big data ecosystem:** Use existing HMS tables and data on HDFS directly.
- **Manage models through SQL:** Train, deploy, and call models, or connect existing model services.
- **Deploy on Kubernetes:** Manage training and inference with Kubernetes and provided deployment scripts.
- **Extend as needed:** Add custom functions, data sources, and model backends.
- **Troubleshoot easily:** Inspect flows and diagnose issues with the UI, metrics, and traces.

## Where to Start

| Your task | Start here |
| --- | --- |
| See recommendation results | [Docker Quick Start](./docker.md) |
| Change recommendation logic | [Write a Recommendation Flow](../guides/recommendation-flow.md) |
| Connect your own data | [Connect Data Sources](../guides/data-sources.md) |
| Connect an existing model service or train a model | [Model Training and Online Inference](../guides/model-lifecycle.md) |
| Deploy a complete development environment | [Service Deployment](../operations/deployment.md) |

The Docker demo includes sample data and a flow solely for trying the features, with no external services required. To integrate SQLRec into your application, define your own tables connected to business data, SQL recommendation flows, and APIs. Manage business definitions as local SQL files; prepare additional services when you need mutable shared definitions, model training, or Flink. See [Architecture](../reference/architecture.md) for components and execution details.

## Current Status

The current release is beta. Production use is not recommended, and interface compatibility is not guaranteed. There is no scheduled date for 1.0.

Available features include SQL recommendation flows, HTTP APIs, a UI, Redis/JDBC/MongoDB/Milvus/Kafka data sources, model training and inference, timeout recovery, metrics, and tracing.

## Future Plans

- Improve unit, integration, and effectiveness testing, SQL compatibility, and runtime performance.
- Add more complete model and service version management and rollback.
- Improve UI management, monitoring, and training visualization.
- Add UDFs, models, and data-source adapters.
- Extend GPU support for training and other model backends; Hugging Face GPU inference already has [configuration options](../reference/models/builtin-models.md#hugging-face-service-options).
- Add authentication, authorization, and search and recommendation tutorials.
