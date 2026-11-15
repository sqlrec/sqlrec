# SQLRec

SQLRec is an engine for recommendation flows written in SQL. Connect data sources, organize recall and ranking, call model services, and publish results as HTTP APIs.

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

HTTP and JDBC support account authentication. All authenticated users currently have the same permissions; role-based or per-resource authorization is not available. See [Entry Authentication](../operations/deployment.md#entry-authentication).
