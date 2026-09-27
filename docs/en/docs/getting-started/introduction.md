# SQLRec

## Introduction

SQLRec is an engine for writing recommendation flows in SQL. Data analysts, data engineers, and backend developers can use SQL to connect data sources, organize recall and ranking, and publish results as APIs.

### Where to Start

- To see recommendations first, follow the [Docker Quick Start](./docker.md) to run the demo and call its built-in function and API.
- To change recommendation logic, read [Writing a Recommendation Flow](../guides/recommendation-flow.md), then review the [demo SQL files](./docker.md#modify-the-demo-sql) and how SQLRec loads them.
- To train models or deploy the full service, check the requirements in [Service Deployment](../operations/deployment.md), then read [Model Training and Online Inference](../guides/model-lifecycle.md).

The diagram below shows the main components. You do not need to understand all of them before trying the demo.

![system_architecture](/sqlrec_arch.svg)

SQLRec has the following features:
- Cloud native, with built-in minikube-based deployment scripts for one-click deployment of SQLRec system and related dependency services
- Extended SQL syntax, making it possible to describe recommendation system business logic using SQL
- Implemented an efficient SQL execution engine based on Calcite, meeting the real-time requirements of recommendation systems
- Based on existing big data ecosystem, easy to integrate
- Easy to extend, supporting custom UDFs, Table types, and Model types

## Roadmap

### When will version 1.0 be released?

Versions before 1.0 are beta releases. They are not recommended for production use and do not guarantee interface compatibility. There is no scheduled date for 1.0. The following work is planned before release; completed items are crossed out:

- Comprehensive unit test, integration test, and effectiveness test coverage
- Code quality optimization, many details still need refinement
- ~~Support for fallback and timeout configuration~~ (see [Timeouts and Recovery](../guides/exception-recovery.md))
- ~~Version management with rollback to previous versions~~ (using filesystem schemas and Docker image versions)
- ~~Metric monitoring system~~ (the `/metrics` endpoint and Prometheus/Grafana deployment configuration are available)
- ~~C++ model serving~~ (available for LightGBM, XGBoost, and CatBoost models)

### Future Feature Plans

- Further enhance frontend UI with more management and monitoring features
- Further optimize SQL syntax compatibility and runtime performance
- More ready-to-use UDFs, models, etc.
- Support for more external data sources, such as Elasticsearch, etc.
- Tensorboard visualization of model training process
- GPU training and inference support
- Support for authentication and authorization
- Best practice tutorials, including search, recommendation, etc.
