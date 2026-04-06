# SQLRec

## Introduction

A recommendation engine that supports SQL development, aiming to enable data science practitioners, including data analysts, data engineers, and backend developers, to quickly build production-ready recommendation systems. The system architecture is shown in the figure below. SQLRec encapsulates underlying component access, model training, inference, and other processes using SQL, allowing upper-level recommendation business logic to be described using only SQL.

![system_architecture](/sqlrec_arch.png)

SQLRec has the following features:
- Cloud native, with built-in minikube-based deployment scripts for one-click deployment of SQLRec system and related dependency services
- Extended SQL syntax, making it possible to describe recommendation system business logic using SQL
- Implemented an efficient SQL execution engine based on Calcite, meeting the real-time requirements of recommendation systems
- Based on existing big data ecosystem, easy to integrate
- Easy to extend, supporting custom UDFs, Table types, and Model types

## Roadmap

### When will version 1.0 be released?

Versions before 1.0 are beta versions, not recommended for production use, and do not guarantee interface compatibility. There is no planned release date yet; it will be released after the following features are completed:

- Comprehensive unit test and integration test coverage
- Complete version management methods for easy rollback to previous versions
- Optimize code quality, many details still need refinement
- Improve metric monitoring system
- C++ model serving
- Validate model training effectiveness on public datasets

### Future Feature Plans

- Frontend UI for viewing current execution DAG, SQL code, statistics, etc.
- Further optimize SQL syntax compatibility and runtime performance
- More ready-to-use UDFs, models, etc.
- Support for more external data sources, such as JDBC, MongoDB, etc.
- Tensorboard visualization of model training process
- GPU training and inference support
- Best practice tutorials, including search, recommendation, etc.
