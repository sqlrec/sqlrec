# SQLRec

## 简介

SQLRec 是一个用 SQL 编写推荐流程的引擎。数据分析师、数据工程师和后端开发者可以用 SQL 连接数据源、编排召回与排序，并将结果发布为 API。

### 从哪里开始

- 想先看到推荐结果：按 [Docker 快速开始](./docker.md)启动 Demo，调用内置函数和 API。
- 想修改推荐逻辑：阅读[编写 SQL 推荐流程](../guides/recommendation-flow.md)，再查看快速开始中的 [Demo SQL 目录](./docker.md#修改-demo-sql)和加载方法。
- 想训练模型或部署完整服务：先查看[服务部署](../operations/deployment.md)的环境要求，再阅读[模型训练与在线推理](../guides/model-lifecycle.md)。

下图展示主要组件；第一次使用时无需先了解全部组件。

![system_architecture](/sqlrec_arch.svg)

SQLRec 有以下特点：
- 云原生，自带基于 minikube 的部署脚本，可以一键部署 SQLRec 系统和相关的依赖服务
- 扩展了 SQL 语法，让使用 SQL 描述推荐系统业务逻辑变得可能
- 基于 Calcite 实现了一个高效的 SQL 执行引擎，可以满足推荐系统的实时性要求
- 基于已有的大数据生态，接入简单
- 易于扩展，可以自定义 UDF、Table 类型、Model 类型

## 路线图

### 1.0 版本什么时候发布

1.0 之前的版本均为 beta 版本，不建议用于生产环境，也不保证接口兼容性。目前没有确定的 1.0 发布时间。以下是发布前计划完善的工作；已完成的条目用删除线标示：

- 完善的单元测试、集成测试、效果测试覆盖
- 优化代码质量，目前仍很多细节要打磨
- ~~支持降级和超时配置~~（用法见[异常处理与恢复](../guides/exception-recovery.md)）
- ~~完善的版本管理方法，可以方便回滚到之前的版本~~（使用文件系统 Schema 和 Docker 镜像管理版本）
- ~~Metric 监控系统完善~~（已提供 `/metrics` 接口及 Prometheus/Grafana 部署配置）
- ~~C++ 模型 Serving~~（已支持 LightGBM、XGBoost 和 CatBoost 模型）

### 后续功能规划

- 进一步完善前端 UI，增加更多管理和监控功能
- 进一步优化 SQL 语法兼容性、运行性能
- 更多开箱可用的 UDF、模型等
- 支持更多的外部数据源，比如 Elasticsearch等
- Tensorboard 可视化模型训练过程
- GPU 训练、推理支持
- 支持认证、鉴权
- 最佳实践教程，包括搜索、推荐等
