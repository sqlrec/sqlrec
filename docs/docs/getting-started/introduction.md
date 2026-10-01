# SQLRec

SQLRec 是一个用 SQL 编写推荐流程的引擎。你可以连接数据源、编排召回与排序、调用模型服务，并将结果发布为 HTTP API。

## 项目优势

- **开发简单**：用 SQL 编写召回、排序等推荐逻辑，并发布为 API。
- **面向在线推荐**：基于 Calcite 执行 SQL，支持缓存、并发和超时降级。
- **复用大数据生态**：可直接使用 HMS 中的表和 HDFS 上的数据。
- **统一模型管理**：通过 SQL 训练、部署和调用模型，也可接入已有模型服务。
- **云原生**：基于 Kubernetes 管理训练和推理，提供部署脚本。
- **易于扩展**：支持自定义函数、数据源和模型后端。
- **便于排障**：通过 UI、指标和 Trace 查看流程、定位问题。

## 从哪里开始

| 你要做什么 | 从这里开始 |
| --- | --- |
| 先看到推荐结果 | [Docker 快速开始](./docker.md) |
| 修改推荐逻辑 | [编写推荐流程](../guides/recommendation-flow.md) |
| 连接自己的数据 | [接入数据源](../guides/data-sources.md) |
| 接入已有模型服务或训练模型 | [模型训练与在线推理](../guides/model-lifecycle.md) |
| 部署完整开发环境 | [服务部署](../operations/deployment.md) |

Docker Demo 内置示例数据和流程，仅用于体验功能，无需外部服务。实际业务接入需要自行定义连接业务数据的表、SQL 推荐流程和 API。业务定义可以使用本地 SQL 文件管理；需要动态管理定义、训练模型或使用 Flink 时，再按需准备相应服务。组件和执行机制见[架构设计](../reference/architecture.md)。

## 当前状态

当前版本为 beta，不建议用于生产环境，也不保证接口兼容性。1.0 尚无确定发布时间。

已有功能包括 SQL 推荐流程、HTTP API、UI、Redis/JDBC/MongoDB/Milvus/Kafka 等数据源、模型训练与推理、超时降级，以及指标和 Trace。

## 后续计划

- 完善单元、集成和效果测试，继续优化 SQL 兼容性及运行性能。
- 增加更完整的模型与服务版本管理和回滚能力。
- 完善 UI 管理、监控和模型训练可视化。
- 增加 UDF、模型和数据源适配。
- 扩展训练及其他模型后端的 GPU 能力；Hugging Face GPU 推理已有[配置说明](../reference/models/builtin-models.md#hugging-face-服务配置)。
- 支持认证、鉴权，并补充搜索、推荐等实践教程。
