# SQLRec

SQLRec 是一个用 SQL 编写推荐流程的引擎。你可以连接数据源、编排召回与排序、调用模型服务，并将结果发布为 HTTP API。

## 从哪里开始

| 你要做什么 | 从这里开始 |
| --- | --- |
| 先看到推荐结果 | [Docker 快速开始](./docker.md) |
| 修改推荐逻辑 | [编写推荐流程](../guides/recommendation-flow.md) |
| 连接自己的数据 | [接入数据源](../guides/data-sources.md) |
| 接入已有模型服务或训练模型 | [模型训练与在线推理](../guides/model-lifecycle.md) |
| 部署完整开发环境 | [服务部署](../operations/deployment.md) |

Docker Demo 内置示例数据和流程，仅用于体验功能，无需外部服务。实际业务接入需要自行定义连接业务数据的表、SQL 推荐流程和 API。业务定义可以使用本地 SQL 文件管理；需要持久化并跨实例共享定义、训练模型或使用 Flink 时，再按需准备相应服务。组件和执行机制见[架构设计](../reference/architecture.md)。

## 当前状态

当前版本为 beta，不建议用于生产环境，也不保证接口兼容性。1.0 尚无确定发布时间。

已有功能包括 SQL 推荐流程、HTTP API、UI、Redis/JDBC/MongoDB/Milvus/Kafka 等数据源、模型训练与推理、超时降级，以及指标和 Trace。

HTTP 和 JDBC 已支持账号认证；当前所有认证用户拥有相同能力，尚无角色或资源级权限控制。配置见[入口认证](../operations/deployment.md#入口认证)。
