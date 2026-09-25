# SQLRec 架构

SQLRec 用 SQL 编排推荐流程：读取特征与候选集、调用函数和模型服务、返回结果，也可以将结果写入外部存储。下面先看模块全景，再从一次请求出发解释执行模型和数据边界。具体语法、参数和部署步骤见文末链接。

## 模块全景

```text
                        ┌──────────────────────────────────────────┐
                        │             sqlrec-frontend              │
                        │         Thrift / REST / CLI / UI         │
                        └───────────────┬──────────────────────────┘
                                        │ 调用
                        ┌───────────────▼──────────────────────────┐
                        │               sqlrec-core                │
                        │    语句分发 / Calcite 编译 / 函数执行    │
                        │         元数据 / 模型与服务管理          │
                        └──┬───────────────┬───────────────┬───────┘
                           │               │               │
              ┌────────────▼───┐   ┌───────▼──────┐  ┌─────▼────────┐
              │  sqlrec-sql-   │   │ sqlrec-model │  │  sqlrec-udf  │
              │     parser     │   │   模型后端   │  │   内置函数   │
              │ 双 SQL 解析器  │   │              │  │              │
              └────────────────┘   └──────────────┘  └──────────────┘
                           │               │               │
                        ┌──▼───────────────▼───────────────▼───────┐
                        │              sqlrec-common               │
                        │        表抽象 / 执行上下文 / 配置        │
                        └───────────────┬──────────────────────────┘
                                        │ 运行时 SPI 发现
                        ┌───────────────▼──────────────────────────┐
                        │            sqlrec-connectors             │
                        │    Redis / JDBC / MongoDB / Milvus /     │
                        │            Kafka / filesystem            │
                        └──────────────────────────────────────────┘
```

图中上半部分表示请求进入引擎后使用的主要模块，下半部分表示共享抽象和存储适配。通往 Connector 的箭头表示运行时通过 SPI 发现实现；`sqlrec-common` 不依赖具体 Connector。`sqlrec-ui` 的静态资源由 frontend 提供；`sqlrec-demo` 提供示例，`sqlrec-flink` 用于 Flink 集成测试。

先区分几个贯穿全文的对象：

| 对象 | 在执行流程中的含义 |
| --- | --- |
| Connector 表 | 外部数据或本地文件在 SQL 中的表入口，决定可用的查询和写入方式 |
| SQL 函数 | 由多条语句组成、可反复调用的推荐流程 |
| Schema | 当前执行器可见的表和函数定义，也容纳命名的 `CACHE TABLE` |
| 执行上下文 | 当前调用的变量、指标标签和取消状态 |
| API / Service | API 将 SQL 函数发布给业务调用方；Service 提供模型推理端点 |

## 从请求到结果

```text
客户端
  ├─ Thrift / REST SQL / CLI ─→ SQL 解析与分发
  │                              ├─ 本地可执行 ─→ Calcite 编译 ─→ 执行 ─→ 返回结果
  │                              │                              └─ Connector / SQL 函数
  │                              └─ 其他语句 ─→ Flink SQL Gateway（远程模式的 Thrift 会话）
  └─ REST API ─→ 查找 API 对应的 SQL 函数 ─→ 执行函数 ─→ 返回 JSON
```

SQLRec 使用两个解析器：标准 Flink SQL 优先由 Flink 解析器处理，SQLRec 的 `CACHE TABLE`、`CALL`、模型管理等扩展语句由独立的 SQLRec 解析器处理。解析后，执行器按语句类型决定由本地 Calcite 执行、操作元数据，还是交给 Flink。

本地路径负责面向在线请求的 SQL 查询、数据写入、SQL 函数和控制语句。Calcite 将查询编译成可执行代码，执行时通过 Connector 读取或写入数据。`SHOW`、模型与服务等资源命令由 SQLRec 自己处理。远程元数据模式下，Thrift 会话把未由本地处理的语句（例如部分流式 DDL）转发到 Flink SQL Gateway；`USE` 和 `SET` 同时作用于本地与远程会话。REST `/sql/v1` 和 CLI 没有这条透明转发路径，本地文件元数据模式也不依赖 Flink Gateway。

一个 Thrift 会话持有自己的 SQL 执行器；REST `/sql/v1` 每个请求新建一个执行器，同一请求中的多条 SQL 依次运行。`POST /api/v1/<name>` 则查找已发布的 API 和对应的 SQL 函数，为每次调用建立执行数据和输入表。API 的发布与请求格式见[发布和调用 API](../guides/api.md)。

按入口看，同一段 SQL 的可见状态和转发能力并不完全一样：

| 入口 | 执行器和状态的范围 | Flink 转发 |
| --- | --- | --- |
| Thrift | 一个会话保留一个执行器、默认数据库和命名中间表 | 远程模式下支持 |
| REST `/sql/v1` | 一个请求内的多条 SQL 共用执行器，请求结束后释放 | 不支持自动转发 |
| CLI | 当前 CLI 执行过程使用本地执行器 | 不支持自动转发 |
| REST `/api/v1/<name>` | 每次调用建立函数输入表和执行上下文 | 不转发 |

## 执行模型

### 普通 SQL 与 SQL 函数

普通 `SELECT`、`INSERT`、`UPDATE`、`DELETE` 经过 Calcite 的校验、优化和编译，然后在当前执行上下文中运行。上下文保存请求变量、指标标签和取消状态；查询结果在进程内物化后返回给客户端。`WITH` 查询不走本地 Calcite 路径；在远程模式的 Thrift 会话中，它可交给 Flink 处理。

SQL 函数是一组按顺序提交的 SQL 语句，而不是单条查询。例如：

```sql
CREATE SQL FUNCTION recommend;

DEFINE INPUT TABLE user_info (user_id BIGINT);

CACHE TABLE candidates AS
SELECT item_id, score FROM hot_item LIMIT 10;

RETURN candidates;
```

顶层 `RETURN` 结束定义并保存函数。调用时，SQLRec 取得已编译的函数（首次访问编译，后续可复用），将输入表加入本次调用的 schema，再执行函数体。独立 API 请求之间不共享输入表或中间结果。函数定义、输入表和调用语法见[编写 SQL 推荐流程](../guides/recommendation-flow.md)。

一次函数调用大致经历三步：

1. 按函数声明检查输入表，取得函数定义和编译结果；
2. 按表的读写依赖执行查询、`CACHE TABLE`、`CALL` 和控制语句；
3. 从执行到的 `RETURN` 取得结果，交给 Thrift/REST 转成客户端响应。

例如，业务方调用 `POST /api/v1/recommend` 时，API 名先映射到 SQL 函数；请求里的输入行变成该函数可见的输入表。函数读取 Connector、生成中间 `CacheTable`，必要时调用模型 Service，最后由 `RETURN` 给出 JSON 响应。直接运行普通 SQL 走同一个本地查询引擎，但不会自动获得“已编译 SQL 函数”的复用能力。

函数体中的几个概念决定了数据如何流动：

| 语句 | 执行时的作用 |
| --- | --- |
| `CACHE TABLE t AS ...` | 执行查询或同步调用，将结果命名为当前执行过程可引用的内存表 `t` |
| `CALL f(...)` | 调用 SQL 函数或 Java 表函数；同步调用的结果可继续处理或返回 |
| `IF ... THEN ... ELSE ...` | 按查询条件选择分支；`TIMEIN` 可在超时或异常时回退到 `ELSE` |
| `RETURN` | 返回表、查询或同步调用的结果，并结束当前函数的后续执行 |

`CACHE TABLE` 不是持久表。它留在当前执行器的 schema 中：Thrift 会话和单个 REST SQL 请求中的后续语句可以引用它，独立 REST/API 请求不能共享它。普通查询返回的结果也不会自动注册为命名的 `CacheTable`。需要跨请求保存数据时，应写入外部 Connector 表。

### 依赖、并行与异步

SQL 函数按读写表关系确定语句依赖：必须等待前序结果的语句按依赖执行；没有依赖的节点在 `PARALLELISM_EXEC` 开启时可以并行。`RETURN` 后未开始的节点会跳过。`CALL ... PARTITION BY` 将输入表拆成多份并发调用，再合并结果；`CALL ... ASYNC` 提交后台任务并立即返回，因而不能作为同步结果供 `CACHE TABLE` 或 `RETURN` 消费。

本地查询和中间结果主要在 JVM 内存中物化，规模受进程内存约束。SQLRec 在这里没有通用的落盘或流式中间结果机制；需要持续处理数据时使用 Flink 路径。完整控制流语法见[SQL 参考](./sql.md)。

## 数据来源与缓存

### Connector 能力

所有 Connector 都提供 Calcite 表，具体查询能力取决于存储类型：

| Connector | 本地执行中的主要能力 |
| --- | --- |
| Redis | 按主键取行；不支持全表扫描；支持 SQL 写入和 Flink Lookup/写入 |
| JDBC、MongoDB | 主键点查或按条件扫描，支持 SQL 写入 |
| Milvus | 主键/过滤查询和向量检索，支持 SQL 写入及 Flink Sink |
| Kafka | Calcite 表只写入；流式读取交给 Flink |
| filesystem | 首次使用时把本地 CSV/JSON 加载到内存；后续读写作用于该表对象的内存数据，不回写源文件 |

KV Join 在右表关联列是主键时批量点查；非主键关联逐键扫描。Milvus 可作为向量检索 Join 的右表。Connector 配置、查询约束和写入语义见[内置 Connector](./connectors/builtin-connectors.md)。

### 四种容易混淆的“缓存”

| 数据 | 作用域与用途 | 更新方式 |
| --- | --- | --- |
| `CACHE TABLE` | 当前执行器 schema 中的命名中间结果 | 执行该语句时生成；`FLUSH` 不删除 |
| Connector 主键行缓存 | Redis、JDBC、MongoDB 的每个 Calcite 表对象；加速主键点查 | 按表配置的 TTL 和容量过期；经 SQL 写入时失效相关主键 |
| filesystem 内存快照 | 每个 filesystem 表对象首次加载的整表数据 | SQL 写入只修改内存；无 TTL，不自动感知文件变化 |
| 定义缓存 | 进程内的 schema、SQL/Java 函数、API 和服务配置 | 按各自策略刷新或过期；`FLUSH` 使这些定义缓存失效 |

### Connector 表数据缓存与查询路径

主键行缓存只作用于 `getByPrimaryKey()`：Redis 的主键等值查询（包括 `AND` 中可提取的主键条件）、JDBC/MongoDB 的单一主键等值查询，以及右表按主键关联的 KV Join 会使用它。JDBC/MongoDB 的复合条件、非主键条件、范围查询和全表扫描直接访问数据源。Milvus、filesystem 和 Flink Redis Lookup 不使用这层主键缓存；Kafka Calcite 表不支持读取。

以 `id` 为主键时，可用下面的条件区分常见路径：

| 查询条件 | Redis | JDBC / MongoDB |
| --- | --- | --- |
| `WHERE id = 1` | 主键缓存 | 主键缓存 |
| `WHERE id = 1 AND status = 'active'` | 先按主键取候选行，再过滤 | 复合条件扫描，不使用主键缓存 |
| `WHERE status = 'active'` | 无主键条件，无法全扫 | 按条件扫描，不使用主键缓存 |

外部系统或 Flink 写入不会主动清除已有 Calcite 表对象的主键缓存。`FLUSH` 也不直接清除主键缓存、filesystem 快照或会话中的 `CACHE TABLE`；重新构建表对象才会得到新的缓存或重新读取文件。

## 元数据管理

元数据描述数据库、表、Java UDF、SQL 函数、API、模型和服务；它决定 SQLRec 能识别和调用什么，不保存 Connector 中的业务数据。两种模式通过同一元数据访问层向执行器提供定义，区别在于定义的来源和更新方式。`SQL_SCHEMA_DIR` 为空时使用远程模式；设置为非空目录时使用本地 SQL 文件模式。

| 项目 | 远程元数据模式 | 本地 SQL 文件模式 |
| --- | --- | --- |
| 表和 Java UDF | 从 Hive Metastore（HMS）读取数据库、表结构和函数定义 | 启动时递归解析 `SQL_SCHEMA_DIR` 下的 `.sql` 文件，在进程内建立 schema |
| SQL 函数、API、模型、Checkpoint、Service | 保存在 PostgreSQL，由 SQLRec 的管理命令读取或更新 | SQL 文件中的函数、API、模型和 Service 定义初始化为进程内对象；没有共享的元数据存储 |
| 更新定义 | 资源命令更新 PostgreSQL；远程模式的 Thrift 会话可将表等 Flink DDL 转发给 Flink SQL Gateway，由其更新 HMS | 修改 SQL 文件并重启或重新部署实例；`FLUSH` 不重新解析文件 |
| 依赖与用途 | 需要 HMS 和 PostgreSQL，适合多实例共享定义；Flink Gateway 只在需要转发语句时使用 | 适合 Demo、本地开发和线上 serving：SQL 文件可随镜像或部署配置纳入版本管理，省去 HMS、PostgreSQL 和 Flink Gateway；实际使用的 Connector、模型端点仍需可访问 |

本地 SQL 文件模式**不允许执行 DDL**。定义只能通过 SQL 文件加载；查询、数据写入、`CACHE TABLE` 和调用已定义的函数仍可执行。线上 serving 可把完整 SQL 目录与应用版本一起发布，让每个实例启动时加载同一版本；修改定义后需重新部署或重启所有实例。

两种模式都会把表定义转换为 Calcite 表，并缓存数据库列表、表结构和函数/API/服务定义。远程模式按 `SCHEMA_CACHE_EXPIRE` 在后续访问时刷新相关缓存，也可用 `FLUSH` 使其立即失效；HMS 表刷新会依据 `transient_lastDdlTime` 决定是否复用已有表对象，外部改表但未更新该时间时可能继续使用旧对象。本地模式的元数据只在启动初始化时从文件读取；`FLUSH` 只使进程内缓存失效，后续访问会重建缓存，文件改动仍需重启才能生效。部署所需配置见[服务部署](../operations/deployment.md)，本地文件示例见[Docker 快速开始](../getting-started/docker.md)。

## 模型与在线服务

模型功能包含三个对象：**Model** 定义类型和输入输出，**Checkpoint** 标识训练、下载或导出的版本，**Service** 将模型关联到在线推理端点（自托管时负责部署，外部模型则指向已有 URL）。常见训练型模型的路径是：

```text
CREATE MODEL → TRAIN MODEL → 检查 Checkpoint 状态
             → EXPORT MODEL → CREATE SERVICE → call_service(...)
```

训练和导出提交 Kubernetes Job；提交成功不代表任务已完成，应检查 Checkpoint 状态。自托管模型的 `CREATE SERVICE` 部署在线工作负载；SQL 函数中的 `call_service` 把输入行发送给该服务，并将预测结果并回行中。不同后端的路径不同：Hugging Face Transformers 不需要导出，`external` 模型直接对接已有 HTTP 服务，不进行训练、导出或自行部署推理容器。

`CREATE SERVICE` 管理模型推理服务；`CREATE API` 发布 SQL 函数供业务请求调用。一个对外 API 可以在函数内部查询 Connector，并通过 `call_service` 使用模型服务。操作步骤和后端差异见[模型训练与在线推理](../guides/model-lifecycle.md)及[内置模型](./models/builtin-models.md)。

## 运行边界

远程模式运行在线查询需要元数据服务（HMS、PostgreSQL）和实际使用的存储系统；转发语句需要 Flink SQL Gateway，自托管模型的训练和部署需要 Kubernetes。本地 SQL 文件模式也适合线上 serving，可减少元数据服务依赖并把定义纳入版本管理。REST 把同步业务执行放在独立线程组，Thrift 按会话管理执行器；指标和 Trace 用于观察请求与节点执行。部署依赖、端口和资源要求见[服务部署](../operations/deployment.md)。

读这个架构时要记住三个边界：SQLRec 的本地查询在进程内物化；`CACHE TABLE` 只在当前执行范围内可见；外部数据、模型服务和 Flink 作业各自有独立的生命周期。部署依赖与资源规模应按实际启用的路径选择。

## 深入阅读

- [编写 SQL 推荐流程](../guides/recommendation-flow.md)与[SQL 参考](./sql.md)：函数、控制流与完整语法。
- [接入数据源](../guides/data-sources.md)与[内置 Connector](./connectors/builtin-connectors.md)：表定义、查询和缓存参数。
- [发布和调用 API](../guides/api.md)与[模型训练与在线推理](../guides/model-lifecycle.md)：对外接口及模型生命周期。
- [自定义 Connector](../development/custom-connector.md)、[自定义 UDF](../development/custom-udf.md)、[自定义模型](../development/custom-model.md)：扩展点与实现要求。
