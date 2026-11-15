# SQLRec 架构

SQLRec 在 JVM 内编译和调度 SQL 推荐流程，通过 UDF 扩展计算、Connector 访问存储、HTTP 调用模型服务。本文介绍编译、调度、状态隔离和外部访问的实现机制。

## 核心组件 {#模块全景}

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

箭头表示模块协作及运行时 SPI 发现，不是 Maven 依赖关系；Connector 通过 `sqlrec-common` 的接口接入。

| 层次 | 主要实现 | 职责 |
| --- | --- | --- |
| 服务入口 | `sqlrec-frontend` | HTTP、Thrift/JDBC、CLI、UI、会话管理 |
| SQL 编译 | `sqlrec-sql-parser`、`sqlrec-core` 的 `compiler` 包 | 解析、查询编译、函数编译 |
| 流程运行时 | `sqlrec-core` 的 `runtime` 包 | DAG 调度、变量、返回、超时和取消 |
| 数据与函数适配 | `sqlrec-connectors`、`sqlrec-udf` | 存储算子、标量及表函数 |
| 元数据与模型管理 | `sqlrec-core` 的 `db`、`model` 包，`sqlrec-model` | 定义与缓存、模型任务、推理服务 |
| 共享接口 | `sqlrec-common` | 表结构、上下文和扩展契约 |

| 执行层次 | 决定什么 | 实现基础 |
| --- | --- | --- |
| SQL 函数 DAG | 多条语句的依赖与并行顺序 | SQLRec 执行节点与调度器 |
| 单条 SQL 关系计划 | 扫描、过滤、Join、排序等操作 | Calcite + SQLRec 存储算子 |

## 请求入口与执行路径 {#从请求到结果}

### HTTP 推荐 API

```text
HTTP 请求
  → Netty I/O：解码
  → HttpServerHandler：业务线程处理
  → RestFunctionExecutor：查找 API 与函数编译计划
  → 创建根 schema，绑定输入 CacheTable 与执行变量
  → SqlFunctionBindable：执行函数 DAG
  → 物化 RETURN 结果 → JSON 响应

进程共享：API 映射、函数编译计划
请求独立：输入表、中间表、执行变量
```

| 环节 | 关键实现 |
| --- | --- |
| 网络与业务线程分离 | `RestServer` 使用 Netty I/O 线程及独立业务线程组；节点并发使用引擎执行器 |
| API → 函数 | `SqlApiCache` 保存名称映射，`CompileManager` / `SqlFunctionCache` 取得编译结果 |
| 请求数据绑定 | `CalciteSchemaFactory` 创建根 schema；JSON 行绑定为输入表，`params` 写入 `ExecuteContextImpl` |

### SQL 入口与语句分发

CLI、REST SQL 和 Thrift 会话通过 `SqlExecutor` 分发：

| 语句类别 | 处理路径 |
| --- | --- |
| SQL 函数定义 | 收集输入和函数体，编译并保存定义 |
| 本地查询、写入、流程语句 | `CompileManager` → 执行节点 → `bind` |
| 库、表、Java UDF 的纯元数据 DDL | 元数据适配层；远程模式经 HiveCatalog 写 HMS |
| SHOW、DESCRIBE | 读取元数据或会话缓存表 |
| 模型、checkpoint、Service 命令 | 资源管理器，必要时提交外部任务 |
| 可转发的远程语句 | 远程模式的 Thrift 会话 → Flink SQL Gateway |

没有本地处理结果才进入远程路由；本地编译或运行异常直接返回，不触发 Gateway 重试。

## SQL 解析与编译 {#执行模型}

```text
单条 SQL · NormalSqlCompiler
  解析与校验 → 关系表达式 → 计划优化 → Enumerable 代码生成
                                             ↓
                                      CalciteBindable

SQL 函数 · FunctionCompiler
  登记结构占位 → 逐条编译节点 → 检查 RETURN 结构 → 建立依赖 DAG
                                                 ↓
                                        SqlFunctionBindable
```

### 解析与可执行性判断

| 阶段 | 实现与判断 |
| --- | --- |
| 解析标准 SQL | `SqlRecSqlParser` 优先使用 Flink 解析结果 |
| 解析扩展语句 | SQLRec 独立语法处理 `CACHE TABLE`、函数调用、模型管理等扩展 |
| 判断本地能力 | `SqlTypeChecker` 检查语句类型及表适配；当前 CTE 不进入本地查询路径 |

可解析、可保存元数据、可本地执行，分别由不同阶段判断。

### 单条 SQL：从关系计划到执行代码

| 处理 | 关键实现与产物 |
| --- | --- |
| 名称与类型校验 | 从当前 schema 取得表结构、函数签名 |
| 计划优化 | `VolcanoPlanner` 使用 Calcite 通用规则及 SQLRec 的 KV Join、向量 Join、写入、UNION 规则 |
| 代码生成 | 转为 Enumerable 计划，由 `EnumerableInterpretable.toBindable()` 编译成 JVM 内执行对象 |
| 运行与物化 | `CalciteBindable` 通过 `SqlRecDataContextImpl` 绑定 schema、变量及参数，将结果物化为行数组 |
| 诊断 | 保留逻辑计划、物理计划和生成代码，供 UI 使用 |

### SQL 函数：编译期建立数据结构与节点

`FunctionCompiler` 在输入声明及缓存表编译后登记**只有结构、没有请求数据**的 `CacheTable`，供后续语句校验；顶层 `RETURN` 完成返回结构检查和 DAG 初始化。

| 执行节点 | 工作 |
| --- | --- |
| `CalciteBindable` | 本地查询或写入 |
| `CacheTableBindable` | 运行子节点，发布命名结果表 |
| `CallSqlFunctionBindable` / `JavaFunctionBindable` | SQL 子函数 / Java `evaluate` 调用 |
| `IfBindable` / `ReturnBindable` | 分支 / 返回 |
| `SqlFunctionBindable` | 整个函数的节点、结构和依赖 |

定义函数时进行编译校验；元数据加载时可重新编译。`SqlFunctionCache` 在进程内复用结果，远程模式按刷新策略重编译。

| 调用形式 | 目标绑定与结构处理 |
| --- | --- |
| 静态 SQL 子函数 | 编译时绑定，取得返回结构；存在 `LIKE` 时检查兼容性 |
| 动态调用 | `FunctionProxyBindable` 运行时解析函数名；编译期通过 `LIKE` 声明结构 |

## UDF 注册与执行

Java UDF 分为嵌入 SQL 表达式的**标量函数**和通过 `CALL` 处理整表的**表函数**；SQL 子函数则由前述 `FunctionCompiler` 编译。

### 注册与执行路径

```text
函数名 → Java 类名
  ├── 内置：FunctionConfigs
  └── 自定义：CREATE FUNCTION → 元数据
                       ↓ 从运行时类路径加载
标量函数
  HmsSchema → UdfManager → SqlRecScalarFunctionImpl
    → Calcite 类型校验、重载解析与代码生成 → SQL 表达式结果

Java 表函数
  CALL → FunctionBindableFactory → JavaFunctionUtils
    → JavaFunctionBindable → 注入参数并反射调用 evaluate
    → CacheTable 行数据 → 后续 DAG 节点
```

| 类型 | 接入方式 | 计算粒度与用途 |
| --- | --- | --- |
| 标量函数 | 公共 `evaluate` 方法适配为 Calcite `ScalarFunction` | 在表达式中对行值计算，如 `l2_norm`、`array_contains` |
| Java 表函数 | `JavaFunctionBindable` 包装为执行节点，按静态实参类型选择 `evaluate` 重载 | 接收 `CacheTable`、字面量或变量，进行批量处理，如 `dedup`、`weighted_merge`、`call_service` |

注册只保存名称和类名映射，JAR 及依赖须预先部署到各 SQLRec 进程的类路径；UDF 在 SQLRec JVM 内执行。

### 编译契约与运行状态

| 机制 | 实现与边界 |
| --- | --- |
| 标量函数上下文 | `SqlRecCallNotNullImplementor` 在生成调用代码时注入 `DataContext`，不占 SQL 显式参数 |
| 标量函数确定性 | 读取 `@UDFType` 的 `deterministic` / `stateful` 标记并传递给 Calcite，避免将非确定性调用按常量折叠 |
| 表函数返回结构 | `LIKE` 提供编译契约；未指定时，返回 `CacheTable` 的函数默认会在编译期执行以推断字段。`ENABLE_JAVA_FUNCTION_SCHEMA_INFERENCE=false` 可禁止此行为 |
| 表函数上下文 | 运行时自动注入 `ReadonlyContext` / `ExecuteContext`；可写上下文使节点不可并行，调度规则见下节 |
| 表函数结果 | 返回 `CacheTable` 时物化并按编译期字段适配；无结果操作可返回 `Void`，不提供结果表 |
| 实例状态 | `JavaFunctionBindable` 持有 UDF 实例，可随编译计划跨请求复用；请求状态应通过参数和上下文传递 |

网络或有副作用的表函数应显式使用 `LIKE`。实现和部署步骤见[自定义 UDF](../development/custom-udf.md)，调用语法见[CALL](./sql.md#call)。

## DAG 调度与运行上下文

### 依赖关系如何建立

`TopologicalSortUtils` 按语句顺序读取节点的 `getReadTables()`、`getWriteTables()` 和并行声明：

| 约束 | 依赖方向 |
| --- | --- |
| RAW：后读依赖先写 | 等待表产生或更新 |
| WAR：后写依赖先读 | 读取完成后才能覆盖 |
| WAW：后写依赖先写 | 同表写入保序 |
| 执行屏障 | 等待前序节点，后续节点等待屏障 |

`RETURN`、不可并行节点形成屏障；包括 `SET`、`ASSERT` 和声明可写 `ExecuteContext` 的 Java 表函数。仅使用 `ReadonlyContext` 不触发该限制。构图后进行拓扑排序和冗余边消除；不推断 Java UDF 内任意外部副作用。

下面是函数体片段：假设子函数已定义，两路召回结构相同、无额外共享写入，且允许并行。

```sql
CACHE TABLE interest AS CALL interest_recall(user_info);
CACHE TABLE hot AS CALL hot_recall(user_info);
CACHE TABLE candidates AS
SELECT * FROM interest UNION ALL SELECT * FROM hot;
CACHE TABLE ranked AS CALL rank_items(candidates);
RETURN ranked;
```

```text
interest_recall → interest ─┐
                           ├→ UNION ALL → candidates → rank_items → ranked → RETURN
hot_recall ─────→ hot ──────┘

两路召回可并行；合并等待两路完成，排序等待合并完成，RETURN 形成执行屏障。
```

SQL 子函数的读写信息映射回调用方，参与外层依赖分析；对同一缓存表再次写入时，也须等待先前读取完成。

### 并行、分区与异步

| 方式 | 实现 | 结果与边界 |
| --- | --- | --- |
| DAG 并行 | `SqlFunctionBindable` 用 `CompletableFuture` 等待依赖，再提交节点 | `ExecutorServiceUtils` 使用按任务创建的虚拟线程；关闭 `PARALLELISM_EXEC` 时顺序执行 |
| 分区调用 | `PartitionExecutor` 切分输入，为分区建立 schema | 并发执行目标后合并结果 |
| `ASYNC` | 提交后台调用后立即返回 | 无同步结果供当前 `CACHE TABLE` / `RETURN` 消费 |

这些并发发生在 SQLRec 进程内，不为每个节点提交分布式作业。

### 共享计划、隔离数据与函数返回

```text
进程共享
├── 函数编译计划
├── 数据库 HmsSchema
└── Connector 表对象
        ↑ 复用
        ├── 请求 A（独立状态）
        │   ├── 根 schema：输入表、中间表
        │   ├── 变量映射 ←→ SQL 子函数共享
        │   └── SQL 子函数：独立 schema、局部表、返回帧
        └── 请求 B（独立状态，结构同上）

取消状态：父上下文 → 子上下文
```

| 实现 | 状态处理 |
| --- | --- |
| `CallSqlFunctionBindable` | 检查实参与形参结构，在新 schema 中按位置绑定；局部表不暴露给调用方 |
| `ExecuteContextImpl` | 管理变量、调用栈、Trace、取消和返回帧；子函数共享变量映射，独立返回帧 |
| `CacheTableBindable` | 完整物化结果 → 检查取消 → 发布到当前 schema；后续节点按表名复用结果 |

表作用域隔离不等于变量复制。查询结果和中间表主要保存在内存中，受 JVM 内存约束。

### 失败、超时与结果发布

| 层次 | 实现与处理 |
| --- | --- |
| 节点 | `ProxyAllBindable` 统一处理超时、取消检查、Trace、耗时、行数及合并分支降级 |
| DAG | 未恢复错误尽早结束等待，取消当前函数范围；子上下文沿父链感知取消，任务协作退出 |
| TIMEIN | `IfBindable` 在子上下文执行 THEN；失败或超时后取消并运行 ELSE，阻止迟到缓存或返回值覆盖备用结果 |
| 分区 | 按配置整次失败，或丢弃失败分区后合并 |

取消不保证网络请求立即停止，已完成的外部写入也没有跨 Connector 统一回滚。详见[异常恢复](../guides/exception-recovery.md)及[可观测性](../operations/observability.md)。

## 元数据、表对象与存储执行 {#数据来源与缓存}

### 元数据管理

`MetadataAccess` 分离 schema 定义、资源记录和模型文件访问；`MetadataAccessFactory` 根据 `SQL_SCHEMA_DIR` 选择实现。

| 模式 | Schema 定义 | 资源与文件 |
| --- | --- | --- |
| 远程 | `HmsSchemaAccess` → HMS；DDL 经 HiveCatalog | `DbStoreAccess` → PostgreSQL；文件访问走 Hadoop 接口 |
| 本地文件 | `SqlFileParser` → `SqlFileSchemaAccess` | `InMemoryStoreAccess`；Service 可指向已有端点 |

| 对象 | 复用与更新 |
| --- | --- |
| 根 schema | `CalciteSchemaFactory` 为每个执行器创建 |
| 数据库 `HmsSchema` | 在进程内复用，加载表与函数；两种元数据模式均使用 |
| Connector Table | 根据表定义经工厂创建，供编译及执行使用 |
| 本地内存 DDL | 不回写文件、不跨实例共享，重启恢复文件定义 |
| 远程持久定义 | 可跨实例共享；每个实例仍维护自己的执行对象与缓存 |

定义范围见[元数据 DDL](./sql.md#元数据-ddl)。

### Connector 如何接入查询计划

```text
表元数据 → SPI / HmsTableFactory → Calcite Table
                                       ↓ 按能力与查询形态选算子
KV 主键 Join（INNER / LEFT）
  物化左表、去重关联键（忽略 NULL）
    → 主键类型转换 → 查询表级缓存 → 未命中键批量读取后端
    → 按键合并并拼接（LEFT 无匹配补 NULL）

向量检索 Join
  识别排序、Top K、过滤
    → 按左行构造 VectorSearchRequest
    → VectorSearchable / Milvus 检索
    → 拼接左行、右行与分数 → 继续外层关系计划
```

| 路径 | 实现与限制 |
| --- | --- |
| 工厂 | `TableFactoryUtils` 通过 SPI 查找 `HmsTableFactory`；按 `connector` 选项创建 Table |
| KV 主键 Join | `SqlrecEnumerableKvJoin` → `KvJoinUtils` → `SqlRecKvTable.getByPrimaryKey()`；返回同键多行，LEFT 无匹配补 NULL |
| KV 非主键 Join | 按不同关联键构造过滤扫描，成本不同于批量主键读取 |
| 向量 Join | `SqlRecVectorJoinRule` → `SqlrecEnumerableVectorLookupJoin` → `VectorJoinExecutor`；按左行构造向量、字段、过滤及 Top K 请求 |
| 后续计算 | 检索结果拼接后继续外层投影、排序；未下推条件由支持的本地计划处理 |

特化路径受表能力与查询形态限制，不支持的查询会拒绝执行，见[内置 Connector](./connectors/builtin-connectors.md)。

### 缓存与定义刷新 {#connector-表数据缓存与查询路径}

| 缓存或数据 | 所在层次 | 更新与失效 |
| --- | --- | --- |
| `CACHE TABLE` | 执行 schema | 语句完成后发布，随请求、会话或子函数作用域 |
| 表结构与 Connector 对象 | 数据库 schema | 比较元数据，未变化复用，变化重建 |
| 函数编译结果 | `SqlFunctionCache` | 命中复用，定义刷新或失效后重新编译 |
| 主键缓存 | Table 内的 Caffeine | TTL、容量淘汰、SQLRec 写入使相关键失效 |
| Filesystem 数据 | Table 内的文件快照 | 只写内存，对象重建后重载文件 |

| 操作 | 保留什么 / 重建什么 |
| --- | --- |
| 普通元数据刷新 | 保留定义未变化的 Table、主键缓存及 Filesystem 数据 |
| `FLUSH` | 失效定义缓存和 Table；保留会话缓存表及本地内存 DDL，不重读 SQL 文件 |
| 外部系统写入 | 不主动清主键缓存，可见性受 TTL 等策略影响 |

细则见[查询缓存](./connectors/builtin-connectors.md#查询缓存)与 [FLUSH](./sql.md#flush)。

## 模型任务与在线推理 {#模型与在线服务}

`ModelManager`、`ServiceManager` 经 `ModelController` SPI 调用后端，生成训练、导出及推理工作负载配置。

### 任务管理与产物衔接

```text
准备阶段（训练型后端）
  Model / ModelController
    → 训练、导出 Job（Kubernetes）
    → checkpoint 元数据 + 模型文件
    → 部署推理服务（Deployment / Service）
                       ↓ 提供 HTTP 端点
在线调用
  输入 CacheTable → CallServiceFunction 特征编码
    → PredictionClient HTTP 推理 → 追加预测字段 → 新 CacheTable

API 发布推荐流程；Service 提供模型预测。
```

| 对象或阶段 | 保存 / 执行内容 |
| --- | --- |
| Model | 字段、模型类型、配置 |
| checkpoint 元数据 | Model 定义快照、名称、类型、任务 DDL/YAML、状态；按快照与名称定位产物 |
| 模型存储 | 实际权重与导出文件；训练型后端由 origin 导出 export |
| 导出 / 服务创建前 | 检查源 checkpoint 的完成状态及类型 |
| 自托管服务创建 | 校验配置 → 提交 Kubernetes → 保存定义与 URL；提交成功不等于就绪 |
| 文件模式加载 Service | 只建立调用配置，不执行 Kubernetes 部署 |

### 推理调用如何进入 SQL 结果

| 环节 | 实现 |
| --- | --- |
| 解析输入输出 | `CallServiceFunction` 读取 Service 配置及模型字段 |
| 请求 | 按 Model 选输入列，编码行式或 User-Item JSON；`PredictionClient` 发 HTTP 请求 |
| 响应 | 按输入位置追加声明的预测字段，形成新 `CacheTable`；缺失或过短列补 NULL |

服务提供方仍须保证响应行数和顺序。推理服务独立运行并扩缩容；Hugging Face 可用下载的 origin 直接部署，`external` 连接已有端点。**API 发布流程，Service 提供预测**。后端差异见[模型指南](../guides/model-lifecycle.md)。

## 执行边界 {#运行边界}

### Gateway 转发边界

| 处理 | 实现与边界 |
| --- | --- |
| 会话与操作归属 | `SessionManager` 管理会话对应的 `SqlExecutor`，区分本地和远程操作 |
| 远程执行 | `GatewayClient` 按需连接、同步数据库及会话设置、执行 RPC |
| 转发入口 | 仅远程元数据模式的 Thrift/JDBC；REST API、REST SQL、CLI 无透明转发 |
| 路由限制 | 可转发的 CTE 等进入 Gateway；SQLRec 扩展不能靠转发绕过限制 |
| 纯元数据 DDL | 本地适配层写 HMS，不创建 Flink 执行环境 |

详见[SQL 参考](./sql.md#元数据-ddl)与[客户端连接](../guides/clients.md#gateway-errors)。

### 内存与状态边界

| 入口或资源 | 状态范围 |
| --- | --- |
| HTTP 推荐 API | 每次调用独立输入、变量和根 schema |
| REST SQL | 同一请求内共用执行器 |
| Thrift/JDBC | 当前会话保留执行器、默认库和缓存表 |
| 本地 CLI | 当前进程，与 HTTP 服务的内存独立 |
| Connector、模型服务、Flink 作业 | 各自的存储、计算和资源生命周期 |

本地查询物化在内存中，无通用中间结果落盘或持续流式执行机制。持续大规模处理使用相应 Flink 路径，训练和自托管模型部署依赖 Kubernetes。
