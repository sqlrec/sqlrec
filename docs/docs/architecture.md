# SQLRec 架构设计

## 1. 项目概述

### 1.1 定位

SQLRec 是一个**用 SQL 描述推荐系统全部业务逻辑**的推荐引擎。目标是让数据分析师、数据工程师、后端开发（不必然精通 Java/Python 工程化）都能快速搭建生产可用的推荐系统。它把以下能力统一封装进 SQL：

- 底层存储访问（Redis / JDBC / MongoDB / Milvus / Kafka / HDFS / 本地文件）
- 模型训练、导出、在线推理服务部署（XGBoost / LightGBM / CatBoost / DSSM / Wide&Deep / 外部服务）
- 推荐业务编排（召回 → 排序 → 多样化 → 落盘）

### 1.2 核心设计决策

| 决策 | 说明 |
| --- | --- |
| **Calcite 作为在线执行引擎** | 自研 SQL 引擎基于 Apache Calcite 1.32 的 Enumerable 执行层（Janino 动态编译为 Java 字节码），满足推荐在线场景的实时性要求 |
| **Flink Parser 作为语法前端** | 复用 Flink 1.19 的 parser codegen（`FlinkSqlParserImpl`），在其上扩展自定义 DDL/DML AST 节点，保证与 Flink SQL 语法兼容 |
| **双执行栈** | 在线路径走 Calcite（本进程内执行）；流式/离线路径把语句透传给远端 Flink SQL Gateway |
| **双元数据模式** | 远程模式下表/Java UDF 元数据来自 Hive Metastore，模型/服务/API/SQL 函数定义存 PostgreSQL；设置 `SQL_SCHEMA_DIR` 后可使用本地 SQL 文件 + 内存存储，不依赖 HMS/PG |
| **K8s 作为算力底座** | 模型训练/导出是 K8s Job，推理服务是 K8s Deployment，由引擎生成 YAML 并 serverSideApply |
| **全内存执行模型** | 所有中间结果（CacheTable）物化为内存 List，不做落盘/流式，依赖控制流（cache/if/assert）管理数据规模 |

### 1.3 技术栈

| 类别 | 组件 |
| --- | --- |
| 语言 | Java 21（core/frontend/model/parser/demo）、Java 11（common/udf/connectors/ui/flink） |
| SQL 引擎 | Apache Calcite 1.32（Enumerable + VolcanoPlanner） |
| 语法前端 | Flink 1.19 `flink-sql-parser`（codegen 扩展） |
| 元数据 | Hive Metastore 3.1.3（thrift client）、PostgreSQL（MyBatis 3.5.19） |
| 存储 | Lettuce（Redis）、HikariCP（JDBC）、MongoDB driver、Milvus SDK、Kafka client、Hadoop HDFS client |
| K8s | fabric8 kubernetes-client |
| 网络入口 | Thrift（Hive TCLIService 协议，兼容 beeline/Kyuubi）、Netty（REST）、JLine（CLI） |
| 观测 | Micrometer + Prometheus，OpenTelemetry 1.29（OTLP trace），Jaeger |
| 缓存 | Caffeine 2.9、自研 `ObjCache` |
| 模型 Serving | C++（catboost_server / onnx_server / tzrec server，内嵌 cpp-httplib）、Python（训练脚本） |

## 2. 总体架构

### 2.1 模块划分与依赖关系

```
                        ┌──────────────────────────────────────────┐
                        │            sqlrec-frontend               │
                        │  ThriftServer / RestServer / Cli / UI    │
                        └───────────────┬──────────────────────────┘
                                        │ 依赖
                        ┌───────────────▼──────────────────────────┐
                        │               sqlrec-core                │
                        │  compiler / executor / runtime / rules   │
                        │  node / schema / db / k8s / model / udf  │
                        └──┬───────────────┬───────────────┬───────┘
                           │               │               │
              ┌────────────▼───┐   ┌───────▼──────┐  ┌────▼─────────┐
              │ sqlrec-sql-    │   │ sqlrec-model │  │ sqlrec-udf   │
              │ parser (Flink  │   │ (gbdt/tzrec/ │  │ (scalar/table│
              │  codegen 扩展) │   │  external)   │  │  UDF 实现)   │
              └────────────────┘   └──────────────┘  └──────────────┘
                           │               │               │
                        ┌──▼───────────────▼───────────────▼───────┐
                        │              sqlrec-common               │
                        │  config / schema 抽象 / runtime 上下文 /  │
                        │  rest client / utils                     │
                        └───────────────┬──────────────────────────┘
                                        │ SPI (HmsTableFactory)
                        ┌───────────────▼──────────────────────────┐
                        │            sqlrec-connectors             │
                        │ filesystem / jdbc / kafka / milvus /     │
                        │ mongodb / redis（6 个子模块）             │
                        └──────────────────────────────────────────┘

   独立模块：sqlrec-demo（演示业务）、sqlrec-flink（Flink 集成测试）、sqlrec-ui（静态 UI 资源）
```

各模块职责：

| 模块 | 职责 |
| --- | --- |
| **sqlrec-common** | 共享基础层：配置体系（`ConfigOption`）、依赖 Calcite 类型的表抽象（`SqlRecTable`/`SqlRecKvTable`/`CacheTable`/`VectorSearchable`）、执行上下文、远程 REST client 和通用工具 |
| **sqlrec-sql-parser** | 通过 FMPP + Freemarker codegen 扩展 Flink parser，产出 `CREATE/DROP MODEL`、`TRAIN/EXPORT MODEL`、`CREATE/DROP SERVICE`、`CREATE/DROP SQL FUNCTION`、`CREATE/DROP API`、`CACHE`、`IF...CACHE`、`ASSERT`、`CALL`、`RETURN`、`FLUSH`、`SET` 等 AST 节点（`com.sqlrec.sql.parser.Sql*`） |
| **sqlrec-core** | 引擎核心：编译器（compiler）、执行器（executor）、运行时 Bindable 体系（runtime）、Calcite 优化规则（rules）与物理节点（node）、schema 组装（schema）、元数据访问（db）、K8s 管理（k8s）、模型/服务管理（model） |
| **sqlrec-udf** | 内置 UDF：scalar（`get`/`get_or_default`/`l2_norm`/`ip`/`random_vec`/`uuid`/`array_contains*`）与 table（`call_service`/`call_sqlrec_api`/`window_diversify`/`dpp_diversity`/`rule_diversity`/`dedup`/`shuffle`/`weighted_merge`/`json_to_table`/`add_col`/`truncate_table`/`tag_to_vec`/`sleep`/`get_variables`/`set_variables`/`get_growthbook_features`/`feature_coverage_metrics` 等） |
| **sqlrec-connectors** | 6 个数据源连接器，双栈适配：Calcite 侧（`*CalciteTable(+Factory)` 实现 `HmsTableFactory` SPI）+ Flink 侧（`*DynamicTableFactory` 实现 Flink `Factory` SPI，仅 redis/milvus） |
| **sqlrec-model** | 模型后端实现 `ModelController` SPI：GBDT 家族（XGBoost/LightGBM/CatBoost，Python 训练 + ONNX/CatBoost C++ serving）、TZRec 家族（DSSM/Wide&Deep）、External（透传外部 URL）。含 K8s YAML 生成（`K8sYamlBuilder`） |
| **sqlrec-frontend** | 三种入口：Thrift（Hive2 协议，兼容 beeline/kyuubi/JDBC）、REST（Netty，`/sql/v1`、`/api/v1/*`、`/metrics`、`/ui/*`）、CLI（JLine REPL）。会话管理、操作生命周期、Prometheus 指标 |
| **sqlrec-demo** | 无外部依赖的 quick-start 演示，以及完整召回→排序→多样化链路的 MovieLens SQL 定义 |
| **sqlrec-flink** | Flink 环境集成测试（connector、BatchCallServiceUDTF 等） |
| **sqlrec-ui** | 前端静态资源（仅 pom 占位） |

### 2.2 运行时全景（一条 SQL 的旅程）

```
 客户端 (beeline / kyuubi / curl / cli.sh)
    │
    ├─ Thrift:30000 ──► TCLIServiceImpl ─► SessionManager ─► SqlExecutor.executeSqlAsync()
    ├─ REST:30001 ────► Netty HttpServerHandler ─► RestSqlExecutor / RestFunctionExecutor
    └─ CLI ───────────► Cli (JLine) ─► RestFunctionExecutor / SqlExecutor
    │
    ▼
 CompileManager.parseFlinkSql()  ── SqlPreProcesser 预处理 → FlinkSqlParserImpl 解析
    │
    ▼
 SqlExecutor.executeSqlAsync() 语句分发：
    ├─ SQL 函数编译状态机 (FunctionCompiler) ── CREATE SQL FUNCTION 多语句会话式编译
    ├─ USE DATABASE / FLUSH / SET
    ├─ 资源查询 (SHOW *, DESCRIBE, SHOW CREATE)
    ├─ 可编译 CRUD (SqlTypeChecker.isFlinkSqlCompilable)
    │      └─► CompileManager.compileSql()
    │             ├─ SqlCallSqlFunction → FunctionProxyBindable (函数代理，支持 async/PARTITION BY)
    │             ├─ SqlIfCache        → IfBindable (条件分支)
    │             ├─ SqlCache          → CacheTableBindable (缓存表物化)
    │             ├─ SqlAssert         → AssertBindable (断言)
    │             ├─ SqlSet            → SetBindable (变量)
    │             └─ 普通 SQL          → NormalSqlCompiler (Calcite 全流程)
    │                                       parse → validate → sql2rel → VolcanoPlanner
    │                                       → EnumerableRelImplementor → Janino → CalciteBindable
    ├─ 资源编辑 (CREATE/DROP/TRAIN/EXPORT MODEL, SERVICE, API, SQL FUNCTION)
    │      └─► ModelManager / ServiceManager → K8sManager.applyYaml / MetadataAccess
    └─ 其他（如 CREATE TABLE 等流式 DDL）
           └─► 返回 null → SessionManager 经 ClientProxy 转发远端 Flink SQL Gateway
    │
    ▼
 bindable.bind(schema, context)  ── SqlRecDataContextImpl 提供 Calcite DataContext
    │                              执行结果物化为 CacheTable 注册进 schema（供后续语句引用）
    ▼
 ThriftUtils.convertObjectArrayToTRowSet / JsonUtils.toJson → 返回客户端
```

## 3. 服务入口层（sqlrec-frontend）

### 3.1 启动流程

`Main.main()`（`sqlrec-frontend/src/main/java/com/sqlrec/frontend/Main.java`）：

1. 校验 `ENABLE_REST_SERVER` / `ENABLE_THRIFT_SERVER` 至少一个为 true；
2. 每个启用的 server 在独立线程中启动（`startServer`），任一 server 退出即 `countDown` 主 latch，进程 `System.exit(1)`（fail-fast）。

两个 server 启动时都执行相同的三步初始化：`FunctionUpdater.initFunctionUpdateService()`（周期函数热更新，仅远程元数据模式）、`PrometheusMetricsUtils.initMetrics()`、`CalciteSchemaFactory.createCalciteSchema()`（预热 schema）。

### 3.2 Thrift 服务（兼容 Hive 协议）

- `ThriftServer.java`：`TThreadPoolServer` + `TBinaryProtocol`，暴露 Hive `TCLIService`（端口 30000）。**每连接一线程**，worker 上限未显式配置。
- `TCLIServiceImpl` 委托 `SessionManager`。会话四张 `ConcurrentHashMap`：`clientMap`（远端 Flink Gateway 代理）、`sqlExecutorMap`（每会话一个 `SqlExecutor`）、`operationMap`、`operationToSessionMap`。
- **本地/远程执行决策**（`SessionManager`）：
  1. 同步调用 `sqlExecutor.executeSqlAsync()`；
  2. 若返回非 null 且（本地文件元数据模式 或 语句不是 `USE`/`SET`）→ 包装为本地 `SqlOperation`；
  3. 否则经 `ClientProxy` 转发远端 Flink SQL Gateway（`FLINK_SQL_GATEWAY_ADDRESS:30018`）——即 `USE`/`SET` 会同时作用于本地与远端会话，未识别的 DDL（如 Kafka 流表的 `CREATE TABLE`）由 Flink 执行并写回 HMS。
- `SessionTimeoutChecker`：单线程 scheduled executor，按 `SESSION_CHECK_INTERVAL`（默认 5 分钟）扫描 `getLastAccessTime()`，超 `SESSION_IDLE_TIMEOUT`（默认 30 分钟）的会话由 `cleanupSession` 关闭。
- 结果获取：`FetchResults` 一次性返回全部行（`hasMoreRows` 恒 false），取完即 `setEnumerable(null)`。

### 3.3 REST 服务

- `RestServer.java`：Netty `NioEventLoopGroup(1)` + 默认 worker 组，`HttpServerCodec` + `HttpObjectAggregator(65536)` + `HttpServerHandler`（端口 30001）。
- 路由（`HttpServerHandler.java`）：
  - `POST /sql/v1` → `RestSqlExecutor`（受 `ENABLE_REST_SQL_API` 开关控制）：解析 `RequestData{sqls, params, metricTags}`，**每请求新建一个 `SqlExecutor`**，顺序执行 SQL，返回 `ExecuteDataList`；
  - `POST /api/v1/<name>` → `RestFunctionExecutor`：按 API 名查 `SqlApi` → `CompileManager.getApiBindSqlFunction()` 编译/取缓存函数 → bind 执行；
  - `GET /metrics` → Prometheus scrape；
  - `/ui/*` → UI 静态资源与 UI API。
- 每个请求记录 micrometer timer/counter（按 path/method/status 打标）。

### 3.4 CLI

`Cli` 支持 `-e`（执行语句）、`-f`（执行文件）、交互式 REPL（JLine3，`SqlLineParser` 实现多行 SQL 拼接、`SqlHighlighter` 语法高亮、`SqlOutputFormatter` 表格输出）。

## 4. SQL 语言层（sqlrec-sql-parser）

### 4.1 扩展机制

复用 Flink parser 的 codegen 流水线：`config.fmpp` + `Parser.tdd`（声明生成类为 `FlinkSqlParserImpl`，import `com.sqlrec.sql.parser.*`）+ `parserImpls.ftl`（语法产生式模板）+ `compoundIdentifier.ftl`/`sqlRecIdentifier.ftl`。扩展关键字：`FLUSH`、`ASSERT`、`SERVICE`、`MODEL`、`TRAIN`、`EXPORT`、`CHECKPOINT`、`CACHE`、`PARTITION`、`LIKE`、`FUNCTION`、`API` 等。

### 4.2 扩展语句一览

| 语句 | AST 类 | 语义 |
| --- | --- | --- |
| `CREATE MODEL ... / DROP MODEL` | `SqlCreateModel` / `SqlDropModel` | 定义模型（输入输出字段、算法参数、路径） |
| `TRAIN MODEL m CHECKPOINT c FROM table WHERE ...` | `SqlTrainModel` | 触发 K8s 训练 Job |
| `EXPORT MODEL m CHECKPOINT = c [ON table] [WHERE ...]` | `SqlExportModel` | 从训练 checkpoint 触发导出 Job |
| `ALTER MODEL m DROP CHECKPOINT c` | `SqlAlterModelDropCheckpoint` | 清理 checkpoint |
| `CREATE SERVICE s ON MODEL m [CHECKPOINT = c]` | `SqlCreateService` | 部署在线推理 Deployment |
| `CREATE SQL FUNCTION f ...` | `SqlCreateSqlFunction` | 开始多语句函数定义 |
| `DEFINE INPUT TABLE t LIKE other / (col type...)` | `SqlDefineInputTable` | 声明函数入参表 schema |
| `RETURN [t / SELECT ... / CALL ...]` | `SqlReturn` | 返回空值、缓存表、查询或同步函数调用；顶层 RETURN 结束函数定义，IF 内 RETURN 可提前结束执行 |
| `CALL f(t1, t2) [LIKE ...] [ASYNC] [PARTITION BY t SIZE n]` | `SqlCallSqlFunction` | 调用 SQL/Java 函数，支持异步与分区并行 |
| `CACHE TABLE t AS SELECT ... / CALL ...` | `SqlCache` | 物化缓存表 |
| `IF [TIMEIN] (cond) THEN (stmt) [ELSE (stmt)]` | `SqlIfCache` | 条件执行 CACHE、CRUD、ASSERT、CALL、SET 或 RETURN；TIMEIN 为正超时值时在超时/异常后回退 ELSE |
| `ASSERT SELECT ...` | `SqlAssert` | 非空断言（数据质量门禁） |
| `CREATE API a WITH f` | `SqlCreateApi` | 把函数发布为 REST API |
| `FLUSH` | `SqlFlush` | 失效全部缓存 |

所有 AST 节点实现 `unparse`（经 `SqlUnparseUtils`），支持 `SHOW CREATE` 回显与规范化存储。

### 4.3 预处理（SqlPreProcesser）

`SqlPreProcesser.java`（`sqlrec-core/src/main/java/com/sqlrec/compiler/SqlPreProcesser.java`）在解析前做方言兼容：`use default` 大小写、`set k=v` → `set 'k'='v'` 等。

## 5. 编译子系统（sqlrec-core/compiler）

### 5.1 CompileManager（编译入口与分发）

`CompileManager.java`（`sqlrec-core/src/main/java/com/sqlrec/compiler/CompileManager.java`）：

- `parseFlinkSql()`：预处理 + Flink conformance 解析（Lex.JAVA）。
- `compileSql()`：先 `SqlTypeChecker.isFlinkSqlCompilable()` 校验，再按 AST 类型分发到 6 类 Bindable 构造器（见 §6）。
- **SQL 函数缓存**：`static ConcurrentHashMap<String, SqlFunctionBindable> functionBindableMap`（进程级共享）；`Caffeine sqlApiCache` 缓存 API 名 → 函数名映射（TTL = `SCHEMA_CACHE_EXPIRE`，默认 60s）。
- **循环依赖检测**：实例字段 `compilingSqlFunctions`（ArrayList 链）记录编译栈，`compileSqlFunction()` 递归编译时检测环。
- `getApiBindSqlFunction()`：API 名 → SqlApi → 函数名 → `getSqlFunction()`（缓存命中或编译）。

### 5.2 FunctionCompiler（多语句函数编译状态机）

`FunctionCompiler.java`（`sqlrec-core/src/main/java/com/sqlrec/compiler/FunctionCompiler.java`）。SQL 函数以**多条独立语句**的形式在会话中逐条提交，状态机驱动：

```
FUNCTION_DEFINITION (CREATE SQL FUNCTION f)
   → FUNCTION_PARAM (0..n 条 DEFINE INPUT TABLE)
   → FUNCTION_BODY (任意 SQL / CACHE / CALL / IF，包括 IF 内提前 RETURN)
   → FUNCTION_RETURN (顶层 RETURN / RETURN t / RETURN SELECT / RETURN CALL)
   → isFunctionCompileFinish
```

- 输入表若无 `LIKE`，用占位 `CacheTable(enumerable=null)` 注册进临时 schema，使函数体 SQL 能通过校验；
- 函数体内 `CACHE` 语句的表同样以占位 CacheTable 注册（同名重复时以序号命名 proxyBindable）；
- IF 内 RETURN 参与返回模式校验但不推进状态机；只有顶层 RETURN 才结束函数定义，且所有返回点必须同为空返回或具有相同 schema；
- 完成后由 `SqlExecutor.saveSqlFunction()` 持久化（JSON 语句列表）到元数据库，并 `CacheManager.invalidateAll()` 立即驱逐旧编译产物。

### 5.3 NormalSqlCompiler（Calcite 编译全流程）

`NormalSqlCompiler.java`（`sqlrec-core/src/main/java/com/sqlrec/compiler/NormalSqlCompiler.java`）：

1. Calcite parser（Lex.MYSQL、DEFAULT conformance）重新解析（因为 Flink AST 与 Calcite AST 不完全兼容，靠 unparse 成 SQL 字符串再解析）；
2. `RootFirstCatalogReader`（自研，root schema 优先查找）+ `SqlValidator` 校验；
3. `SqlTypeChecker.isSqlContainKvTable()` 判断是否含 KV 表（决定是否加载 KV 专用规则）；
4. `RuleManager.createPlanner()` 创建 VolcanoPlanner；`SqlToRelConverter`（trim unused fields、expand、inSubQueryThreshold）转换；
5. `Programs.sequence(subQuery, 自定义 program, calc)` 优化（`getProgram()`）；
6. `EnumerableRelImplementor.implementRoot()` 生成 Java 表达式树；
7. `EnumerableInterpretable.toBindable()`（Janino 编译）产出 `CalciteBindable`，同时保留 logical plan / physical plan / java 源码用于日志与调试。

### 5.4 SqlTypeChecker（可编译性判定）

`SqlTypeChecker.java`（`sqlrec-core/src/main/java/com/sqlrec/compiler/SqlTypeChecker.java`）：递归判定语句是否可在本地 Calcite 执行——顶层必须是 SELECT/INSERT/UPDATE/DELETE/ORDER BY/UNION（或控制流节点递归判定），且引用的所有表解析为 `SqlRecTable`（connector 提供的 Calcite 表）。该判定同时决定"本地执行 vs 转发 Flink Gateway"。

### 5.5 FunctionUpdater（函数热更新）

`FunctionUpdater.java`（`sqlrec-core/src/main/java/com/sqlrec/compiler/FunctionUpdater.java`）：单线程 scheduled executor（默认 300s 周期），对 `functionBindableMap` 中每个函数做依赖图遍历（`inProgress` 集合防环）：

- 函数在 DB 中 `updatedAt > bindable.createTime` → 重编译；
- 依赖函数被更新/删除 → 级联重编译；函数在 DB 中已删除 → 从缓存移除；
- 依赖表（减去函数入参占位表）的 HMS 更新时间晚于 `createTime` → 重编译；
- 依赖 Java UDF 类名变更/缺失 → 重编译（todo：无法感知同类名字节码变更）。

## 6. 执行子系统（executor + runtime）

### 6.1 SqlExecutor（语句分发中枢）

`SqlExecutor.java`（`sqlrec-core/src/main/java/com/sqlrec/executor/SqlExecutor.java`）。每会话/每 REST 请求一个实例，持有：

- `schema`：启动时 `CalciteSchemaFactory.createCalciteSchema()` 构建；
- `context`：`ExecuteContextImpl`（变量表、metrics 标签、函数调用栈、取消位）；
- `defaultSchema`：`USE DATABASE` 可变；
- `functionCompiler`：函数多语句编译的会话内状态。

`executeSqlAsync()` 分发顺序：函数编译状态机 → `USE` → `FLUSH`（全缓存失效）→ 资源查询（SHOW/DESC 族）→ 可编译 CRUD（编译 + bind + 组装 `SqlProcessResult`）→ 资源编辑（CREATE/DROP/TRAIN/EXPORT MODEL、SERVICE、API、SQL FUNCTION，编辑后 `CacheManager.invalidateAll()`）→ 兜底返回 null（由 frontend 转发 Flink Gateway）。

`executeSql()`：同步包装，轮询 `result.isCompleted()` + `SQL_SYNC_EXECUTE_TIMEOUT`（默认 180s）超时。

### 6.2 Bindable 体系（运行时执行树）

所有可执行单元实现 `BindableInterface`（`bind(schema, context)` 返回 `Enumerable<Object[]>`，暴露返回字段/读写表/缓存表名/依赖函数等元信息）：

```
BindableInterface
 ├─ CalciteBindable        普通 SQL：包装 Calcite Bindable；bind() 时创建 SqlRecDataContextImpl，
 │                          执行并把结果全量物化为 List（列宽自适应单列/多列）
 ├─ CacheTableBindable     CACHE 语句：执行子 Bindable → 结果注册为 schema 中的 CacheTable；
 │                          返回 (table_name, count) 一行
 ├─ FunctionProxyBindable  CALL 语句代理：静态绑定（delegate）或运行时按变量解析函数名；
 │                          支持 ASYNC（提交虚拟线程后返回 null）与 PARTITION BY t SIZE n
 │                          （按分区表行数切分，clone context，每分区独立 schema，并发执行合并）
 ├─ SqlFunctionBindable    SQL 函数本体：按依赖串行/并行执行节点；RETURN 后跳过尚未执行的节点
 ├─ ReturnBindable         RETURN 语句：返回缓存表或委托 SELECT/CALL，并写入当前函数帧的 ReturnState
 ├─ ProxyAllBindable       节点执行包装：超时、取消、Trace、Metrics 和日志；
 │                          根据 isIgnoreException() 和缓存表元数据统一恢复可忽略异常并计数
 ├─ CallSqlFunctionBindable 函数调用绑定（校验输入表存在性）
 ├─ JavaFunctionBindable   Java table UDF 绑定
 ├─ IfBindable             条件分支：条件是单行单列 SELECT；支持 CACHE、CRUD、CALL、SET、ASSERT、RETURN；
 │                          TIMEIN 在隔离 ReturnState 中执行 THEN，超时/异常时丢弃临时结果并回退 ELSE
 ├─ AssertBindable         断言：SELECT 结果为空则抛异常
 └─ SetBindable            变量设置（context.setVariable）
```

**上下文传播**：`ExecuteContextImpl.clone()` 共享 variableMap/metricsTagMap（父子双向可见），复制 funNameStack（环检测），持有 parent 引用实现取消位沿父链传播（`isCancelled()` 向上遍历）。

**并行执行**：`ExecutorServiceUtils` 提供**全局虚拟线程池**（`newVirtualThreadPerTaskExecutor`），用于 CACHE 超时控制、ASYNC CALL、分区并行。

### 6.3 执行结果与 CacheTable

- `SqlProcessResult{enumerable, fields}` + `isCompleted()`；
- `CacheTable`（common）= `ScannableTable` + 内存 `Enumerable` + 字段列表 + createSql；注册进会话 schema 后，后续 SQL 即可直接 `SELECT * FROM cache_table` 引用；
- `FLUSH` / 任何资源编辑 / 函数保存 → `CacheManager.invalidateAll()` 统一失效四大缓存（CalciteSchemaFactory、JavaFunctionUtils、CompileManager、ServiceManager）。

## 7. Calcite 集成（rules + node）

### 7.1 优化规则

`RuleManager.java`（`sqlrec-core/src/main/java/com/sqlrec/rules/RuleManager.java`）`createPlanner(boolean addKvTableRules)`：

- 注册默认 trait（Convention，可选 Collation）+ Calcite 默认规则（禁 interpreter）；
- **总是**替换：`ENUMERABLE_TABLE_MODIFICATION_RULE → SqlRecTableModifyRule`（写路径走自研 modify 节点）、`ENUMERABLE_UNION_RULE → SqlRecUnionRule`；
- **含 KV 表时**追加：`FILTER_SCAN/FILTER_INTERPRETER_SCAN`（谓词下推到表扫描）、`FILTER_INTO_JOIN`、`KV_JOIN`（替换 EnumerableJoin/MergeJoin，并移除 JOIN_COMMUTE 等干扰规则）、`VECTOR_JOIN`（带/不带 filter 两个变体）；
- connector 可通过 `HmsTableFactory.getRules()` 注入自定义规则（`addTableFactoryRules`）。

### 7.2 物理算子（node 包）

| 算子 | 说明 |
| --- | --- |
| `FilterableTableScan` | 携带下推谓词的表扫描（KV/向量表的过滤入口） |
| `SqlrecEnumerableKvJoin` | KV join：左表全量收集 join key → 右表 `SqlRecKvTable.getByPrimaryKey()` 批量点查（或非主键时逐 key 带 filter scan）→ 字符串 key map 合并（`MergeUtils.snakeMerge`）；支持 LEFT join 补 null |
| `SqlrecEnumerableVectorLookupJoin` | 双输入向量 lookup join：遍历左表，使用右侧 `VectorSearchable` 表执行带过滤条件的 ANN 检索（`DEFAULT_VECTOR_SEARCH_LIMIT` 默认 100） |
| `SqlrecEnumerableUnion` | union 合并（配合 `IGNORE_UNION_EXCEPTION` 分支降级） |
| `SqlrecEnumerableTableModify` | INSERT/UPDATE/DELETE 写路径：分发到 connector 的写接口（批量 upsert/delete） |

### 7.3 KV Join 数据流（KvJoinUtils）

`KvJoinUtils.kvJoin()`（`sqlrec-core/src/main/java/com/sqlrec/utils/KvJoinUtils.java`）：

1. 左表物化为 List，提取 join key 集合（null 跳过）；
2. join key 列 = 右表主键 → `getByPrimaryKey(joinKeys)` 一次批量点查；否则**逐 key** 构造 `EQUALS` filter 调 `scan(filter)`（N+1 访问模式）；
3. 右表结果按 `key.toString()` 建 HashMap（规避 Integer/Long equals 语义差异，但引入跨类型字符串碰撞）；
4. 按 join 类型拼装（LEFT 无匹配补 null），`snakeMerge` 合并输出。

## 8. 元数据子系统（db + schema）

### 8.1 双模式架构

`MetadataAccessFactory.java`（`sqlrec-core/src/main/java/com/sqlrec/db/MetadataAccessFactory.java`）按 `SQL_SCHEMA_DIR` 是否为空选择：

| | 本地文件模式 | 远程模式 |
| --- | --- | --- |
| SchemaAccess | `SqlFileSchemaAccess`（`SqlFileParser` 解析目录下 SQL 文件中的建表/UDF） | `HmsSchemaAccess`（thrift 访问 HMS） |
| StoreAccess | `InMemoryStoreAccess`（函数/API/模型节点内存态；启动时回放 service 节点） | `DbStoreAccess`（MyBatis + PostgreSQL） |
| HdfsAccess | `LocalHdfsAccess`（本地目录） | `RemoteHdfsAccess`（Hadoop client） |

`MetadataAccess = SchemaAccess + StoreAccess + HdfsAccess` 聚合门面，全静态单例（DCL）。

### 8.2 HmsClient

`HmsClient.java`（`sqlrec-core/src/main/java/com/sqlrec/db/remote/HmsClient.java`）：`HiveMetaStoreClient` 静态缓存（DCL + volatile + JVM shutdown hook）；`withRetry()` 对 `TTransportException` 失效重建客户端后重试一次（读幂等）；**所有公开方法 `synchronized static`** —— HMS 访问进程内全局串行。

### 8.3 Schema 组装与缓存

- `CalciteSchemaFactory.java`（`sqlrec-core/src/main/java/com/sqlrec/schema/CalciteSchemaFactory.java`）：`createCalciteSchema()` 构建根 schema：远程模式下按 `databaseListCache`（`ObjCache`，TTL 60s，异步刷新）为每个库挂 `HmsSchema`；`globalSchema`（由 server 启动时设置）存在时直接复用其 subSchema 映射（快速路径）。
- `HmsSchema.java`（`sqlrec-core/src/main/java/com/sqlrec/schema/HmsSchema.java`）：per-db 两级 `ObjCache`——`tableMapCache`（增量：仅 `getTableUpdateTime` 变化的表重建 `Table` 对象，未变沿用旧实例）与 `functionMapCache`（HMS 函数 + `FunctionConfigs.DEFAULT_SCALAR_FUNCTION_CONFIGS` 内置 UDF → `UdfManager.createScalarFunction` 反射）。
- **`ObjCache`**（`sqlrec-core/src/main/java/com/sqlrec/utils/ObjCache.java`）：TTL + 可选异步刷新的极简缓存。`getObj()` 失效即 `updateObj()`（synchronized）：首载同步；之后提交到**全局单线程** executor 异步刷新并立即返回旧值；`invalidate()` 清空。

### 8.4 表抽象体系（common/schema）

```
AbstractTable
 └─ SqlRecTable (name, createSql)
     ├─ CacheTable            内存结果表（ScannableTable）
     ├─ SqlRecKvTable         KV 表抽象：getByPrimaryKey(Set)/scan(filters) —— Redis/JDBC/Mongo 实现
     │                          （按 connector 配置决定点查 vs 全扫）
     └─ (实现 VectorSearchable) 向量检索表：Milvus
```

`HmsTableFactory`（SPI，`META-INF/services` 注册）：HMS 表的 storage handler / serde / 参数 → 对应 connector 的 Calcite Table + 可选自定义优化规则 + Flink DynamicTableFactory。connector 通过 SPI 被发现（`TableFactoryUtils`）。

## 9. 存储连接层（sqlrec-connectors）

六个 connector 结构一致：`config/`（Options 解析表属性）+ `calcite/`（CalciteTable + Factory）+ `handler/`（存储访问）+（redis/milvus）`flink/`（Flink DynamicTable Source/Sink）。

| Connector | 读路径 | 写路径 | 要点 |
| --- | --- | --- | --- |
| **redis** | `RedisCalciteTable`（KV 表）：主键 `get/mget/lrange`；非主键全扫过滤 | `RedisSinkTableFunction`（lpush/setex 等，codec：String/Json） | `RedisWrapper`/`RedisClusterWrapper` 静态连接池（按 URL）；pipeline 用全局锁串行（autoFlush 是连接级全局）；shutdown hook 释放；Flink 侧另有 Lookup 源 |
| **jdbc** | `JdbcCalciteTable`：`JdbcHandler.scan(filters)` 生成带 where 的 select | upsert/delete（含 `upsertBatch`/`deleteBatch`） | HikariCP 池（key= url+user+driver+schema+password，凭据轮换自动驱逐旧池）；主键点查走 IN 批量 |
| **mongodb** | `MongoCalciteTable`（KV 表） | 同 | `MongoHandler` |
| **milvus** | `MilvusCalciteTable` 实现 `VectorSearchable`：ANN 检索 | `MilvusDynamicTableSink`（Flink 写入） | 向量 join 的右表；Flink DynamicTableFactory 双栈 |
| **kafka** | `KafkaCalciteTable` | — | 流表（配合 Flink Gateway 的 DDL） |
| **filesystem** | `FileSystemCalciteTable` | truncate | 本地/HDFS 文件表 |

**Guava 版本对齐**：根 pom 统一 32.1.3（hive-metastore 带 19.0 与 milvus/grpc 冲突）。

## 10. UDF 子系统（sqlrec-udf + core/udf）

### 10.1 注册与加载

- **内置 UDF**：`FunctionConfigs.DEFAULT_SCALAR_FUNCTION_CONFIGS` 静态注册 scalar 函数名 → 类名；table 函数经 `JavaFunctionUtils.getTableFunction()` 查找（HMS 注册的 Java 函数或内置）。
- **动态 Java UDF**：`UdfManager`/`JavaFunctionUtils` 从 HMS function 注册表按 className 反射加载，`isJavaFunctionModifiedSince` 支撑热更新（类名级）。

### 10.2 HTTP 类 UDF（在线推理核心）

- `CallServiceFunction.java`（`sqlrec-udf/src/main/java/com/sqlrec/udf/table/CallServiceFunction.java`）（`call_service`）：`ServiceManager.getServiceConfig()` 取服务 URL（ObjCache 缓存）→ 输入表序列化为 JSON 数组 → POST → 解析预测 map → `mergePredictions` 按列名拼回输入行。静态 `OkHttpClient`（30s 三段超时，volatile 可测试替换）。
- `CallServiceWithQVFunction`、`BatchCallServiceUDTF`（Flink UDTF，JDK HttpURLConnection，按 batchSize 攒批）、`CallSqlRecApiFunction`（跨 sqlrec 实例调用，走 `SqlRecApiClient`）。
- **多样化族**：`WindowDiversify`（滑动窗口内按 tag 配额）、`DppDiversity`（行列式点过程）、`RuleDiversity`、`WeightedMerge`、`DedupFunction`、`ShuffleFunction`。
- **特征族**：`TagToVecFunction`、`RandomVecFunction`、`FeatureCoverageMetricsFunction`、`GetGrowthbookFeaturesFunction`（GrowthBook SDK）、`Get/SetVariablesFunction`（变量传播）。
- 其他：`JsonToTableFunction`、`AddColFunction`、`TruncateTableFunction`、`SleepFunction`（测试用）。

## 11. 模型子系统（sqlrec-model + core/model + k8s）

### 11.1 生命周期状态机

```
CREATE MODEL ──► 校验(ModelController.checkModel: 字段/参数/输入输出重名)
                     │
TRAIN MODEL ──► (同 checkpoint 且 CREATED → 幂等返回已有; 否则先删旧)
                genModelTrainK8sYaml → injectPodConfig(params)
                → insertCheckpoint(status=CREATED) → K8sManager.applyYaml (训练 Job)
                     │  (客户端轮询)
isCheckpointOperationCompleted:
   checkJobsStatusFromYaml → succeeded → status=SUCCEEDED, deleteYaml(Job)
                          → failed    → status=FAILED
                     │
EXPORT MODEL ─► genModelExportK8sYaml (导出 Job: 训练 checkpoint → serving 格式)
                → export checkpoint(status=CREATED→…→SUCCEEDED)
                     │
CREATE SERVICE ► 校验 checkpoint=SUCCEEDED + 类型合法
                → getServiceK8sYaml (ConfigMap+Service+Deployment) → applyYaml
                → isDeploymentReadyFromYaml (Ready 副本校验)
                     │
在线推理  ────► call_service('service_name', input_table) UDF → HTTP POST
```

- **Checkpoint 保护**：被 Service 引用的 checkpoint/模型禁止删除（`deleteCheckpoint`/`deleteModel` 前置校验）。
- **路径安全**：`PathUtils.validateModelPath` 防越界删除。

### 11.2 ModelController SPI 与后端

`ModelController`（common，SPI：`META-INF/services`）由 `ModelControllerFactory` 按 `engine` 分发：

| 后端 | 实现 | 训练 | Serving |
| --- | --- | --- | --- |
| xgboost / lightgbm / catboost | `XGBoostModel`/`LightGBMModel`/`CatBoostModel`（继承 `GbdtModelBase`） | K8s Job 跑 Python（`train_*.py`） | `onnx_server.cpp` / `catboost_server.cpp`（cpp-httplib） |
| dssm / wide_and_deep | `DSSMModel`/`WideAndDeepModel`（继承 `TzrecModelBase`） | K8s Job 跑 tzrec（`server.py`） | `tzrec/server.cpp` |
| external | `ExternalModel` | — | 直接透传外部 URL |

`K8sYamlBuilder`（model/common）统一生成 ConfigMap/Service/Deployment（容器端口、资源 request/limit、副本数、`REPLICAS`/`LABEL_COLUMNS` 等 env）；`GbdtK8sYamlUtils`/`TzrecK8sYamlUtils` 生成训练/导出 Job YAML（含 HDFS 挂载、Python 镜像）。

### 11.3 K8sManager

`K8sManager.java`（`sqlrec-core/src/main/java/com/sqlrec/k8s/K8sManager.java`）：fabric8 client 静态缓存（DCL + shutdown hook）；`applyYaml`（serverSideApply）、`deleteYaml`（先查存在再删，支持 Deployment/Service/ConfigMap/Secret/Pod/Job）、`checkJobsStatusFromYaml`、`isDeploymentReadyFromYaml`；传输级/5xx 失败自动 `resetClient()` 重建（4xx 业务错误不重建）。

## 12. Demo 与编程模型（sqlrec-demo）

`src/main/sql/quick_start/` 提供基于 filesystem connector 内存表的无外部依赖体验链路，其全局对象统一使用 `demo_` 前缀（`demo_user_interest_category`、`demo_category_hot_item`、`demo_exposure_item`、`demo_rec`）；MovieLens 完整推荐链路位于 `src/main/sql/movielens/`：

1. **表**（table/）：`user_table`/`item_table`/`item_embedding`/`rec_log_kafka`（流式曝光日志）等；
2. **特征**（function/）：`recall_fun`（多路召回：`itemcf_i2i` + `genre_hot_item` + `user_interest_genre`…）、`rank_fun`（join 特征 → `call_service('rank_service')`）、`diversify_fun`（`window_diversify`）、`main_rec`（编排：`get_or_default` 按变量选择分支 → 召回 → 排序 → 多样化 → 异步 `save_rec_item` 落盘）；
3. **模型**（model/）：`recall_model`（DSSM 双塔）、`rank_model`（GBDT）；
4. **服务**（service/）：`recall_service_user/item`、`rank_service`（绑定导出 checkpoint 部署）；
5. **API**（api/）：`main_rec` 发布为 REST API。

编程模型要点：`IF` 表达条件执行，`IF TIMEIN` 在超时值大于 0 时为 THEN 设置毫秒级超时并在失败时回退 ELSE，`IF + RETURN` 支持函数提前返回；`CALL ... ASYNC` 表达旁路落盘；`ASSERT` 做数据门禁；变量（`get_or_default`/`SET`）做请求级参数传递。

## 13. 可观测性

- **Metrics**：Micrometer 复合 registry + Prometheus `/metrics`（HTTP 请求 timer/counter、会话/操作 gauge、操作开关 counter、cache-table 忽略异常 counter、函数更新 timer 等）；deploy/prometheus 提供 ServiceMonitor + Grafana JSON。
- **Trace**：OpenTelemetry（OTLP gRPC → Jaeger），`DEBUG_TRACE` 开关 + `TRACE_ENDPOINT/HEADERS/SERVICE_NAME`；`TraceUtils` 在节点执行处埋点。
- **日志**：log4j2；编译流程 INFO 级打印完整 logical/physical plan 与生成的 Java 源码。

## 14. 线程模型与并发设计汇总

| 线程资源 | 类型 | 用途 |
| --- | --- | --- |
| `ExecutorServiceUtils` 虚拟线程池 | 全局、无界 | CACHE 超时执行、ASYNC CALL、PARTITION 并行 |
| `ObjCache.executorService` | 全局、单线程 | 所有 ObjCache 的异步刷新（共享） |
| `FunctionUpdater.executor` | 单线程 scheduled | 函数热更新检查（300s） |
| `SessionTimeoutChecker.timeoutChecker` | 单线程 scheduled | 会话超时（5min 检查） |
| Thrift `TThreadPoolServer` | 每连接一平台线程 | RPC 处理（含同步 SQL 执行） |
| Netty event loop | boss(1)+worker(默认) | REST 处理（**业务在 event loop 上同步执行**） |
| 各静态客户端 | — | HmsClient（全局 synchronized）、K8sManager、RedisWrapper/JdbcHandler 连接池 |

静态可变状态：`CompileManager.functionBindableMap/sqlApiCache`、`CalciteSchemaFactory.schemaMap/globalSchema`、`ServiceManager.serviceConfigCacheMap`、各 connector 静态连接池、`HmsClient.client`、`K8sManager.kubernetesClient`。

## 15. 部署架构（deploy/ + bin/ + docker/）

- `deploy_minikube.sh` + `deploy_components.sh`：minikube 一键全家桶（HMS、PostgreSQL、Redis(单机/集群)、Milvus、MongoDB、Kafka、HDFS、Flink SQL Gateway、Kyuubi、GrowthBook、Jaeger、Prometheus/Grafana、DolphinScheduler、ClickHouse、OpenSearch、Jupyter…），每组件独立 `deploy.sh/uninstall.sh` + PVC。
- `deploy/sqlrec/sqlrec.yaml`：sqlrec 引擎 Deployment（镜像由 `build_sqlrec_docker.sh` 构建，Dockerfile 基于发行包）。
- `bin/`：`beeline.sh`（Thrift 客户端）、`kyuubi.sh`、`server.sh`、`cli.sh`、`sqlrec.cmd`。
- `benchmark/`：movielens（端到端召回链路 + wrk 压测 request.lua）与 criteo1m 两套基准。

## 附录 A：关键配置项（SqlRecConfigs）

| 配置 | 默认 | 说明 |
| --- | --- | --- |
| `THRIFT_SERVER_PORT` / `REST_SERVER_PORT` | 30000 / 30001 | 服务端口 |
| `REST_BUSINESS_EXECUTOR_THREADS` | 16 | REST 业务执行线程数 |
| `REST_BUSINESS_MAX_PENDING_TASKS` | 32 | 每个 REST 业务 EventExecutor 的最大排队任务数，最小为 16 |
| `ENABLE_REST_SERVER/SQL_API/UI_API/THRIFT_SERVER` | true | 入口开关 |
| `PARALLELISM_EXEC` | true | 并行执行 |
| `NODE_EXEC_TIMEOUT` | 0（不限） | 可超时节点的执行超时（ms） |
| `FUNCTION_UPDATE_INTERVAL` | 300s | 函数热更新周期 |
| `SCHEMA_CACHE_EXPIRE` / `ASYNC_SCHEMA_UPDATE` | 60s / true | schema 缓存 |
| `SQL_SCHEMA_DIR` | 空（用 HMS） | 本地文件元数据模式 |
| `META_DB_URL/USER/PASSWORD/DRIVER` | PG | 元数据库 |
| `HIVE_METASTORE_URI` | thrift://...:30008 | HMS |
| `FLINK_SQL_GATEWAY_ADDRESS/PORT` | .../30018 | 远端 Flink Gateway |
| `SESSION_CHECK_INTERVAL` / `SESSION_IDLE_TIMEOUT` | 5min / 30min | 会话管理 |
| `SQL_SYNC_EXECUTE_TIMEOUT` | 180s | 同步执行超时 |
| `IGNORE_UNION_EXCEPTION` / `IGNORE_JOIN_QUERY_EXCEPTION` | true | 降级开关 |
| `DEFAULT_VECTOR_SEARCH_LIMIT` | 100 | 向量检索默认 topK |
