# SQLRec Connector 系统

本文档介绍 SQLRec Connector 的整体架构、表类型体系以及连接器的加载和执行流程。

## 概述

Connector 将外部存储系统映射为 SQLRec 中可查询或可写入的表。用户通过 `CREATE TABLE ... WITH ('connector' = '...')` 声明数据源；SQLRec 从 Hive Metastore 读取表定义，找到对应的 `HmsTableFactory`，再创建 Calcite `Table` 参与 SQL 规划与执行。

### 核心概念

| 概念 | 说明 |
|------|------|
| **Connector** | 对一种外部存储系统的适配实现，例如 Redis、Milvus 或 JDBC |
| **Table** | Connector 在 Calcite 中的表抽象，定义表结构以及查询、写入等能力 |
| **`HmsTableFactory`** | Calcite 侧的工厂 SPI，根据 Metastore 表定义创建 `Table`，并可提供优化规则 |
| **`SqlRecTable`** | 所有 SQLRec Calcite 表的抽象基类 |
| **`SqlRecKvTable`** | 面向键值访问的表基类，提供主键查询、过滤、修改和可选的本地缓存 |
| **`VectorSearchable`** | 可选的向量检索接口，由需要向量相似度搜索的表实现 |

### 从 DDL 到执行

```text
CREATE TABLE ... WITH ('connector' = 'redis')
                    │
                    ▼
          Hive Metastore 表定义
                    │
                    ▼
     ServiceLoader 加载 HmsTableFactory
                    │
                    ▼
        创建并缓存 Calcite Table
                    │
          ┌─────────┴─────────┐
          ▼                   ▼
      SQL 查询/过滤        INSERT/UPDATE/DELETE
```

`connector` 属性是工厂选择的关键。工厂通过 `getConnectorName()` 声明标识符，通过 `getTableFromHmsTable()` 把表定义转换为 Calcite `Table`。表对象由 SQLRec 统一管理并在查询间共享，因此实现必须保证线程安全。

## 表类型体系

```text
Calcite AbstractTable
        │
        ▼
SqlRecTable
        │
        ├── Connector 自定义表（按需实现 Calcite 能力接口）
        │
        └── SqlRecKvTable
                │
                └── 可选实现 VectorSearchable
```

### `SqlRecTable`

`SqlRecTable` 继承 Calcite 的 `AbstractTable`，保存表名和建表语句，是 SQLRec 表实现的最小公共基类。Connector 可以按需实现 Calcite 的能力接口，例如使用 `ModifiableTable` 支持写入。

适用于不需要通用主键查询与缓存能力的表。例如 Kafka Connector 直接继承 `SqlRecTable`，并实现 `ModifiableTable`。

### `SqlRecKvTable`

`SqlRecKvTable` 继承 `SqlRecTable`，同时实现 `FilterableTable` 和 `ModifiableTable`，统一提供以下能力：

- 从过滤条件中识别主键查询，并调用 `getByPrimaryKeyImpl()` 批量读取
- 通过 `initCache()` 启用可选的 Caffeine 本地缓存
- 通过 `scanImpl()` 执行非主键扫描或下推后的过滤查询
- 在查询边界完成主键和行数据的 SQL 类型转换，并记录指标

子类至少需要定义表结构、主键位置、主键批量读取、扫描和可修改集合。Redis、JDBC、MongoDB、Filesystem 和 Milvus 的 Calcite 表均基于该类型。

### `VectorSearchable`

`VectorSearchable` 是独立的能力接口，不是表基类。需要向量检索的 `SqlRecTable` 子类实现 `searchByEmbeddingImpl()`；接口的默认方法负责结果类型转换和指标记录。当前 Milvus Connector 组合使用 `SqlRecKvTable` 与 `VectorSearchable`。

### 如何选择

| 需求 | 推荐类型 |
|------|----------|
| 只需要基础表能力，或能力模型不适合键值访问 | 继承 `SqlRecTable`，按需实现 Calcite 接口 |
| 需要主键批量查询、过滤、修改或本地缓存 | 继承 `SqlRecKvTable` |
| 在上述任一表类型上增加向量相似度搜索 | 额外实现 `VectorSearchable` |

## Connector 扩展机制

Calcite 侧 Connector 由三个部分组成：

1. 配置层解析 `WITH` 属性并构造 Connector 配置。
2. `HmsTableFactory` 声明 Connector 标识符，将 Metastore 元数据转换为表实例，并返回可选的规划规则。
3. 表与数据访问层实现具体的读写、过滤下推、缓存或向量检索逻辑。

工厂实现通过 Java SPI 文件 `META-INF/services/com.sqlrec.common.schema.HmsTableFactory` 注册。SQLRec 使用 `ServiceLoader` 发现实现，并以 Connector 标识符建立工厂映射。

部分 Connector 还提供 Flink `DynamicTableFactory`，用于需要转交 Flink 执行的场景；它与 Calcite 侧使用相同的 Connector 标识符，但属于独立的运行时适配。

## 下一步

- 查看[内置 Connectors](./builtin_connectors.md)，了解可用数据源、配置项和 SQL 示例。
- 查看[自定义 Connectors](./custom_connectors.md)，了解实现与注册新 Connector 的完整步骤。
