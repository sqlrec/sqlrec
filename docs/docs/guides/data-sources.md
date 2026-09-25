# Connector 基础概念

Connector 把 Redis、Milvus、JDBC 数据库等外部存储映射为 SQLRec 中的表。使用时只需要在 `CREATE TABLE ... WITH (...)` 中选择 Connector 并填写连接参数。

```sql
CREATE TABLE user_profile (
  user_id BIGINT,
  country STRING,
  age INT,
  PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
  'connector' = 'redis',
  'url' = 'redis://localhost:6379/0'
);
```

## 如何选择

| Connector | 主要用途 | 查询 | 写入 | 数据持久化 | 特殊能力 |
|-----------|----------|------|------|------------|----------|
| Redis | 在线特征、召回结果、曝光记录 | 是 | 是 | 是 | 主键查询、本地缓存 |
| Milvus | 向量召回 | 是 | 是 | 是 | 向量近邻检索 |
| JDBC | PostgreSQL、MySQL 等关系数据库 | 是 | 是 | 是 | 过滤下推、连接池 |
| MongoDB | 文档数据 | 是 | 是 | 是 | 复杂过滤下推 |
| Kafka | 推荐日志、行为事件 | 否 | 是 | 由 Kafka 保证 | 异步消息写入 |
| Filesystem | Demo、测试、小型静态数据 | 是 | 仅内存 | 否 | CSV/JSON 初始数据 |

Filesystem Connector 的 `INSERT`、`UPDATE` 和 `DELETE` 只修改当前 SQLRec 进程中的数据，不会回写文件。因此它适合 Demo 和测试，不适合保存生产数据。

## 建表时需要确认的事情

### 主键

Redis、Milvus、JDBC、MongoDB 和 Filesystem 表通常应声明主键：

```sql
PRIMARY KEY (user_id) NOT ENFORCED
```

SQLRec 可以利用这个字段进行批量查找并优化 Join。`NOT ENFORCED` 表示声明本身不保证数据唯一；同一主键值能否对应多行，取决于 Connector 的存储方式：

- Redis 的 `list` 模式和 Filesystem 将主键当作索引键，同一键下允许多行，包括内容完全相同的行。建表时应选择用于分组查找的字段，例如 `user_id`。
- Redis 的 `json`、`string` 模式每个键只保存一个值，写入相同键会覆盖原值。
- 其他 Connector 的唯一性和写入行为由对应实现及底层存储决定，请查看各自的[连接器说明](../reference/connectors/builtin-connectors.md)。

因此，声明 `PRIMARY KEY` 不等于要求每行都有唯一的键；对于支持多行的模式，它只用于定位一组记录。

### 数据类型

表字段类型必须能与数据源中的真实值互相转换。优先使用明确的 `BIGINT`、`DOUBLE`、`VARCHAR` 和 `ARRAY<FLOAT>` 等类型，避免依赖隐式转换。

### 连接地址

文档示例中的 `localhost` 只适用于数据源与 SQLRec 在同一网络环境的情况。在 Kubernetes 中通常应使用 Service DNS 名称或集群内可达地址。

## 查询和写入

Connector 表在 SQL 中的用法与普通表相同：

```sql
SELECT * FROM user_profile WHERE user_id = 1001;

INSERT INTO user_profile VALUES (1001, 'CN', 25);
```

但不同 Connector 的能力不同。例如 Kafka 只用于写入，Redis 更适合主键查询，Milvus 的向量检索需要通过特定 Join 形式触发。使用前请查看[内置 Connector](../reference/connectors/builtin-connectors.md) 中对应的限制。

## 下一步

- [内置 Connector](../reference/connectors/builtin-connectors.md)：配置参数、建表示例和使用限制。
- [自定义 Connector](../development/custom-connector.md)：实现并注册新的数据源适配。
