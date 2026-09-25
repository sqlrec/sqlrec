# 内置 Connectors

本文档介绍 SQLRec 内置的数据连接器及其使用方法。

## 概述

SQLRec 提供了多种内置连接器，用于连接不同的数据存储系统。连接器基于 Calcite 表抽象实现，支持 SQL 查询和数据写入操作。

如果还不熟悉数据源的创建和使用方式，请先阅读[接入数据源](../../guides/data-sources.md)。

## 内置连接器

### 1. Redis Connector

Redis 连接器用于连接 Redis 数据库，支持键值存储和查询。

**连接器标识符**：`redis`

**继承类型**：`SqlRecKvTable`

**特性**：
- 支持单机模式和集群模式
- 支持 `json`、`list` 和 `string` 数据结构
- 支持 JSON 和 Protobuf value 格式
- 支持本地缓存加速查询
- 支持主键过滤查询
- 支持 `INSERT`、`UPDATE` 和 `DELETE`

**配置参数**：

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `url` | String | - | Redis 连接 URL，格式：`redis://password@host:port/db` |
| `redis-mode` | String | `single` | Redis 模式，可选值：`single`（单机）、`cluster`（集群） |
| `data-structure` | String | `json` | 数据结构，可选值：`json`、`list`、`string` |
| `format` | String | `json` | Value 格式，可选值：`json`、`protobuf` |
| `protobuf.message-class-name` | String | - | Protobuf generated Message 类全名；`format = protobuf` 时必填 |
| `max-list-size` | Integer | 0 | List 最大长度，0 表示无限制 |
| `ttl` | Integer | 2592000 | Key 过期时间（秒），默认 30 天 |
| `cache-ttl` | Integer | 30 | 本地缓存过期时间（秒），0 表示不缓存 |
| `max-cache-size` | Integer | 100000 | 本地缓存最大条目数 |

**使用示例**：

```sql
CREATE TABLE user_table (
  id BIGINT,
  name STRING,
  country STRING,
  age INT,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'redis',
  'url' = 'redis://localhost:6379/0'
);

CREATE TABLE user_interest_category1 (
  user_id BIGINT,
  category1 STRING,
  score FLOAT,
  PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
  'connector' = 'redis',
  'data-structure' = 'list',
  'url' = 'redis://localhost:6379/0'
);

CREATE TABLE user_proto (
  value STRING,
  PRIMARY KEY (value) NOT ENFORCED
) WITH (
  'connector' = 'redis',
  'url' = 'redis://localhost:6379/0',
  'format' = 'protobuf',
  'protobuf.message-class-name' = 'com.google.protobuf.StringValue'
);
```

**注意事项**：
- 查询必须包含主键等值条件（如 `WHERE user_id = 42`）；取回该键下的数据后，还可按其他列过滤，不支持全表扫描
- 使用本地缓存可以显著提升查询性能
- `list` 模式下，主键是 Redis 列表的索引键，不要求行唯一；同一个键可以保存多条记录。`INSERT` 将新行写到列表头部，`max-list-size` 限制写入后的列表长度
- `list` 模式的 `UPDATE` 按查询条件选中旧行，每条旧行只移除列表中一条完全相同的记录，再将新行写入列表头部；修改主键时，新行写到新键下。旧行已不存在时不会补插新行
- `list` 模式的 `DELETE` 会移除与选中行完全相同的所有列表记录；`UPDATE` 则为每条选中的旧行移除一个匹配项。`UPDATE` 的移除和写入是多条 Redis 命令，不保证原子性
- Protobuf 格式要求 generated Message 类位于 SQLRec 和 Flink 运行时 classpath
- `format = protobuf` 不支持与 `data-structure = string` 组合使用

### 2. Milvus Connector

Milvus 连接器用于连接 Milvus 向量数据库，支持向量相似度检索。

**连接器标识符**：`milvus`

**继承类型**：`SqlRecKvTable`（implements `VectorSearchable`）

**特性**：
- 支持向量相似度搜索（ANN）
- 支持主键查询
- 支持过滤条件
- 支持数据插入和删除
- 支持投影列优化

**配置参数**：

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `url` | String | - | Milvus 服务器地址 |
| `token` | String | - | Milvus 认证令牌 |
| `database` | String | `default` | 数据库名称 |
| `collection` | String | - | 集合名称 |
| `batch-size` | Integer | 4096 | 批量写入（bulk insert）的批次大小 |
| `pool.max-idle-per-key` | Integer | 10 | 连接池中每个 key 的最大空闲连接数 |
| `pool.max-total-per-key` | Integer | 100 | 连接池中每个 key 的最大连接总数 |
| `pool.max-total` | Integer | 100 | 连接池最大连接总数 |
| `pool.max-block-wait-duration` | Long | 5 | 从连接池获取连接的最大阻塞等待时间（秒） |
| `pool.min-evictable-idle-duration` | Long | 10 | 连接池中连接的最小可驱逐空闲时长（秒） |
| `flush-interval` | Long | 1 | 批量写入的刷新间隔（秒），缓冲区满或到达间隔时触发刷新 |
| `rpc-deadline-ms` | Long | 30000 | Milvus gRPC 调用的超时时间（毫秒），0 表示不限制 |

**使用示例**：

```sql
CREATE TABLE item_embedding (
  id BIGINT,
  embedding ARRAY<FLOAT>,
  name STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'milvus',
  'url' = 'http://localhost:19530',
  'token' = 'root:Milvus',
  'database' = 'default',
  'collection' = 'item_embedding'
);
```

**注意事项**：
- Milvus 连接器支持复杂的过滤条件
- 向量搜索需要指定向量字段和查询向量
- 支持投影列优化，只返回需要的列

### 3. Kafka Connector

Kafka 连接器用于连接 Apache Kafka 消息队列，支持消息写入。

**连接器标识符**：`kafka`

**继承类型**：`SqlRecTable`

**特性**：
- 支持消息写入到 Kafka Topic
- 支持 JSON 和 Protobuf 格式消息
- 支持批量发送优化

**配置参数**：

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `properties.bootstrap.servers` | String | - | Kafka Broker 地址 |
| `topic` | String | - | Kafka Topic 名称 |
| `format` | String | `json` | 消息格式，可选值：`json`、`protobuf` |
| `protobuf.message-class-name` | String | - | Protobuf generated Message 类全名；`format = protobuf` 时必填 |
| `properties.producer.linger.ms` | Integer | 5000 | 批量发送等待时间（毫秒） |

**使用示例**：

```sql
CREATE TABLE rec_log_kafka (
  user_id BIGINT,
  item_id BIGINT,
  item_name STRING,
  rec_reason STRING,
  req_time BIGINT,
  req_id STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'rec_log',
  'properties.bootstrap.servers' = 'localhost:9092',
  'format' = 'json'
);
```

**注意事项**：
- Kafka 连接器主要用于消息写入，不支持查询操作
- `linger.ms` 参数控制批量发送，较大的值可以提高吞吐量但增加延迟
- Protobuf 格式要求 generated Message 类位于 SQLRec 运行时 classpath

### 4. JDBC Connector

JDBC 连接器用于连接关系型数据库（如 PostgreSQL、MySQL 等），支持 SQL 查询和数据写入。

**连接器标识符**：`jdbc`

**继承类型**：`SqlRecKvTable`

**特性**：
- 支持多种 JDBC 数据库（PostgreSQL、MySQL 等）
- 支持主键查询和本地缓存加速
- 支持复杂过滤条件查询（不仅限于主键过滤）
- 支持数据 Upsert 和删除
- 使用 HikariCP 连接池管理数据库连接
- 支持自定义 JDBC 属性

**配置参数**：

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `url` | String | - | JDBC 连接 URL，例如 `jdbc:postgresql://host:port/db` |
| `table-name` | String | - | JDBC 表名 |
| `username` | String | `""` | 数据库用户名 |
| `password` | String | `""` | 数据库密码 |
| `driver` | String | `""` | JDBC 驱动类名，例如 `org.postgresql.Driver` |
| `schema` | String | `""` | 数据库 Schema 名称（如 PostgreSQL 的 schema） |
| `max-cache-size` | Integer | 100000 | 本地缓存最大条目数 |
| `cache-ttl` | Integer | 30 | 本地缓存过期时间（秒），0 表示不缓存 |
| `connection.pool.size` | Integer | 0 | 连接池最大连接数（HikariCP maximumPoolSize），0 表示使用默认值 |
| `connection.pool.min-idle` | Integer | 0 | 连接池最小空闲连接数，0 表示使用默认值 |
| `connection.pool.idle-timeout` | Long | 0 | 连接池空闲超时时间（秒），0 表示使用默认值 |
| `connection.pool.max-lifetime` | Long | 0 | 连接池连接最大生命周期（秒），0 表示使用默认值 |
| `connection.pool.connection-timeout` | Long | 0 | 连接池连接超时时间（秒），0 表示使用默认值 |
| `connection.pool.validation-timeout` | Long | 0 | 连接池验证超时时间（秒），0 表示使用默认值 |
| `connection.pool.keepalive-time` | Long | 0 | 连接池保活时间（秒），0 表示使用默认值 |
| `connection.pool.pool-name` | String | `""` | 连接池名称 |
| `jdbc.properties.*` | String | - | 自定义 JDBC 属性，前缀 `jdbc.properties.` 后的部分作为属性名 |

**使用示例**：

```sql
CREATE TABLE user_profile (
  id BIGINT,
  name STRING,
  age INT,
  country STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://localhost:5432/mydb',
  'table-name' = 'user_profile',
  'username' = 'postgres',
  'password' = 'postgres',
  'driver' = 'org.postgresql.Driver'
);
```

**注意事项**：
- JDBC 连接器支持复杂过滤条件，不仅限于主键相等过滤
- 使用 HikariCP 连接池管理数据库连接，相同 URL 和用户名共享连接池
- 支持 Upsert 操作，根据主键自动判断插入或更新
- 可通过 `jdbc.properties.*` 前缀传递自定义 JDBC 属性

### 5. MongoDB Connector

MongoDB 连接器用于连接 MongoDB 文档数据库，支持文档查询和数据写入。

**连接器标识符**：`mongodb`

**继承类型**：`SqlRecKvTable`

**特性**：
- 支持 MongoDB 连接 URI
- 支持主键查询和本地缓存加速
- 支持复杂过滤条件（AND、OR、比较运算、IS NULL 等）
- 支持数据 Upsert 和删除
- 自动将 Calcite 过滤条件下推为 MongoDB 查询

**配置参数**：

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `uri` | String | - | MongoDB 连接 URI，例如 `mongodb://host:port` |
| `database` | String | - | 数据库名称 |
| `collection` | String | - | 集合名称 |
| `max-cache-size` | Integer | 100000 | 本地缓存最大条目数 |
| `cache-ttl` | Integer | 30 | 本地缓存过期时间（秒），0 表示不缓存 |

**使用示例**：

```sql
CREATE TABLE user_behavior (
  event_id BIGINT,
  user_id BIGINT,
  item_id BIGINT,
  action STRING,
  timestamp BIGINT,
  PRIMARY KEY (event_id) NOT ENFORCED
) WITH (
  'connector' = 'mongodb',
  'uri' = 'mongodb://localhost:27017',
  'database' = 'recommendation',
  'collection' = 'user_behavior'
);
```

**注意事项**：
- MongoDB 连接器支持复杂过滤条件，包括 AND、OR、等于、不等于、大于、小于等
- 无法下推的过滤条件将由 Calcite 在内存中处理
- 相同 URI 共享 MongoClient 实例
- Upsert 操作基于主键自动判断插入或更新
- 行为记录使用 `event_id` 作为主键；若改用 `user_id`，同一用户的后续写入会替换已有记录

### 6. Filesystem Connector

Filesystem 连接器用于读取本地文件系统中的数据文件，支持 CSV 和 JSON 格式。

**连接器标识符**：`filesystem`

**继承类型**：`SqlRecKvTable`

**特性**：
- 支持 CSV 和 JSON 两种文件格式
- 数据仅在首次访问时加载一次，后续访问直接使用内存数据
- 支持主键查询和过滤查询
- 主键是查找键，同一主键可保存多行
- 支持内存中的插入、更新和删除（不写回文件系统）
- 如果未配置路径或路径不存在，自动初始化为空表

**配置参数**：

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `path` | String | - | 文件路径，支持 `file:///` 前缀，例如 `file:///path/to/data.csv` |
| `format` | String | `csv` | 文件格式，可选值：`csv`、`json` |

**使用示例**：

```sql
-- CSV 格式
CREATE TABLE user_profile (
  id INT,
  name STRING,
  age INT,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'filesystem',
  'path' = '/data/users.csv',
  'format' = 'csv'
);

-- JSON 格式
CREATE TABLE product_info (
  id INT,
  name STRING,
  price INT,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'filesystem',
  'path' = '/data/products.json',
  'format' = 'json'
);

-- 不指定路径，初始化为空表
CREATE TABLE temp_table (
  id INT,
  name STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'filesystem'
);

-- 同一用户可以有多条兴趣记录
CREATE TABLE user_interest (
  user_id BIGINT,
  category STRING,
  PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
  'connector' = 'filesystem',
  'path' = '/data/user_interest.csv'
);
```

**注意事项**：
- CSV 文件第一行为表头，会被自动跳过
- CSV 文件支持双引号包裹的字段、字段中的逗号及双引号转义；暂不支持字段内换行
- JSON 文件支持数组格式 `[{...}, {...}]` 和单对象格式 `{...}`
- 数据写入操作仅修改内存中的数据，不会写回到文件系统
- 如果路径不存在或格式无效，表会被初始化为空表，不会抛出异常
- 加载 CSV/JSON 时保留同键的全部记录；写入会追加到该键的列表头部，删除按整行内容匹配并移除全部相同记录
- `UPDATE` 按查询条件选中旧行，并逐行替换；更新查找键时，其他同键记录仍会保留
- 仍需声明单列主键，但它只是查找键，不要求每行唯一
