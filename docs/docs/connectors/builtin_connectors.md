# 内置 Connectors

本文档介绍 SQLRec 内置的数据连接器及其使用方法。

## 概述

SQLRec 提供了多种内置连接器，用于连接不同的数据存储系统。连接器基于 Calcite 表抽象实现，支持 SQL 查询和数据写入操作。

### 表类型层次结构

SQLRec 的连接器基于以下表类型层次结构：

```
SqlRecTable (抽象基类)
    │
    ├── SqlRecKvTable (键值表，支持主键查询和缓存)
    │       │
    │       └── SqlRecVectorTable (向量表，支持向量检索)
    │
    └── 其他表类型...
```

**表类型说明**：

| 表类型 | 说明 | 特性 |
|--------|------|------|
| `SqlRecTable` | 抽象基类，继承自 Calcite 的 `AbstractTable` | 提供基础表功能 |
| `SqlRecKvTable` | 键值表，支持主键查询 | 支持主键索引、缓存机制、过滤查询、数据修改 |
| `SqlRecVectorTable` | 向量表，支持向量检索 | 继承 `SqlRecKvTable`，支持向量相似度搜索 |

## 内置连接器

### 1. Redis Connector

Redis 连接器用于连接 Redis 数据库，支持键值存储和查询。

**连接器标识符**：`redis`

**继承类型**：`SqlRecKvTable`

**特性**：
- 支持单机模式和集群模式
- 支持 String 和 List 数据结构
- 支持本地缓存加速查询
- 支持主键过滤查询
- 支持数据写入和删除

**配置参数**：

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `url` | String | - | Redis 连接 URL，格式：`redis://password@host:port/db` |
| `redis-mode` | String | `single` | Redis 模式，可选值：`single`（单机）、`cluster`（集群） |
| `data-structure` | String | `string` | 数据结构，可选值：`string`、`list` |
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
```

**注意事项**：
- Redis 连接器只支持主键相等过滤（`WHERE key = value`）
- 使用本地缓存可以显著提升查询性能
- List 数据结构适合存储多值场景

### 2. Milvus Connector

Milvus 连接器用于连接 Milvus 向量数据库，支持向量相似度检索。

**连接器标识符**：`milvus`

**继承类型**：`SqlRecVectorTable`

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
- 支持 JSON 格式消息
- 支持批量发送优化
- 支持自定义序列化器

**配置参数**：

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `properties.bootstrap.servers` | String | - | Kafka Broker 地址 |
| `topic` | String | - | Kafka Topic 名称 |
| `format` | String | `json` | 消息格式 |
| `properties.producer.key.serializer` | String | `org.apache.kafka.common.serialization.StringSerializer` | Key 序列化器 |
| `properties.producer.value.serializer` | String | `org.apache.kafka.common.serialization.StringSerializer` | Value 序列化器 |
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
- 消息以 JSON 格式发送到 Kafka
