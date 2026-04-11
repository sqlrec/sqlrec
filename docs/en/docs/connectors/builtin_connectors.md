# Built-in Connectors

This document introduces the built-in data connectors in SQLRec and their usage.

## Overview

SQLRec provides multiple built-in connectors for connecting to different data storage systems. Connectors are implemented based on Calcite table abstractions, supporting SQL queries and data write operations.

### Table Type Hierarchy

SQLRec connectors are based on the following table type hierarchy:

```
SqlRecTable (Abstract Base Class)
    │
    ├── SqlRecKvTable (Key-Value Table, supports primary key queries and caching)
    │       │
    │       └── SqlRecVectorTable (Vector Table, supports vector retrieval)
    │
    └── Other table types...
```

**Table Type Descriptions**:

| Table Type | Description | Features |
|------------|-------------|----------|
| `SqlRecTable` | Abstract base class, inherits from Calcite's `AbstractTable` | Provides basic table functionality |
| `SqlRecKvTable` | Key-value table, supports primary key queries | Supports primary key indexing, caching mechanism, filter queries, data modification |
| `SqlRecVectorTable` | Vector table, supports vector retrieval | Inherits `SqlRecKvTable`, supports vector similarity search |

## Built-in Connectors

### 1. Redis Connector

The Redis connector is used to connect to Redis databases, supporting key-value storage and queries.

**Connector Identifier**: `redis`

**Inheritance Type**: `SqlRecKvTable`

**Features**:
- Supports standalone and cluster modes
- Supports String and List data structures
- Supports local cache for query acceleration
- Supports primary key filter queries
- Supports data insertion and deletion

**Configuration Parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `url` | String | - | Redis connection URL, format: `redis://password@host:port/db` |
| `redis-mode` | String | `single` | Redis mode, options: `single` (standalone), `cluster` |
| `data-structure` | String | `string` | Data structure, options: `string`, `list` |
| `max-list-size` | Integer | 0 | Maximum list length, 0 means unlimited |
| `ttl` | Integer | 2592000 | Key expiration time (seconds), default 30 days |
| `cache-ttl` | Integer | 30 | Local cache expiration time (seconds), 0 means no cache |
| `max-cache-size` | Integer | 100000 | Maximum local cache entries |

**Usage Example**:

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

**Notes**:
- Redis connector only supports primary key equality filtering (`WHERE key = value`)
- Using local cache can significantly improve query performance
- List data structure is suitable for multi-value scenarios

### 2. Milvus Connector

The Milvus connector is used to connect to Milvus vector databases, supporting vector similarity retrieval.

**Connector Identifier**: `milvus`

**Inheritance Type**: `SqlRecVectorTable`

**Features**:
- Supports vector similarity search (ANN)
- Supports primary key queries
- Supports filter conditions
- Supports data insertion and deletion
- Supports projection column optimization

**Configuration Parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `url` | String | - | Milvus server address |
| `token` | String | - | Milvus authentication token |
| `database` | String | `default` | Database name |
| `collection` | String | - | Collection name |

**Usage Example**:

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

**Notes**:
- Milvus connector supports complex filter conditions
- Vector search requires specifying the vector field and query vector
- Supports projection column optimization, returning only needed columns

### 3. Kafka Connector

The Kafka connector is used to connect to Apache Kafka message queues, supporting message writing.

**Connector Identifier**: `kafka`

**Inheritance Type**: `SqlRecTable`

**Features**:
- Supports message writing to Kafka Topic
- Supports JSON format messages
- Supports batch sending optimization
- Supports custom serializers

**Configuration Parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `properties.bootstrap.servers` | String | - | Kafka Broker address |
| `topic` | String | - | Kafka Topic name |
| `format` | String | `json` | Message format |
| `properties.producer.key.serializer` | String | `org.apache.kafka.common.serialization.StringSerializer` | Key serializer |
| `properties.producer.value.serializer` | String | `org.apache.kafka.common.serialization.StringSerializer` | Value serializer |
| `properties.producer.linger.ms` | Integer | 5000 | Batch sending wait time (milliseconds) |

**Usage Example**:

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

**Notes**:
- Kafka connector is mainly used for message writing, does not support query operations
- `linger.ms` parameter controls batch sending, larger values can improve throughput but increase latency
- Messages are sent to Kafka in JSON format
