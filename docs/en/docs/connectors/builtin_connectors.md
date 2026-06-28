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
| `data-structure` | String | `json` | Data structure, options: `json`, `list`, `string` |
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

### 4. JDBC Connector

The JDBC connector is used to connect to relational databases (e.g., PostgreSQL, MySQL), supporting SQL queries and data writes.

**Connector Identifier**: `jdbc`

**Inheritance Type**: `SqlRecKvTable`

**Features**:
- Supports various JDBC databases (PostgreSQL, MySQL, etc.)
- Supports primary key queries and local cache acceleration
- Supports complex filter condition queries (not limited to primary key filtering)
- Supports data upsert and deletion
- Uses HikariCP connection pool for database connection management
- Supports custom JDBC properties

**Configuration Parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `url` | String | - | JDBC connection URL, e.g. `jdbc:postgresql://host:port/db` |
| `table-name` | String | - | JDBC table name |
| `username` | String | `""` | Database username |
| `password` | String | `""` | Database password |
| `driver` | String | `""` | JDBC driver class name, e.g. `org.postgresql.Driver` |
| `schema` | String | `""` | Database schema name (e.g. PostgreSQL schema) |
| `max-cache-size` | Integer | 100000 | Maximum local cache entries |
| `cache-ttl` | Integer | 30 | Local cache expiration time (seconds), 0 means no cache |
| `connection.pool.size` | Integer | 0 | Connection pool max size (HikariCP maximumPoolSize), 0 means use default |
| `connection.pool.min-idle` | Integer | 0 | Connection pool min idle connections, 0 means use default |
| `connection.pool.idle-timeout` | Long | 0 | Connection pool idle timeout in seconds, 0 means use default |
| `connection.pool.max-lifetime` | Long | 0 | Connection pool max lifetime in seconds, 0 means use default |
| `connection.pool.connection-timeout` | Long | 0 | Connection pool connection timeout in seconds, 0 means use default |
| `connection.pool.validation-timeout` | Long | 0 | Connection pool validation timeout in seconds, 0 means use default |
| `connection.pool.keepalive-time` | Long | 0 | Connection pool keepalive time in seconds, 0 means use default |
| `connection.pool.pool-name` | String | `""` | Connection pool name |
| `jdbc.properties.*` | String | - | Custom JDBC properties, the part after `jdbc.properties.` prefix is used as the property name |

**Usage Example**:

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

**Notes**:
- JDBC connector supports complex filter conditions, not limited to primary key equality filtering
- Uses HikariCP connection pool for database connection management, sharing the pool for the same URL and username
- Supports upsert operations, automatically determining insert or update based on primary key
- Custom JDBC properties can be passed via the `jdbc.properties.*` prefix

### 5. MongoDB Connector

The MongoDB connector is used to connect to MongoDB document databases, supporting document queries and data writes.

**Connector Identifier**: `mongodb`

**Inheritance Type**: `SqlRecKvTable`

**Features**:
- Supports MongoDB connection URI
- Supports primary key queries and local cache acceleration
- Supports complex filter conditions (AND, OR, comparison operators, IS NULL, etc.)
- Supports data upsert and deletion
- Automatically pushes down Calcite filter conditions to MongoDB queries

**Configuration Parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `uri` | String | - | MongoDB connection URI, e.g. `mongodb://host:port` |
| `database` | String | - | Database name |
| `collection` | String | - | Collection name |
| `max-cache-size` | Integer | 100000 | Maximum local cache entries |
| `cache-ttl` | Integer | 30 | Local cache expiration time (seconds), 0 means no cache |

**Usage Example**:

```sql
CREATE TABLE user_behavior (
  user_id BIGINT,
  item_id BIGINT,
  action STRING,
  timestamp BIGINT,
  PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
  'connector' = 'mongodb',
  'uri' = 'mongodb://localhost:27017',
  'database' = 'recommendation',
  'collection' = 'user_behavior'
);
```

**Notes**:
- MongoDB connector supports complex filter conditions including AND, OR, equals, not equals, greater than, less than, etc.
- Filter conditions that cannot be pushed down will be handled by Calcite in memory
- MongoClient instances are shared for the same URI
- Upsert operations automatically determine insert or update based on primary key

### 6. Filesystem Connector

The Filesystem connector is used to read data files from the local file system, supporting CSV and JSON formats.

**Connector Identifier**: `filesystem`

**Inheritance Type**: `SqlRecKvTable`

**Features**:
- Supports CSV and JSON file formats
- Data is loaded only once on first access; subsequent accesses use in-memory data
- Supports primary key queries and filter queries
- Supports data upsert and deletion (modifies memory only, does not write back to the file system)
- Automatically initializes as an empty table if no path is configured or the path does not exist

**Configuration Parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `path` | String | - | File path, supports `file:///` prefix, e.g. `file:///path/to/data.csv` |
| `format` | String | `csv` | File format, options: `csv`, `json` |

**Usage Example**:

```sql
-- CSV format
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

-- JSON format
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

-- No path specified, initializes as empty table
CREATE TABLE temp_table (
  id INT,
  name STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'filesystem'
);
```

**Notes**:
- The first line of a CSV file is treated as a header and is automatically skipped
- CSV files support double-quoted fields (RFC 4180)
- JSON files support array format `[{...}, {...}]` and single object format `{...}`
- Write operations only modify in-memory data and do not write back to the file system
- If the path does not exist or the format is invalid, the table is initialized as an empty table without throwing an exception
