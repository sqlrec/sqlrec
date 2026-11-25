# Built-in Connectors

This document introduces the built-in data connectors in SQLRec and their usage.

## Overview

SQLRec provides multiple built-in connectors for connecting to different data storage systems. Query and write capabilities depend on the data source.

If you are new to creating and using data-source tables, start with [Connecting Data Sources](../../guides/data-sources.md).

### Query Caching

Redis, JDBC, and MongoDB support primary-key query caching. `cache-ttl` controls expiration and `max-cache-size` controls capacity; setting either to 0 disables caching. Writes outside SQLRec become visible to an existing cache after TTL expiry. Shorten the TTL or disable caching when freshness is more important.

Query caching reuses storage query results, unlike `CACHE TABLE` results in the current execution scope. With `id` as the primary key, common query paths are:

| Query condition | Redis | JDBC / MongoDB |
| --- | --- | --- |
| `WHERE id = 1` | Primary-key cache | Primary-key cache |
| `WHERE id = 1 AND status = 'active'` | Fetch candidates by key, then filter | Compound scan; no primary-key cache |
| `WHERE status = 'active'` | Full scans unsupported | Filtered scan; no primary-key cache |

KV Joins on the right table's primary key also use this cache. Non-key lookups, Milvus, Filesystem, and Flink Redis Lookup do not use it.

SQLRec table writes invalidate affected primary-key cache entries. DDL refreshes retain unchanged table definitions and their caches. Explicit `FLUSH` invalidates Connector table objects, which are recreated on subsequent access. Filesystem reloads its files, resetting runtime in-memory writes; session `CACHE TABLE` results are retained. See [FLUSH](../sql.md#flush).

## Built-in Connectors

### 1. Redis Connector

The Redis connector is used to connect to Redis databases, supporting key-value storage and queries.

**Connector Identifier**: `redis`

**Configuration Parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `url` | String | - | Redis connection URL, format: `redis://password@host:port/db` |
| `redis-mode` | String | `single` | Redis mode, options: `single` (standalone), `cluster` |
| `data-structure` | String | `json` | Data structure, options: `json`, `list`, `string` |
| `format` | String | `json` | Value format: `json` or `protobuf` |
| `protobuf.message-class-name` | String | - | Fully qualified generated Protobuf Message class; required for `format = protobuf` |
| `max-list-size` | Integer | 0 | Maximum list length, 0 means unlimited |
| `ttl` | Integer | 2592000 | Key expiration time (seconds), default 30 days |
| `cache-ttl` | Integer | 30 | Local cache expiration time (seconds), 0 means no cache |
| `max-cache-size` | Integer | 100000 | Maximum cached primary-key entries; 0 disables caching |

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

CREATE TABLE user_proto (
  `value` STRING,
  PRIMARY KEY (`value`) NOT ENFORCED
) WITH (
  'connector' = 'redis',
  'url' = 'redis://localhost:6379/0',
  'format' = 'protobuf',
  'protobuf.message-class-name' = 'com.google.protobuf.StringValue'
);
```

**Notes**:
- Queries require a primary-key equality condition (for example, `WHERE user_id = 42`); other columns can be filtered after the key lookup, but full-table scans are unsupported
- Using local cache can significantly improve query performance
- In `list` mode, the primary key is a Redis list lookup key, not a unique row identifier. Multiple rows can share one key. `INSERT` prepends rows, and `max-list-size` limits the list length after writes
- In `list` mode, `UPDATE` selects old rows using the SQL predicate, removes one exact list entry per selected row, then prepends each new row. Changing the primary key moves the row to the new key. A missing old row is not inserted
- In `list` mode, `DELETE` removes all list entries identical to a selected row, while `UPDATE` removes one matching entry for each selected old row. The remove and insert commands in `UPDATE` are not atomic
- In `json` and `string` modes, `UPDATE` can change the primary key, including across Redis Cluster slots. SQLRec writes the new key with the configured TTL before deleting the old key. An existing destination key causes an error. Both keys can be visible between commands and may remain if deletion fails
- Protobuf format requires the generated Message class on the SQLRec and Flink runtime classpaths
- `format = protobuf` cannot be combined with `data-structure = string`

### 2. Milvus Connector

The Milvus connector is used to connect to Milvus vector databases, supporting vector similarity retrieval.

**Connector Identifier**: `milvus`

**Configuration Parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `url` | String | - | Milvus server address |
| `token` | String | - | Milvus authentication token |
| `database` | String | `default` | Database name |
| `collection` | String | - | Collection name |
| `batch-size` | Integer | 4096 | Batch size for bulk insert |
| `pool.max-idle-per-key` | Integer | 10 | Maximum idle connections per key in the connection pool |
| `pool.max-total-per-key` | Integer | 100 | Maximum total connections per key in the connection pool |
| `pool.max-total` | Integer | 100 | Maximum total connections in the connection pool |
| `pool.max-block-wait-duration` | Long | 5 | Maximum blocking wait time (seconds) when acquiring a connection from the pool |
| `pool.min-evictable-idle-duration` | Long | 10 | Minimum evictable idle duration (seconds) for connections in the pool |
| `flush-interval` | Long | 1 | Interval (seconds) checked when a Flink sink receives a new record; full batches, checkpoints and close also flush. Idle input does not trigger flushing |
| `rpc-deadline-ms` | Long | 30000 | Timeout (milliseconds) for Milvus gRPC calls, 0 means no limit |

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

**Configuration Parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `properties.bootstrap.servers` | String | - | Kafka Broker address |
| `topic` | String | - | Kafka Topic name |
| `format` | String | `json` | Message format: `json` or `protobuf` |
| `protobuf.message-class-name` | String | - | Fully qualified generated Protobuf Message class; required for `format = protobuf` |
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
- Protobuf format requires the generated Message class on SQLRec's runtime classpath

### 4. JDBC Connector

The JDBC connector is used to connect to relational databases (e.g., PostgreSQL, MySQL), supporting SQL queries and data writes.

**Connector Identifier**: `jdbc`

**Configuration Parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `url` | String | - | JDBC connection URL, e.g. `jdbc:postgresql://host:port/db` |
| `table-name` | String | - | JDBC table name |
| `username` | String | `""` | Database username |
| `password` | String | `""` | Database password |
| `driver` | String | `""` | JDBC driver class name, e.g. `org.postgresql.Driver` |
| `schema` | String | `""` | Database schema name (e.g. PostgreSQL schema) |
| `max-cache-size` | Integer | 100000 | Maximum cached primary-key entries; 0 disables caching |
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
- PostgreSQL, MySQL and H2 reserved words and identifiers containing special characters are quoted automatically; declared names must match the database identifiers, including case for quoted names
- H2 upserts use declared column names, allowing a different physical column order or a subset of columns
- Supports upsert operations, automatically determining insert or update based on primary key
- JDBC commits writes one batch at a time and rolls back the batch on failure. `UPDATE` matches the original primary key and can change it, subject to the database's uniqueness and foreign key constraints
- Custom JDBC properties can be passed via the `jdbc.properties.*` prefix

### 5. MongoDB Connector

The MongoDB connector is used to connect to MongoDB document databases, supporting document queries and data writes.

**Connector Identifier**: `mongodb`

**Configuration Parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `uri` | String | - | MongoDB connection URI, e.g. `mongodb://host:port` |
| `database` | String | - | Database name |
| `collection` | String | - | Collection name |
| `max-cache-size` | Integer | 100000 | Maximum cached primary-key entries; 0 disables caching |
| `cache-ttl` | Integer | 30 | Local cache expiration time (seconds), 0 means no cache |

**Usage Example**:

```sql
CREATE TABLE user_behavior (
  event_id BIGINT,
  user_id BIGINT,
  item_id BIGINT,
  action STRING,
  `timestamp` BIGINT,
  PRIMARY KEY (event_id) NOT ENFORCED
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
- `UPDATE` matches documents by their original primary key and can change a lookup key stored in an ordinary field. MongoDB's native `_id` is immutable. If a batch fails, earlier successful writes may already have taken effect
- Behavior records use `event_id` as the primary key; using `user_id` would make later writes for the same user replace an existing record

### 6. Filesystem Connector

The Filesystem connector is used to read data files from the local file system, supporting CSV and JSON formats.

**Connector Identifier**: `filesystem`

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

-- Multiple interests can share one user ID
CREATE TABLE user_interest (
  user_id BIGINT,
  category STRING,
  PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
  'connector' = 'filesystem',
  'path' = '/data/user_interest.csv'
);
```

**Notes**:
- The first nonblank CSV record is treated as a header and skipped. Values map to declared fields by position, without reordering by header name
- Apache Commons CSV parses quoted fields, embedded commas, escaped double quotes and quoted newlines. Empty lines and single-field records containing only whitespace are skipped
- JSON files support array format `[{...}, {...}]` and single object format `{...}`
- Write operations only modify in-memory data and do not write back to the file system
- If the path does not exist or the format is invalid, the table is initialized as an empty table without throwing an exception
- All CSV/JSON rows with the same key are retained; writes prepend a row and deletes remove every matching full row
- `UPDATE` replaces each selected old row; changing its lookup key preserves other rows under the old key
- A single-column primary key is still required, but it is only a lookup key and need not be unique per row
