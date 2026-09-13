# Connecting Data Sources

Connectors map external stores such as Redis, Milvus, and JDBC databases to SQLRec tables. To use one, select a connector and provide its connection settings in `CREATE TABLE ... WITH (...)`.

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

## Choosing a Connector

| Connector | Primary use | Read | Write | Persistent | Special capability |
|-----------|-------------|------|-------|------------|--------------------|
| Redis | Online features, recall results, exposure history | Yes | Yes | Yes | Primary-key lookup, local cache |
| Milvus | Vector recall | Yes | Yes | Yes | Nearest-neighbor search |
| JDBC | PostgreSQL, MySQL, and other relational databases | Yes | Yes | Yes | Filter pushdown, connection pooling |
| MongoDB | Document data | Yes | Yes | Yes | Complex filter pushdown |
| Kafka | Recommendation logs and behavior events | No | Yes | Managed by Kafka | Asynchronous message writes |
| Filesystem | Demos, tests, and small static datasets | Yes | In memory only | No | Initial data from CSV or JSON |

`INSERT`, `UPDATE`, and `DELETE` on a Filesystem table change only the data in the current SQLRec process; they do not write back to the source file. Use this connector for demos and tests, not production persistence.

## What to Check Before Creating a Table

### Primary Key

Redis, Milvus, JDBC, MongoDB, and Filesystem tables should generally declare a primary key:

```sql
PRIMARY KEY (user_id) NOT ENFORCED
```

SQLRec can use the key to optimize a Join into a batched lookup. The key must match the external store. SQLRec does not verify uniqueness in the source data.

### Data Types

Each SQL field type must be convertible to and from the actual source value. Prefer explicit types such as `BIGINT`, `DOUBLE`, `VARCHAR`, and `ARRAY<FLOAT>` instead of relying on implicit conversion.

### Connection Address

`localhost` in an example works only when SQLRec and the data source share the same network environment. In Kubernetes, normally use a Service DNS name or another address reachable from the cluster.

## Querying and Writing

Use a connector table like a regular SQL table:

```sql
SELECT * FROM user_profile WHERE user_id = 1001;

INSERT INTO user_profile VALUES (1001, 'CN', 25);
```

Capabilities differ by connector. Kafka is write-only, Redis is optimized for primary-key lookup, and Milvus vector search is triggered by a specific Join pattern. Check the limitations for the selected [built-in connector](../reference/connectors/builtin-connectors.md) before using it.

## Next Steps

- [Built-in Connectors](../reference/connectors/builtin-connectors.md): options, table examples, and limitations.
- [Custom Connector](../development/custom-connector.md): implement and register another data source adapter.
