# Building a Custom Connector

A custom connector integrates another storage system with SQLRec. A complete implementation normally includes:

1. option definitions and a parsed configuration object;
2. a Handler or Client for the external system;
3. a Calcite Table implementation;
4. an `HmsTableFactory`;
5. a Java SPI registration file.

This page describes the current contracts and a practical implementation order. The safest starting point is a built-in module with similar capabilities; replace its client and configuration while retaining the established integration pattern.

## Choose a Reference Implementation

| Requirement | Reference module |
|-------------|------------------|
| Primary-key lookup and local cache | `sqlrec-connector-redis` |
| Relational database and complex filter pushdown | `sqlrec-connector-jdbc` |
| Document database | `sqlrec-connector-mongodb` |
| Vector search | `sqlrec-connector-milvus` |
| Write-only sink | `sqlrec-connector-kafka` |
| Minimal readable/writable key-value implementation | `sqlrec-connector-filesystem` |

## Choose Table Capabilities

### `SqlRecTable`

`SqlRecTable` is the minimum base class. If the source is not key-value oriented, extend it and implement Calcite's `ScannableTable`, `FilterableTable`, or `ModifiableTable` as needed. The Kafka connector follows this pattern.

### `SqlRecKvTable`

Extend `SqlRecKvTable` when you need batched primary-key lookup, filtering, and writes. Implement at least:

```java
protected Enumerable<Object[]> scanImpl(List<RexNode> filters);

public Map<Object, List<Object[]>> getByPrimaryKeyImpl(
    Set<Object> keySet
);

public RelDataType getRowType(RelDataTypeFactory typeFactory);

public Collection getModifiableCollection();

public int getPrimaryKeyIndex();
```

`scanImpl()` performs a scan or handles filters that can be pushed down. `getByPrimaryKeyImpl()` should fetch all keys in one batch instead of making one network request per key. Its `List<Object[]>` result allows multiple rows per lookup key; retain every matching row when the source permits duplicate keys.

Keep the default `onlyFilterByPrimaryKey()` if the source supports only key filters. Override it only when the connector correctly handles broader filters, as JDBC and MongoDB do.

### `VectorSearchable`

A Table that supports vector search also implements:

```java
public interface VectorSearchable {
    List<VectorSearchResult> searchByEmbeddingImpl(
        VectorSearchRequest request
    );
}
```

`VectorSearchable` is an optional capability, not a Table base class. The Milvus Table extends `SqlRecKvTable` and implements this interface.

## Implementation Steps

### 1. Define Options

```java
public final class ExampleOptions {
    public static final String CONNECTOR_IDENTIFIER = "example";

    public static final ConfigOption<String> URL = new ConfigOption<>(
        "url",
        null,
        "Example server URL",
        null,
        String.class
    );

    private ExampleOptions() {
    }
}
```

Parse table properties into a dedicated configuration class. In addition to connector-specific settings, it normally stores:

```java
public List<FieldSchema> fieldSchemas;
public String primaryKey;
public Integer primaryKeyIndex;
```

Reject missing required values while creating the table, with an actionable message, rather than failing on the first query.

### 2. Implement Data Access

Keep SDK usage, pooling, serialization, and retry behavior in a Handler or Client instead of putting all storage logic in the Calcite Table. Cover the capabilities the connector exposes:

- batched primary-key reads;
- scans or pushed-down filters;
- batched writes/upserts and deletes;
- type conversion, `NULL`, and resource cleanup;
- bounded timeouts and the distinction between retryable and permanent errors.

### 3. Implement the Table

The following sketch shows the boundary between the Table and Handler:

```java
public final class ExampleCalciteTable extends SqlRecKvTable {
    private final ExampleConfig config;
    private final ExampleHandler handler;

    public ExampleCalciteTable(ExampleConfig config) {
        this.config = config;
        this.handler = new ExampleHandler(config);
        initCache(config.maxCacheSize, config.cacheTtlSeconds);
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return DataTypeUtils.getRelDataType(typeFactory, config.fieldSchemas);
    }

    @Override
    public int getPrimaryKeyIndex() {
        return config.primaryKeyIndex;
    }

    @Override
    public Map<Object, List<Object[]>> getByPrimaryKeyImpl(Set<Object> keys) {
        return handler.batchGet(keys);
    }

    @Override
    protected Enumerable<Object[]> scanImpl(List<RexNode> filters) {
        return Linq4j.asEnumerable(handler.scan(filters));
    }

    @Override
    public Collection getModifiableCollection() {
        return new ExampleCollection(this, handler);
    }
}
```

This is not a standalone connector. `ExampleCollection` must extend `SqlRecCollection` and implement real upsert/delete behavior like the built-in connectors.

### 4. Implement the Factory

```java
public final class ExampleCalciteTableFactory implements HmsTableFactory {
    @Override
    public Table getTableFromHmsTable(
            org.apache.hadoop.hive.metastore.api.Table tableObj) {
        Map<String, String> options =
            HiveTableUtils.getFlinkTableOptions(tableObj);
        ExampleConfig config = ExampleOptions.getConfig(options);
        config.fieldSchemas = HiveTableUtils.parse(tableObj);
        config.primaryKey = HiveTableUtils.getTablePrimaryKey(tableObj);
        config.primaryKeyIndex = HiveTableUtils.getTablePrimaryKeyIndex(
            config.fieldSchemas,
            config.primaryKey
        );
        return new ExampleCalciteTable(config);
    }

    @Override
    public String getConnectorName() {
        return ExampleOptions.CONNECTOR_IDENTIFIER;
    }
}
```

Read `WITH` properties with `HiveTableUtils.getFlinkTableOptions()` and obtain fields and the primary key through the other `HiveTableUtils` methods. Do not assume the primary key appears as a custom `primary-key` option.

### 5. Register the SPI

Create:

```text
src/main/resources/META-INF/services/com.sqlrec.common.schema.HmsTableFactory
```

Its content is the fully qualified factory class:

```text
com.example.sqlrec.connector.ExampleCalciteTableFactory
```

Put the connector JAR and runtime dependencies on SQLRec's classpath, then restart every instance.

## Thread Safety and Connections

Table instances are shared across queries. Therefore:

- never store current-query filters, result rows, or pagination state in Table fields;
- use a thread-safe client or a connection pool;
- share clients or pools between tables with the same connection settings where practical;
- configure bounded connection, query, and write timeouts;
- confirm cache-expiration and consistency requirements before enabling the local `SqlRecKvTable` cache.

## Suggested Layout

```text
sqlrec-connector-example/
├── pom.xml
└── src/main/
    ├── java/com/example/sqlrec/connector/
    │   ├── ExampleOptions.java
    │   ├── ExampleConfig.java
    │   ├── ExampleHandler.java
    │   ├── ExampleCalciteTable.java
    │   └── ExampleCalciteTableFactory.java
    └── resources/META-INF/services/
        └── com.sqlrec.common.schema.HmsTableFactory
```

## Release Checklist

- Test valid and missing table options.
- Test batched primary-key lookup, including missing keys and type conversion.
- Test filters that can and cannot be pushed down.
- Test cache invalidation after `INSERT`, `UPDATE`, and `DELETE`.
- Test concurrent queries and client disconnect/recovery.
- Confirm that the SPI file is packaged and the connector identifier is unique.

Interfaces and base classes may evolve. Use the current `sqlrec-common` and built-in connector source as authoritative.
