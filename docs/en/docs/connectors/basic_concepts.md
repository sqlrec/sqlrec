# SQLRec Connector System

This document introduces the SQLRec connector architecture, table type system, and connector loading and execution flow.

## Overview

A connector maps an external storage system to a table that SQLRec can query or write. Users declare a data source with `CREATE TABLE ... WITH ('connector' = '...')`. SQLRec reads the table definition from Hive Metastore, locates the matching `HmsTableFactory`, and creates a Calcite `Table` for SQL planning and execution.

### Core Concepts

| Concept | Description |
|---------|-------------|
| **Connector** | An adapter for an external storage system, such as Redis, Milvus, or JDBC |
| **Table** | The connector's Calcite table abstraction, which defines its schema and query or write capabilities |
| **`HmsTableFactory`** | The Calcite-side factory SPI that creates a `Table` from a Metastore table definition and may provide planner rules |
| **`SqlRecTable`** | The abstract base class for all SQLRec Calcite tables |
| **`SqlRecKvTable`** | A key-value table base class with primary-key lookup, filtering, modification, and optional local caching |
| **`VectorSearchable`** | An optional vector-search capability implemented by tables that support vector similarity search |

### From DDL to Execution

```text
CREATE TABLE ... WITH ('connector' = 'redis')
                    │
                    ▼
        Hive Metastore table definition
                    │
                    ▼
      ServiceLoader loads HmsTableFactory
                    │
                    ▼
        Create and cache Calcite Table
                    │
          ┌─────────┴─────────┐
          ▼                   ▼
      Query/filter       INSERT/UPDATE/DELETE
```

The `connector` property selects the factory. A factory declares its identifier through `getConnectorName()` and converts the table definition into a Calcite `Table` through `getTableFromHmsTable()`. SQLRec manages and shares table objects across queries, so implementations must be thread-safe.

## Table Type System

```text
Calcite AbstractTable
        │
        ▼
SqlRecTable
        │
        ├── Connector-specific table (implements Calcite capabilities as needed)
        │
        └── SqlRecKvTable
                │
                └── optionally implements VectorSearchable
```

### `SqlRecTable`

`SqlRecTable` extends Calcite's `AbstractTable`, stores the table name and creation SQL, and is the minimum common base class for SQLRec tables. A connector can implement Calcite capability interfaces as needed, such as `ModifiableTable` for writes.

Use it when the table does not need the common primary-key lookup and caching behavior. For example, the Kafka connector extends `SqlRecTable` directly and implements `ModifiableTable`.

### `SqlRecKvTable`

`SqlRecKvTable` extends `SqlRecTable` and implements both `FilterableTable` and `ModifiableTable`. It provides the following shared behavior:

- Recognizes primary-key predicates and calls `getByPrimaryKeyImpl()` for batched reads
- Enables an optional local Caffeine cache through `initCache()`
- Uses `scanImpl()` for non-primary-key scans or pushed-down filters
- Converts primary keys and rows to SQL types at query boundaries and records metrics

Subclasses define at least the row type, primary-key position, batched primary-key lookup, scan behavior, and modifiable collection. The Calcite tables for Redis, JDBC, MongoDB, Filesystem, and Milvus use this base class.

### `VectorSearchable`

`VectorSearchable` is an independent capability interface, not a table base class. A `SqlRecTable` subclass that supports vector retrieval implements `searchByEmbeddingImpl()`; the interface's default method handles result type conversion and metrics. The Milvus connector currently combines `SqlRecKvTable` with `VectorSearchable`.

### Choosing a Type

| Requirement | Recommended type |
|-------------|------------------|
| Only basic table behavior is needed, or the access pattern is not key-value oriented | Extend `SqlRecTable` and implement Calcite interfaces as needed |
| Primary-key batch lookup, filtering, modification, or local caching is needed | Extend `SqlRecKvTable` |
| Vector similarity search is needed on either table type | Also implement `VectorSearchable` |

## Connector Extension Mechanism

A Calcite-side connector has three parts:

1. The configuration layer parses `WITH` properties and builds connector configuration.
2. `HmsTableFactory` declares the connector identifier, converts Metastore metadata into a table instance, and returns optional planner rules.
3. The table and data-access layer implements reads, writes, filter pushdown, caching, or vector retrieval.

Factory implementations are registered through the Java SPI file `META-INF/services/com.sqlrec.common.schema.HmsTableFactory`. SQLRec discovers them with `ServiceLoader` and builds a factory map keyed by connector identifier.

Some connectors also provide a Flink `DynamicTableFactory` for operations delegated to Flink. It uses the same connector identifier as the Calcite side but is a separate runtime adapter.

## Next Steps

- See [Built-in Connectors](./builtin_connectors.md) for available data sources, options, and SQL examples.
- See [Custom Connectors](./custom_connectors.md) for the complete process of implementing and registering a connector.
