# Custom Connectors

This document introduces how to develop custom connectors to extend SQLRec's data source support.

## Overview

SQLRec provides a flexible connector extension mechanism that allows developers to implement custom data source connectors. By implementing specific interfaces and inheriting base classes, new data storage systems can be quickly integrated.

Before developing a connector, read [Connector Basic Concepts](./basic_concepts.md) and choose `SqlRecTable`, `SqlRecKvTable`, and the optional `VectorSearchable` capability according to the access pattern.

## Developing Custom Connectors

### Step 1: Create Configuration Class

Create a configuration class to store connector configuration parameters:

```java
package com.sqlrec.connectors.example.config;

public class ExampleConfig {
    public String host;
    public int port;
    public String database;
    public int timeout;
}
```

### Step 2: Create Configuration Options Class

Create a configuration options class to define configuration parameters:

```java
package com.sqlrec.connectors.example.config;

import com.sqlrec.common.config.ConfigOption;
import java.util.Map;

public class ExampleOptions {
    public static final String CONNECTOR_IDENTIFIER = "example";

    public static final ConfigOption<String> HOST = new ConfigOption<>(
        "host",
        "localhost",
        "Example server host",
        null,
        String.class
    );

    public static final ConfigOption<Integer> PORT = new ConfigOption<>(
        "port",
        8080,
        "Example server port",
        null,
        Integer.class
    );

    public static final ConfigOption<String> DATABASE = new ConfigOption<>(
        "database",
        "default",
        "Database name",
        null,
        String.class
    );

    public static final ConfigOption<Integer> TIMEOUT = new ConfigOption<>(
        "timeout",
        30000,
        "Connection timeout in milliseconds",
        null,
        Integer.class
    );

    public static ExampleConfig getExampleConfig(Map<String, String> options) {
        ExampleConfig config = new ExampleConfig();
        config.host = HOST.getValue(options);
        config.port = PORT.getValue(options);
        config.database = DATABASE.getValue(options);
        config.timeout = TIMEOUT.getValue(options);
        return config;
    }
}
```

### Step 3: Implement Table Class

Choose the appropriate base class based on requirements and implement the table class:

#### Implementing SqlRecTable

```java
package com.sqlrec.connectors.example.calcite;

import com.sqlrec.common.schema.SqlRecTable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.connectors.example.config.ExampleConfig;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ModifiableTable;
import java.util.Collection;

public class ExampleCalciteTable extends SqlRecTable implements ModifiableTable {
    private final ExampleConfig config;

    public ExampleCalciteTable(ExampleConfig config) {
        this.config = config;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return DataTypeUtils.getRelDataType(typeFactory, config.fieldSchemas);
    }

    @Override
    public Collection getModifiableCollection() {
        return new ExampleCollection(config);
    }

    // ... other necessary method implementations
}
```

#### Implementing SqlRecKvTable

```java
package com.sqlrec.connectors.example.calcite;

import com.sqlrec.common.schema.SqlRecKvTable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.connectors.example.config.ExampleConfig;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import java.util.*;

public class ExampleCalciteTable extends SqlRecKvTable {
    private final ExampleConfig config;
    private final ExampleHandler handler;

    public ExampleCalciteTable(ExampleConfig config) {
        this.config = config;
        this.handler = new ExampleHandler(config);
        this.handler.open();
        initCache(config.maxCacheSize, config.cacheTtl);
    }

    @Override
    public int getPrimaryKeyIndex() {
        return config.primaryKeyIndex;
    }

    @Override
    public Map<Object, List<Object[]>> getByPrimaryKeyImpl(Set<Object> keySet) {
        return handler.batchGet(keySet);
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return DataTypeUtils.getRelDataType(typeFactory, config.fieldSchemas);
    }

    @Override
    protected Enumerable<Object[]> scanImpl(List<RexNode> filters) {
        // Implement filter query logic
        List<Object[]> results = handler.scan(filters);
        return Linq4j.asEnumerable(results);
    }

    @Override
    public Collection getModifiableCollection() {
        return new ExampleCollection(handler);
    }
}
```

#### Implementing VectorSearchable Interface

```java
package com.sqlrec.connectors.example.calcite;

import com.sqlrec.common.schema.SqlRecKvTable;
import com.sqlrec.common.schema.VectorSearchable;
import org.apache.calcite.rex.RexNode;
import java.util.*;

public class ExampleVectorTable extends SqlRecKvTable implements VectorSearchable {
    private final ExampleConfig config;
    private final ExampleHandler handler;

    public ExampleVectorTable(ExampleConfig config) {
        this.config = config;
        this.handler = new ExampleHandler(config);
    }

    @Override
    public List<VectorSearchResult> searchByEmbeddingImpl(VectorSearchRequest request) {
        return handler.vectorSearch(request);
    }

    // ... other necessary method implementations
}
```

### Step 4: Create Table Factory Class

Create a table factory class for creating table instances:

```java
package com.sqlrec.connectors.example.calcite;

import com.sqlrec.common.schema.HmsTableFactory;
import com.sqlrec.connectors.example.config.ExampleConfig;
import com.sqlrec.connectors.example.config.ExampleOptions;
import org.apache.calcite.plan.RelOptRule;
import org.apache.hadoop.hive.metastore.api.Table;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ExampleCalciteTableFactory implements HmsTableFactory {
    @Override
    public org.apache.calcite.schema.Table getTableFromHmsTable(Table table) {
        Map<String, String> parameters = table.getParameters();
        ExampleConfig config = ExampleOptions.getExampleConfig(parameters);
        config.fieldSchemas = table.getSd().getCols();
        config.primaryKeyIndex = getPrimaryKeyIndex(table);
        return new ExampleCalciteTable(config);
    }

    @Override
    public String getConnectorName() {
        return ExampleOptions.CONNECTOR_IDENTIFIER;
    }

    @Override
    public List<RelOptRule> getRules() {
        return new ArrayList<>();
    }

    private int getPrimaryKeyIndex(Table table) {
        // Get primary key index from table properties
        String pk = table.getParameters().get("primary-key");
        if (pk == null) {
            return 0;
        }
        // Find primary key column index
        for (int i = 0; i < table.getSd().getCols().size(); i++) {
            if (table.getSd().getCols().get(i).getName().equals(pk)) {
                return i;
            }
        }
        return 0;
    }
}
```

### Step 5: Register Service

Create a service registration file in the `META-INF/services` directory:

**File Path**: `src/main/resources/META-INF/services/com.sqlrec.common.schema.HmsTableFactory`

**File Content**:
```
com.sqlrec.connectors.example.calcite.ExampleCalciteTableFactory
```

## Table Object Lifecycle

### Global Sharing Mechanism

Table objects in SQLRec are globally shared and managed by `HmsSchema`. This means:

1. **Singleton Pattern**: Each table definition creates only one Table instance, shared by all queries
2. **Caching Mechanism**: Table objects are cached to avoid repeated creation
3. **Lifecycle Management**: Table object lifecycle is managed by the SQLRec framework

### Thread Safety Requirements

Since Table objects are globally shared, custom connectors must ensure thread safety:

1. **Stateless Design**: Table classes should be designed as stateless as possible, avoid using instance variables to store query state
2. **Thread-Safe Data Structures**: If instance variables must be used, use thread-safe data structures

**Incorrect Example** (Not Thread-Safe):

```java
public class UnsafeCalciteTable extends SqlRecKvTable {
    private List<Object[]> queryResult;  // Danger: instance variable stores query result

    @Override
    protected Enumerable<Object[]> scanImpl(List<RexNode> filters) {
        queryResult = handler.query(filters);  // Concurrency issue: multiple queries will overwrite each other
        return Linq4j.asEnumerable(queryResult);
    }
}
```

**Correct Example** (Thread-Safe):

```java
public class SafeCalciteTable extends SqlRecKvTable {
    private final ExampleHandler handler;  // Safe: immutable handler reference

    @Override
    protected Enumerable<Object[]> scanImpl(List<RexNode> filters) {
        List<Object[]> queryResult = handler.query(filters);  // Safe: local variable
        return Linq4j.asEnumerable(queryResult);
    }
}
```

### Connection Resource Management

For connectors that need to manage connection resources, it is recommended to use lazy initialization for connections. To avoid creating a connection for each table, you can share connections between different tables or use a connection pool.

## Best Practices

### 1. Connection Management

- Use connection pools to manage database connections
- Implement lazy loading and automatic reconnection
- Release connection resources when table is closed

### 2. Caching Strategy

- For `SqlRecKvTable`, use local cache appropriately
- Set appropriate cache size and expiration time
- Consider cache consistency issues

### 3. Error Handling

- Provide clear error messages
- Distinguish between temporary and permanent errors
- Implement retry mechanisms for temporary errors

### 4. Performance Optimization

- Use batch operations to reduce network overhead
- Implement projection pushdown, query only needed columns
- Implement filter pushdown, filter data at the data source side

### 5. Type Mapping

- Handle data type conversions correctly
- Support NULL value handling
- Handle data source specific types

## Example Project Structure

```
sqlrec-connector-example/
├── pom.xml
└── src/
    └── main/
        ├── java/
        │   └── com/
        │       └── sqlrec/
        │           └── connectors/
        │               └── example/
        │                   ├── calcite/
        │                   │   ├── ExampleCalciteTable.java
        │                   │   └── ExampleCalciteTableFactory.java
        │                   ├── config/
        │                   │   ├── ExampleConfig.java
        │                   │   └── ExampleOptions.java
        │                   └── handler/
        │                       └── ExampleHandler.java
        └── resources/
            └── META-INF/
                └── services/
                    └── com.sqlrec.common.schema.HmsTableFactory
```

## Reference Implementations

You can refer to the following built-in connector implementations:

- **Redis Connector**: `sqlrec-connector-redis` - Complete implementation of `SqlRecKvTable`
- **Milvus Connector**: `sqlrec-connector-milvus` - Complete implementation of `VectorSearchable`
- **Kafka Connector**: `sqlrec-connector-kafka` - Simple implementation of `SqlRecTable`
