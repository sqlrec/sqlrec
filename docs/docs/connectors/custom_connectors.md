# 自定义 Connectors

本文档介绍如何开发自定义连接器以扩展 SQLRec 的数据源支持。

## 概述

SQLRec 提供了灵活的连接器扩展机制，允许开发者实现自定义的数据源连接器。通过实现特定的接口和继承基础类，可以快速接入新的数据存储系统。

开始开发前，请先阅读 [Connector 基础概念](./basic_concepts.md)，根据访问模式选择 `SqlRecTable`、`SqlRecKvTable` 以及可选的 `VectorSearchable` 能力。

## 开发自定义连接器

### 步骤 1：创建配置类

创建配置类来存储连接器的配置参数：

```java
package com.sqlrec.connectors.example.config;

public class ExampleConfig {
    public String host;
    public int port;
    public String database;
    public int timeout;
}
```

### 步骤 2：创建配置选项类

创建配置选项类来定义配置参数：

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

### 步骤 3：实现表类

根据需求选择合适的基类并实现表类：

#### 实现 SqlRecTable

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

    // ... 其他必要的方法实现
}
```

#### 实现 SqlRecKvTable

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
        // 实现过滤查询逻辑
        List<Object[]> results = handler.scan(filters);
        return Linq4j.asEnumerable(results);
    }

    @Override
    public Collection getModifiableCollection() {
        return new ExampleCollection(handler);
    }
}
```

#### 实现 VectorSearchable 接口

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

    // ... 其他必要的方法实现
}
```

### 步骤 4：创建表工厂类

创建表工厂类用于创建表实例：

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
        // 从表属性中获取主键索引
        String pk = table.getParameters().get("primary-key");
        if (pk == null) {
            return 0;
        }
        // 查找主键列索引
        for (int i = 0; i < table.getSd().getCols().size(); i++) {
            if (table.getSd().getCols().get(i).getName().equals(pk)) {
                return i;
            }
        }
        return 0;
    }
}
```

### 步骤 5：注册服务

在 `META-INF/services` 目录下创建服务注册文件：

**文件路径**：`src/main/resources/META-INF/services/com.sqlrec.common.schema.HmsTableFactory`

**文件内容**：
```
com.sqlrec.connectors.example.calcite.ExampleCalciteTableFactory
```

## Table 对象生命周期

### 全局共享机制

SQLRec 中的 Table 对象是全局共享的，由 `HmsSchema` 统一管理。这意味着：

1. **单例模式**：每个表定义只会创建一个 Table 实例，所有查询共享同一个实例
2. **缓存机制**：Table 对象会被缓存，避免重复创建
3. **生命周期管理**：Table 对象的生命周期由 SQLRec 框架管理

### 并发安全要求

由于 Table 对象是全局共享的，自定义连接器必须保证线程安全：

1. **无状态设计**：Table 类应尽量设计为无状态，避免使用实例变量存储查询状态
2. **线程安全的数据结构**：如果必须使用实例变量，应使用线程安全的数据结构

**错误示例**（非线程安全）：

```java
public class UnsafeCalciteTable extends SqlRecKvTable {
    private List<Object[]> queryResult;  // 危险：实例变量存储查询结果

    @Override
    protected Enumerable<Object[]> scanImpl(List<RexNode> filters) {
        queryResult = handler.query(filters);  // 并发问题：多个查询会互相覆盖
        return Linq4j.asEnumerable(queryResult);
    }
}
```

**正确示例**（线程安全）：

```java
public class SafeCalciteTable extends SqlRecKvTable {
    private final ExampleHandler handler;  // 安全：不可变的 handler 引用

    @Override
    protected Enumerable<Object[]> scanImpl(List<RexNode> filters) {
        List<Object[]> queryResult = handler.query(filters);  // 安全：局部变量
        return Linq4j.asEnumerable(queryResult);
    }
}
```

### 连接资源管理

对于需要管理连接资源的连接器，建议使用懒加载方式初始化连接。为了避免每个表都创建一个连接，可以在不同表之间共享连接，或者使用连接池。

## 最佳实践

### 1. 连接管理

- 使用连接池管理数据库连接
- 实现连接的懒加载和自动重连
- 在表关闭时释放连接资源

### 2. 缓存策略

- 对于 `SqlRecKvTable`，合理使用本地缓存
- 设置合适的缓存大小和过期时间
- 考虑缓存一致性问题

### 3. 错误处理

- 提供清晰的错误信息
- 区分临时错误和永久错误
- 实现重试机制处理临时错误

### 4. 性能优化

- 使用批量操作减少网络开销
- 实现投影下推，只查询需要的列
- 实现过滤下推，在数据源侧过滤数据

### 5. 类型映射

- 正确处理数据类型转换
- 支持 NULL 值处理
- 处理数据源特有的类型

## 示例项目结构

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

## 参考实现

可以参考以下内置连接器的实现：

- **Redis Connector**：`sqlrec-connector-redis` - `SqlRecKvTable` 的完整实现
- **Milvus Connector**：`sqlrec-connector-milvus` - `VectorSearchable` 接口的完整实现
- **Kafka Connector**：`sqlrec-connector-kafka` - `SqlRecTable` 的简单实现
