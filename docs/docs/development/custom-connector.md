# 自定义 Connector

自定义 Connector 用于把新的存储系统接入 SQLRec。一个完整实现通常包含：

1. 配置项和解析后的配置对象；
2. 对外部系统进行读写的 Handler/Client；
3. Calcite Table 实现；
4. `HmsTableFactory` 工厂；
5. Java SPI 注册文件。

本页介绍当前接口约定和实现顺序。最稳妥的做法是复制一个能力接近的内置模块，再用自己的客户端和配置替换数据访问部分。

## 选择参考实现

| 需求 | 建议参考 |
|------|----------|
| 主键查询、本地缓存 | `sqlrec-connector-redis` |
| 关系数据库、复杂过滤下推 | `sqlrec-connector-jdbc` |
| 文档数据库 | `sqlrec-connector-mongodb` |
| 向量检索 | `sqlrec-connector-milvus` |
| 只写数据 | `sqlrec-connector-kafka` |
| 最小的可读写 KV 实现 | `sqlrec-connector-filesystem` |

## 选择 Table 能力

### SqlRecTable

`SqlRecTable` 是最小基类。如果数据源不适合主键访问，可以继承该类，再按需实现 Calcite 的 `ScannableTable`、`FilterableTable` 或 `ModifiableTable`。Kafka Connector 就属于这类实现。

### SqlRecKvTable

需要主键批量查询、过滤和写入时，继承 `SqlRecKvTable`。必须实现：

```java
protected Enumerable<Object[]> scanImpl(List<RexNode> filters);

public Map<Object, List<Object[]>> getByPrimaryKeyImpl(
    Set<Object> keySet
);

public RelDataType getRowType(RelDataTypeFactory typeFactory);

public Collection getModifiableCollection();

public int getPrimaryKeyIndex();
```

`scanImpl()` 负责扫描或处理能下推的过滤。`getByPrimaryKeyImpl()` 应一次批量查询所有 key，避免为每个 key 单独建立网络请求。

如果数据源只支持主键过滤，保持 `onlyFilterByPrimaryKey()` 的默认值。如果能够正确处理更复杂的过滤，可以像 JDBC 或 MongoDB Connector 一样覆盖它。

### VectorSearchable

支持向量检索的 Table 额外实现：

```java
public interface VectorSearchable {
    List<VectorSearchResult> searchByEmbeddingImpl(
        VectorSearchRequest request
    );
}
```

`VectorSearchable` 是可选能力，不是 Table 基类。当前 Milvus Table 同时继承 `SqlRecKvTable` 并实现该接口。

## 实现步骤

### 1. 定义配置

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

建议将建表属性解析到独立的配置类。配置类除了 Connector 自身的字段，通常还需保存：

```java
public List<FieldSchema> fieldSchemas;
public String primaryKey;
public Integer primaryKeyIndex;
```

对必填参数在建表时就抛出清晰错误，不要等到第一次查询时才失败。

### 2. 实现数据访问层

把 SDK、连接池、序列化和重试放在 Handler 或 Client 中，不要把所有外部存储逻辑写进 Calcite Table。数据访问层至少应覆盖：

- 批量主键读取；
- 扫描或可下推过滤；
- 批量写入/Upsert 和删除（如果支持）；
- 类型转换、`NULL` 值和资源释放；
- 超时、可重试错误和永久错误的区分。

### 3. 实现 Table

以 Filesystem Connector 的结构为例：

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

上述片段展示 Table 与 Handler 的分工，`ExampleCollection` 需要像内置 Connector 一样继承 `SqlRecCollection` 并实现真实的 Upsert/Delete。它不是可独立编译的完整 Connector。

### 4. 实现工厂

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

    @Override
    public List<RelOptRule> getRules() {
        return Collections.emptyList();
    }
}
```

使用 `HiveTableUtils.getFlinkTableOptions()` 读取 `WITH` 属性，使用其他 `HiveTableUtils` 方法读取 SQL 表字段和主键，不要假设主键会以自定义的 `primary-key` 参数存在。

### 5. 注册 SPI

创建文件：

```text
src/main/resources/META-INF/services/com.sqlrec.common.schema.HmsTableFactory
```

内容为工厂实现类的全限定名：

```text
com.example.sqlrec.connector.ExampleCalciteTableFactory
```

将 Connector JAR 和运行时依赖放入 SQLRec classpath 并重启所有实例。

## 线程安全和连接管理

Table 实例会在多个查询之间共享。因此：

- 不要把当前查询的 filters、返回行或分页状态保存为 Table 成员变量。
- 客户端必须是线程安全的，或由连接池管理。
- 尽量在相同连接配置的表之间复用客户端/连接池。
- 为连接、查询和写入设置有界超时，避免阻塞 SQLRec 执行线程。
- 启用 `SqlRecKvTable` 本地缓存前，先确认过期时间和数据一致性要求。

## 建议的项目结构

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

## 发布前检查

- 用合法和缺失的建表参数测试配置校验。
- 测试主键批量查询，包括不存在的 key 和类型转换。
- 测试能下推和不能下推的过滤条件。
- 测试 `INSERT`、`UPDATE`、`DELETE` 后的缓存失效。
- 进行并发查询和客户端断开/恢复测试。
- 确认 SPI 文件已被打包到 JAR，且 Connector 标识符没有与其他实现冲突。

接口和基类可能随项目演进，开发时以 `sqlrec-common` 和内置 Connector 的当前源码为准。
