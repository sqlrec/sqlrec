# SQLRec 编程模型

SQLRec 是一种基于 SQL 的数据处理和机器学习编程框架。它扩展了标准 SQL，引入了变量、函数、缓存表等编程概念，使得 SQL 具备了类似编程语言的能力。

## 表类型系统

SQLRec 定义了一套表类型层次结构，不同类型的表具有不同的访问特性。

### 类型层次

```
SqlRecTable (抽象基类)
├── CacheTable          -- 内存缓存表
└── SqlRecKvTable       -- KV 表（支持主键查询）
    └── SqlRecVectorTable -- 向量表（支持向量搜索）
```

### SqlRecTable

`SqlRecTable` 是所有 SQLRec 表的抽象基类，继承自 Calcite 的 `AbstractTable`。所有 SQLRec 自定义表类型都必须继承此类。

```java
public abstract class SqlRecTable extends AbstractTable {
}
```

### CacheTable

`CacheTable` 是内存中的数据表，是 SQLRec 中最常用的表类型。

**特性：**
- 数据存储在内存中
- 支持快速随机访问
- 会话级别生命周期
- 通过 `CACHE TABLE` 语句创建

**使用场景：**
- 函数参数传递
- 中间计算结果存储
- 函数返回值

```sql
-- 创建 CacheTable
CACHE TABLE my_cache AS
SELECT * FROM source_table WHERE condition;
```

### SqlRecKvTable

`SqlRecKvTable` 是支持主键查询的 KV 表，实现了 `ModifiableTable` 和 `FilterableTable` 接口。

**特性：**
- 支持按主键高效查询
- 内置 Caffeine 缓存
- 支持批量主键查询

**核心方法：**

| 方法 | 说明 |
|------|------|
| `getPrimaryKeyIndex()` | 获取主键列索引 |
| `getByPrimaryKey(Set<Object> keySet)` | 按主键批量查询 |
| `getByPrimaryKeyWithCache(Set<Object> keySet)` | 带缓存的批量查询 |
| `initCache(int maxSize, long expireAfterWrite)` | 初始化查询缓存 |

**缓存配置：**

```java
// 初始化缓存：最大 10000 条，写入后 60 秒过期
kvTable.initCache(10000, 60);
```

### SqlRecVectorTable

`SqlRecVectorTable` 继承自 `SqlRecKvTable`，增加了向量搜索能力。

**特性：**
- 继承 KV 表的所有能力
- 支持向量相似度搜索
- 支持 ANN（近似最近邻）查询

**核心方法：**

```java
public abstract class SqlRecVectorTable extends SqlRecKvTable {
    public abstract List<String> getFieldNames();
    
    public List<Object[]> searchByEmbeddingWithScore(
            Object[] leftValue,      // 左表连接值
            List<Float> embedding,   // 查询向量
            String annFieldName,     // 向量字段名
            RexNode filterCondition, // 过滤条件
            int limit,               // 返回数量限制
            List<Integer> projectColumns  // 投影列
    );
}
```

**使用场景：**
- 向量相似度搜索
- 语义检索
- 推荐系统


## SQL 执行路由

SQLRec 作为 SQL 网关，需要决定哪些 SQL 在本地执行，哪些转发到后端引擎（如 Flink SQL Gateway）。

### 路由决策流程

```
SQL 请求
    │
    ▼
解析 SQL → 判断 SQL 类型
    │
    ├─── SQLRec 扩展语法 ──→ 本地执行
    │    ├── CREATE MODEL / DROP MODEL / TRAIN MODEL / EXPORT MODEL
    │    ├── CREATE SERVICE / DROP SERVICE
    │    ├── CREATE API / DROP API
    │    ├── CREATE SQL FUNCTION / DROP SQL FUNCTION
    │    ├── CACHE TABLE
    │    ├── CALL
    │    └── SHOW / DESCRIBE 语句
    │
    ├─── CRUD SQL ──→ 检查表类型
    │    │
    │    ├── 所有表都是 SqlRecTable ──→ 本地执行
    │    │
    │    └── 包含非 SqlRecTable ──→ 转发到 Flink
    │
    └─── 其他 SQL ──→ 转发到 Flink
```

### 本地执行的 SQL 类型

| SQL 类型 | 说明 |
|----------|------|
| `SqlCreateModel` | 创建模型 |
| `SqlDropModel` | 删除模型 |
| `SqlTrainModel` | 训练模型 |
| `SqlExportModel` | 导出模型 |
| `SqlCreateService` | 创建服务 |
| `SqlDropService` | 删除服务 |
| `SqlCreateApi` | 创建 API |
| `SqlDropApi` | 删除 API |
| `SqlCreateSqlFunction` | 创建 SQL 函数 |
| `SqlDropSqlFunction` | 删除 SQL 函数 |
| `SqlCache` | 缓存表 |
| `SqlCallSqlFunction` | 调用函数 |
| `SqlSet` | 设置变量 |
| `SqlShowTables` | 显示表列表 |
| `SqlShowSqlFunction` | 显示函数列表 |
| `SqlShowApi` | 显示 API 列表 |
| `SqlShowModel` | 显示模型列表 |
| `SqlShowService` | 显示服务列表 |
| `SqlShowCheckpoint` | 显示检查点列表 |
| `SqlRichDescribeTable` | 描述表结构 |
| `SqlShowCreateTable` | 显示建表语句 |

### CRUD SQL 的路由判断

对于 SELECT、INSERT、UPDATE、DELETE 等 CRUD 语句，系统会检查所有涉及的表：

```java
public static boolean isSqlTableRunable(SqlNode sqlNode, CalciteSchema schema, String defaultSchema) {
    List<String> tableNames = getTableFromSqlNode(sqlNode);
    for (String tableName : tableNames) {
        Table table = getTableObj(schema, defaultSchema, tableName);
        if (!(table instanceof SqlRecTable)) {
            return false;  // 转发到 Flink
        }
    }
    return true;  // 本地执行
}
```

**判断规则：**
- 所有表都是 `SqlRecTable` 子类 → 本地执行
- 包含非 `SqlRecTable`（如 Hive 表）→ 转发到 Flink

### UNION 语句的特殊处理

UNION 语句会被识别为特殊的 CRUD SQL：

```java
public static boolean isUnionSql(SqlNode sqlNode) {
    if (sqlNode instanceof SqlBasicCall) {
        SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
        if (sqlBasicCall.getOperator() instanceof SqlSetOperator) {
            SqlSetOperator sqlSetOperator = (SqlSetOperator) sqlBasicCall.getOperator();
            return sqlSetOperator.getKind() == SqlKind.UNION;
        }
    }
    return false;
}
```


## SQL 执行逻辑

SQLRec 根据表类型的不同，支持不同的 SQL 查询能力。本节介绍各类表支持的查询操作及其实现原理。

### 表类型与查询能力矩阵

| 表类型 | 过滤查询 | 主键查询 | KV Join | 向量搜索 |
|--------|----------|----------|---------|----------|
| CacheTable | ✅ | ❌ | ❌ | ❌ |
| SqlRecKvTable | ✅ | ✅ | ✅ | ❌ |
| SqlRecVectorTable | ✅ | ✅ | ✅ | ✅ |

> **写入操作支持**：表是否支持写入（INSERT/UPDATE/DELETE）取决于是否实现 `ModifiableTable` 接口，而非表类型本身。例如 `SqlRecKvTable` 和 `KafkaCalciteTable` 都实现了 `ModifiableTable`，因此支持写入操作。

### CACHE TABLE 语句

`CACHE TABLE` 是 SQLRec 中最核心的语句，用于创建内存缓存表，类似于编程语言中的变量赋值。

#### 基本语法

```sql
CACHE TABLE table_name AS
SELECT * FROM source_table WHERE condition;
```

这行代码的含义是：
1. 执行 `SELECT` 查询
2. 将结果存储在名为 `table_name` 的内存表中
3. 后续 SQL 可以引用该表

#### 缓存表特性

缓存表可以被视为"表变量"，具有以下特性：

- **作用域**：在当前会话中全局可见
- **生命周期**：会话结束时自动销毁
- **类型**：表类型，包含列定义和数据行

#### 链式处理

```sql
CACHE TABLE step1 AS
SELECT user_id, COUNT(*) as cnt FROM events GROUP BY user_id;

CACHE TABLE step2 AS
SELECT * FROM step1 WHERE cnt > 10;

CACHE TABLE final_result AS
SELECT * FROM step2 ORDER BY cnt DESC;
```

#### 通过函数调用创建

缓存表可以通过函数调用创建：

```sql
CACHE TABLE processed_data AS
CALL process_function(raw_data, config_table);
```

### 过滤查询

所有 SqlRecTable 子类都支持过滤查询。系统通过 `FilterableTableScan` 节点实现过滤条件下推。

#### 过滤条件下推规则

```java
// SqlRecFilterTableScanRule
public static boolean test(TableScan scan) {
    final RelOptTable table = scan.getTable();
    return table.unwrap(FilterableTable.class) != null
            || table.unwrap(ProjectableFilterableTable.class) != null;
}
```

#### KV 表的主键过滤优化

对于 `SqlRecKvTable`，如果设置了 `onlyFilterByPrimaryKey()` 为 true，则只支持主键过滤：

```java
private boolean shouldFilterByPrimaryKey(SqlRecTable sqlRecTable) {
    if (sqlRecTable == null) return false;
    if (!(sqlRecTable instanceof SqlRecKvTable)) return false;
    SqlRecKvTable kvTable = (SqlRecKvTable) sqlRecTable;
    return kvTable.onlyFilterByPrimaryKey();
}
```

**示例：**

```sql
-- 对于 onlyFilterByPrimaryKey=true 的 KV 表，以下查询有效
SELECT * FROM kv_table WHERE primary_key = 'key123';

-- 以下查询无效（非主键过滤）
SELECT * FROM kv_table WHERE other_column = 'value';
```

### KV Join

KV Join 是 SqlRecKvTable 特有的连接方式，通过主键批量查询实现高效关联。

#### 触发条件

1. **左表必须是 CacheTable**（内存中的数据，可被遍历）
2. Join 条件必须是**等值条件**（`=`）
3. 右表必须是 `SqlRecKvTable`

```java
// SqlRecKvJoinRule 检查条件
RexNode condition = join.getCondition();
try {
    KvJoinUtils.getJoinKeyColIndex(condition);
} catch (Exception e) {
    return; // 非等值条件，不应用此规则
}
```

#### 实现原理

KV Join 的核心是通过主键批量查询右表数据：

```java
// KvJoinUtils.kvJoin
public static Enumerable kvJoin(
        Enumerable left,
        SqlRecKvTable rightTable,
        RexNode condition,
        JoinRelType joinType
) {
    // 1. 提取左表的所有 Join Key
    Set<Object> joinKeys = new HashSet<>();
    for (Object[] leftValue : leftValues) {
        Object leftJoinKey = leftValue[leftJoinKeyColIndex];
        joinKeys.add(leftJoinKey);
    }
    
    // 2. 批量查询右表数据（利用缓存）
    Map<Object, List<Object[]>> rightValuesMap = 
        rightTable.getByPrimaryKeyWithCache(joinKeys);
    
    // 3. 关联左右表数据
    // ...
}
```

#### 支持的 Join 类型

| Join 类型 | 说明 |
|-----------|------|
| INNER JOIN | 只返回匹配的行 |
| LEFT JOIN | 左表全部返回，右表无匹配时填充 NULL |

**示例：**

```sql
-- KV Join 示例
SELECT o.*, u.user_name
FROM orders o
LEFT JOIN user_kv_table u ON o.user_id = u.user_id;
```

### 向量搜索 Join

向量搜索 Join 是 SqlRecVectorTable 特有的连接方式，通过向量相似度进行关联。

#### 触发条件

1. **左表必须是 CacheTable**（内存中的数据，可被遍历）
2. Project 中必须包含 **`ip()` 函数**（向量内积）
3. Join 条件必须为 **true**（无条件连接）
4. 右表必须是 `SqlRecVectorTable`
5. 必须有 **ORDER BY ... LIMIT** 子句

```java
// SqlRecVectorJoinRule 检查条件
if (!VectorJoinUtils.hasIpFunction(project)) {
    return; // 必须有 ip 函数
}
if (!VectorJoinUtils.isTrueCondition(join)) {
    return; // Join 条件必须为 true
}
if (rightTable.unwrap(SqlRecVectorTable.class) == null) {
    return; // 右表必须是向量表
}
```

#### 查询模式

向量搜索 Join 的典型查询模式：

```sql
SELECT 
    left.*,
    ip(left.embedding, right.embedding) as score
FROM left_table left
JOIN vector_table right ON true
WHERE right.category = 'electronics'  -- 可选的过滤条件
ORDER BY score DESC
LIMIT 10;
```

#### 实现原理

```java
// VectorJoinUtils.vectorJoin
public static Enumerable vectorJoin(
        Enumerable left,
        SqlRecVectorTable rightTable,
        RexNode filterCondition,      // 右表过滤条件
        int leftEmbeddingColIndex,    // 左表向量列索引
        String rightEmbeddingColName, // 右表向量列名
        int limit,                    // 返回数量
        List<Integer> projectColumns  // 投影列
) {
    for (Object[] leftValue : leftValues) {
        // 1. 提取左表的查询向量
        List<Float> embedding = DataTransformUtils.convertToFloatVec(
            leftValue[leftEmbeddingColIndex]
        );
        
        // 2. 向量相似度搜索
        List<Object[]> rightValues = rightTable.searchByEmbeddingWithScore(
            leftValue,
            embedding,
            rightEmbeddingColName,
            filterCondition,
            limit,
            vectorProjectColumns
        );
        
        // 3. 关联结果
        // ...
    }
}
```

#### 配置参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `DEFAULT_VECTOR_SEARCH_LIMIT` | 默认返回数量 | 配置项 |

### UNION 操作

UNION 操作通过 `EnumerableUnion` 实现，使用蛇形合并算法。

#### 实现方式

```java
// EnumerableUnion.implement
Expression unionExp = Expressions.call(
    MergeUtils.class.getMethod("snakeMergeEnumerable", Iterable[].class), 
    inputExps
);
```

#### 蛇形合并算法

蛇形合并是一种高效的流式合并算法，适用于多数据源的合并场景：

```java
// MergeUtils.snakeMergeEnumerable
public static List<Object[]> snakeMergeEnumerable(Iterable<Object[]>... iterables) {
    // 蛇形遍历所有输入源，交替输出
}
```

## 变量系统

SQLRec 通过 `ExecuteContext` 管理运行时变量，提供类似编程语言中变量的能力。

### 变量设置

使用 `SET` 语句或 API 设置变量：

```sql
SET 'my_var' = 'my_value';
```

```java
context.setVariable("my_var", "my_value");
```

### 变量获取

使用 `GET()` 表达式获取变量：

```sql
-- 在函数调用中使用
CALL my_function(GET('table_name'));
```

### 变量作用域

| 特性 | 说明 |
|------|------|
| **存储** | `ConcurrentHashMap`（线程安全） |
| **可见性** | 当前会话全局可见 |
| **隔离性** | 不同会话之间变量隔离 |

### 函数调用时的变量

函数调用时会创建新的执行上下文：

```java
ExecuteContext finalContext = context.clone();
finalContext.addFunNameToStack(funName);
```

- **变量共享**：克隆的上下文共享变量映射
- **调用栈隔离**：每个函数调用有独立的调用栈


## 函数系统

SQLRec 支持自定义 SQL 函数，函数是一组 SQL 语句的封装，类似于编程语言中的函数定义。

### 函数定义

一个完整的函数定义包含以下部分：

```sql
-- 1. 函数声明
CREATE SQL FUNCTION my_function;

-- 2. 参数定义（可选，可定义多个）
DEFINE INPUT TABLE input_data (
    id INT,
    name VARCHAR(100),
    score DOUBLE
);

DEFINE INPUT TABLE config_table (
    threshold DOUBLE
);

-- 3. 函数体（多个 SQL 语句）
CACHE TABLE filtered AS
SELECT * FROM input_data WHERE score > (SELECT threshold FROM config_table LIMIT 1);

CACHE TABLE result AS
SELECT id, name, score FROM filtered ORDER BY score DESC;

-- 4. 返回语句
RETURN result;
```

### 函数传参

SQLRec 函数采用**按值传递**的方式，参数是表（CacheTable）或变量。

#### 基本调用

```sql
CALL my_function(table1, table2);
```

#### 参数匹配

调用时传入的表必须与函数定义的输入表结构兼容：

```sql
-- 函数定义
CREATE SQL FUNCTION process_data;
DEFINE INPUT TABLE input_data (id INT, value DOUBLE);
...
RETURN result;

-- 调用时，my_table 的结构必须与 input_data 兼容
CALL process_data(my_table);
```

#### 动态函数名

使用 `GET()` 表达式动态获取函数名：

```sql
-- 从变量获取函数名
CALL GET('function_name')(table1, table2) LIKE template_table;
```

动态调用函数时，需要使用 `LIKE` 子句指定结果表的模式，因为编译期无法知道调用的哪个函数，无法推断类型。

#### 异步调用

使用 `ASYNC` 关键字异步执行函数：

```sql
CALL my_function(input_table) ASYNC;
```

异步调用会立即返回，函数在后台线程执行。适用于不需要立即获取结果的场景。

### 函数返回结果

函数通过 `RETURN` 语句返回结果。

#### 基本返回

```sql
RETURN result_table;
```

返回的必须是 CacheTable，通常是函数体中创建的某个缓存表。

#### 空返回

```sql
RETURN;
```

函数可以不返回任何结果，此时函数仅执行副作用（如写入数据）。

#### 返回值的使用

函数返回值可以直接使用或存储到缓存表：

```sql
-- 直接使用返回值（在 CACHE TABLE 中）
CACHE TABLE output AS
CALL my_function(input_table);

-- 返回值是一个可枚举的结果集
```


## 并发模型

SQLRec 内置了自动并行执行能力，能够自动分析 SQL 语句之间的依赖关系并并行执行。

- **读依赖**：语句读取某个表
- **写依赖**：语句写入某个表（如 CACHE TABLE）
- **变量依赖**：SET 语句、使用了ExecuteContext的UDF、使用了变量的函数调用之间存在变量依赖

```sql
-- 这两个语句可以并行执行（无依赖）
CACHE TABLE a AS SELECT * FROM source1;
CACHE TABLE b AS SELECT * FROM source2;

-- 这个语句必须等待前两个完成
CACHE TABLE c AS SELECT * FROM a UNION ALL SELECT * FROM b;
```

## 循环依赖检测

系统通过调用栈检测函数之间的循环依赖，防止无限递归。 

函数调用时会将函数名压入调用栈：

```java
ExecuteContext finalContext = context.clone();
finalContext.addFunNameToStack(funName);
```

在调用新函数前，检查调用栈中是否已存在该函数：

```java
if (funNameStack.contains(funName)) {
    throw new RuntimeException("Circular dependency detected: " + funName);
}
```

## UDF

SQLRec 支持通过 Java 实现用户定义函数（UDF），可以在 SQL 中直接调用。

### UDF 定义

UDF 是一个普通的 Java 类，需要满足以下条件：

1. **必须有一个 `eval` 方法**：这是 UDF 的入口点
2. **`eval` 方法只能有一个**：不支持重载
3. **参数类型限制**：只支持 `CacheTable`、`String`、`ExecuteContext` 三种类型

```java
public class MyTableFunction {
    
    public CacheTable eval(CacheTable inputTable, String config) {
        // 处理逻辑
        List<Object[]> results = processTable(inputTable, config);
        return new CacheTable("output", results, inputTable.getDataFields());
    }
}
```

### 参数注入

SQLRec 会根据 `eval` 方法的参数类型自动注入相应的值：

| 参数类型 | 注入来源 | SQL 语法 |
|----------|----------|----------|
| `CacheTable` | 传入的缓存表 | 标识符（如 `table_name`） |
| `String` | 字符串字面量或变量 | `'value'` 或 `GET('var')` |
| `ExecuteContext` | 执行上下文 | 自动注入，无需在 SQL 中指定 |
| `ConfigContext` | 配置上下文 | 自动注入，无需在 SQL 中指定 |

参数注入示例

```java
public class MyFunction {
    // 方法签名
    public CacheTable eval(
        CacheTable input1,      // 第一个参数：表
        CacheTable input2,      // 第二个参数：表
        String config,          // 第三个参数：字符串
        ExecuteContext context  // 执行上下文（自动注入）
    ) {
        // ...
    }
}
```

```sql
-- 调用时，参数按顺序匹配
CALL my_function(table1, table2, 'config_value');

-- 使用 GET 获取字符串参数
CALL my_function(table1, table2, GET('config_var'));
```

### 编译期返回数据模式解析

UDF 的返回数据模式（Schema）可以在编译期确定，有以下两种方式：

#### 1. 通过 LIKE 子句指定

```sql
CALL my_function(input_table) LIKE template_table;
```

编译时，系统会从 `template_table` 获取返回数据模式：

```java
if (!StringUtils.isEmpty(likeTableName)) {
    returnDataFields = getDataTypeByLikeTableName(likeTableName, schema);
}
```

#### 2. 通过执行 eval 方法推断

如果没有 LIKE 子句，系统会在编译期执行一次 `eval` 方法来推断返回模式：

```java
if (!isAsync && CacheTable.class.isAssignableFrom(evalMethod.getReturnType())) {
    Object outputTable = callEvalMethod(schema, new ExecuteContextImpl());
    returnDataFields = ((CacheTable) outputTable).getDataFields();
}
```

**注意**：这种方式要求 UDF 在编译期能够正常执行，且不能是异步调用。

### UDF 注册

UDF 需要在 Hive Metastore（HMS）中注册才能被调用。注册时需要指定函数名和对应的 Java 类全限定名：

```sql
-- 在 HMS 中注册 UDF
CREATE FUNCTION my_function AS 'com.example.MyFunction';
```

系统在调用函数时会通过 HMS 获取函数的类名，然后动态加载：

```java
// 从 HMS 获取函数对象
org.apache.hadoop.hive.metastore.api.Function functionObj = HmsClient.getFunctionObj(db, funName);
// 获取类名并加载
String className = functionObj.getClassName();
Class<?> clazz = Class.forName(className);
```

### 函数查找优先级

当调用函数时，系统按以下顺序查找：

1. **Java UDF**：通过 `JavaFunctionUtils.getTableFunction()` 查找
2. **SQL 函数**：通过 `CompileManager.getSqlFunction()` 查找
3. **未找到**：抛出异常

## 编程模型总结

| 概念 | 类比传统编程 | SQLRec 实现 |
|------|-------------|-------------|
| 变量 | 变量赋值 | `CACHE TABLE` |
| 函数 | 函数定义 | `CREATE SQL FUNCTION` |
| 参数 | 函数参数 | `DEFINE INPUT TABLE` |
| 返回值 | return 语句 | `RETURN` |
| 函数调用 | 函数调用 | `CALL` |
| 动态分发 | 反射/动态加载 | `GET()` |
| 并发 | 多线程 | 自动并行 + 虚拟线程 |
| 作用域 | 变量作用域 | 会话级别 |
| 类型系统 | 静态类型 | 表结构检查 |
| UDF | 外部库/插件 | Java 类 + `eval` 方法 |
| 表类型 | 数据结构 | `SqlRecTable` 层次结构 |
| 执行路由 | 编译目标选择 | 本地执行 / 转发 Flink |
| 过滤查询 | 条件筛选 | `FilterableTableScan` + 规则优化 |
| KV Join | 主键关联查询 | `SqlRecKvJoinRule` + 主键批量查询 |
| 向量搜索 | 相似度匹配 | `SqlRecVectorJoinRule` + `ip()` 函数 |
| UNION | 数据合并 | `EnumerableUnion` + 蛇形合并算法 |

SQLRec 将 SQL 从声明式查询语言扩展为具备完整编程能力的语言，同时保持了 SQL 的简洁性和声明式特性。
