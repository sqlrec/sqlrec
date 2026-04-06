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


## 核心概念

### 1. Cache Table（缓存表）

Cache Table 是 SQLRec 中最核心的概念，它是内存中的数据表，类似于编程语言中的变量。

#### 定义缓存表

使用 `CACHE TABLE` 语句创建缓存表：

```sql
CACHE TABLE result AS
SELECT * FROM source_table WHERE status = 'active';
```

这行代码的含义是：
1. 执行 `SELECT` 查询
2. 将结果存储在名为 `result` 的内存表中
3. 后续 SQL 可以引用 `result` 表

#### 缓存表作为变量

缓存表可以被视为"表变量"，它具有以下特性：

- **作用域**：在当前会话中全局可见
- **生命周期**：会话结束时自动销毁
- **类型**：表类型，包含列定义和数据行

```sql
CACHE TABLE step1 AS
SELECT user_id, COUNT(*) as cnt FROM events GROUP BY user_id;

CACHE TABLE step2 AS
SELECT * FROM step1 WHERE cnt > 10;

CACHE TABLE final_result AS
SELECT * FROM step2 ORDER BY cnt DESC;
```

#### 缓存表与函数调用

缓存表可以通过函数调用创建：

```sql
CACHE TABLE processed_data AS
CALL process_function(raw_data, config_table);
```


### 2. 函数定义

SQLRec 支持自定义 SQL 函数，函数是一组 SQL 语句的封装。

#### 函数结构

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

#### 函数编译阶段

函数定义经历四个编译阶段：

| 阶段 | 说明 | 允许的语句 |
|------|------|-----------|
| `FUNCTION_DEFINITION` | 函数声明 | `CREATE SQL FUNCTION` |
| `FUNCTION_PARAM` | 参数定义 | `DEFINE INPUT TABLE` |
| `FUNCTION_BODY` | 函数体 | 任意 SQL 语句 |
| `FUNCTION_RETURN` | 返回 | `RETURN`（结束编译） |

#### 函数替换

使用 `OR REPLACE` 可以覆盖已存在的函数：

```sql
CREATE OR REPLACE SQL FUNCTION my_function;
```


### 3. 函数传参

SQLRec 函数采用**按值传递**的方式，参数是表（CacheTable）。

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
CALL GET('function_name')(table1, table2);

-- 嵌套使用 GET
CALL GET(GET('func_key'))(GET('table_var'));
```

#### LIKE 子句

`LIKE` 子句用于指定结果表的模板：

```sql
CALL my_function(input_table) LIKE template_table;
```

模板表定义了返回结果的结构，当函数返回的表结构与模板不匹配时会报错。

#### 异步调用

使用 `ASYNC` 关键字异步执行函数：

```sql
CALL my_function(input_table) ASYNC;
```

异步调用会立即返回，函数在后台线程执行。适用于不需要立即获取结果的场景。


### 4. 函数返回结果

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


### 5. 并发模型

SQLRec 内置了自动并行执行能力。

#### 自动依赖分析

系统自动分析 SQL 语句之间的依赖关系：

- **读依赖**：语句读取某个表
- **写依赖**：语句写入某个表（如 CACHE TABLE）

```sql
-- 这两个语句可以并行执行（无依赖）
CACHE TABLE a AS SELECT * FROM source1;
CACHE TABLE b AS SELECT * FROM source2;

-- 这个语句必须等待前两个完成
CACHE TABLE c AS SELECT * FROM a UNION ALL SELECT * FROM b;
```

#### 拓扑排序执行

系统使用拓扑排序确定执行顺序：

1. 分析所有语句的读写依赖
2. 构建依赖图
3. 按拓扑序执行
4. 无依赖的语句并行执行

#### 并行执行配置

```java
// 启用并行执行
SqlRecConfigs.PARALLELISM_EXEC.setValue(true);
```

#### 虚拟线程

SQLRec 使用 Java 虚拟线程实现并行执行：

```java
ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();
```

虚拟线程具有以下优势：
- 轻量级，可以创建大量线程
- 自动调度，无需手动管理线程池
- 适合 I/O 密集型任务

#### 并行执行示例

```sql
-- 假设 source1, source2, source3 是独立的 Hive 表

-- 以下三个语句会并行执行
CACHE TABLE part1 AS SELECT * FROM source1 WHERE region = 'north';
CACHE TABLE part2 AS SELECT * FROM source2 WHERE region = 'south';
CACHE TABLE part3 AS SELECT * FROM source3 WHERE region = 'east';

-- 这个语句等待前三个完成
CACHE TABLE all_parts AS
SELECT * FROM part1
UNION ALL SELECT * FROM part2
UNION ALL SELECT * FROM part3;
```


### 6. 变量可见性

SQLRec 通过 `ExecuteContext` 管理运行时变量。

#### 变量设置

使用 `SET` 语句或 API 设置变量：

```sql
SET 'my_var' = 'my_value';
```

```java
context.setVariable("my_var", "my_value");
```

#### 变量获取

使用 `GET()` 表达式获取变量：

```sql
-- 获取变量值
GET('my_var')

-- 在函数调用中使用
CALL my_function(GET('table_name'));

-- 在 CACHE TABLE 中使用
CACHE TABLE dynamic_table AS
SELECT * FROM GET('source_table');
```

#### 变量作用域

| 特性 | 说明 |
|------|------|
| **存储** | `ConcurrentHashMap`（线程安全） |
| **可见性** | 当前会话全局可见 |
| **隔离性** | 不同会话之间变量隔离 |

#### 函数调用时的变量

函数调用时会创建新的执行上下文：

```java
ExecuteContext finalContext = context.clone();
finalContext.addFunNameToStack(funName);
```

- **变量共享**：克隆的上下文共享变量映射
- **调用栈隔离**：每个函数调用有独立的调用栈

#### 循环依赖检测

系统通过调用栈检测循环依赖：

```sql
-- 如果 function_a 调用 function_b，function_b 又调用 function_a
-- 会抛出异常：Circular dependency detected
```

```java
if (funNameStack.contains(funName)) {
    throw new RuntimeException("Circular dependency detected: " + funName);
}
```


## UDF（用户定义函数）

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

#### 参数注入示例

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

#### 参数注入流程

```java
private Object callEvalMethod(CalciteSchema schema, ExecuteContext context) {
    Class<?>[] paramTypes = evalMethod.getParameterTypes();
    List<Object> paramList = new ArrayList<>();
    int inputParamIndex = 0;

    for (Class<?> paramType : paramTypes) {
        SqlNode input = inputTableList.get(inputParamIndex);
        
        if (paramType.equals(CacheTable.class)) {
            // 从 schema 获取缓存表
            paramList.add(getCacheTable(tableName, schema));
            inputParamIndex++;
            
        } else if (paramType.equals(String.class)) {
            // 从字面量或变量获取字符串
            if (input instanceof SqlCharStringLiteral) {
                paramList.add(getStringValue(input));
            } else if (input instanceof SqlGetVariable) {
                paramList.add(context.getVariable(varName));
            }
            inputParamIndex++;
            
        } else if (paramType.equals(ExecuteContext.class)) {
            // 自动注入执行上下文
            paramList.add(context);
        }
    }
    
    return evalMethod.invoke(tableFunction, paramList.toArray());
}
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

### 是否可并发判断

UDF 是否可以并发执行取决于是否使用 `ExecuteContext` 参数：

```java
@Override
public boolean isParallelizable() {
    Class<?>[] paramTypes = evalMethod.getParameterTypes();
    for (Class<?> paramType : paramTypes) {
        if (paramType.equals(ExecuteContext.class)) {
            return false;  // 使用 ExecuteContext 则不可并发
        }
    }
    return true;  // 否则可以并发
}
```

**设计原因：**
- `ExecuteContext` 包含调用栈信息，用于循环依赖检测
- 并发执行时，调用栈可能不一致
- 因此使用 `ExecuteContext` 的 UDF 必须串行执行

### 动态函数调用

SQLRec 支持在运行时动态确定要调用的函数，通过 `GET()` 表达式实现：

```sql
-- 从变量获取函数名
CALL GET('function_name')(input_table);

-- 配合 LIKE 子句使用
CALL GET('func_var')(input_table) LIKE template_table;

-- 异步调用动态函数
CALL GET('func_var')(input_table) ASYNC;
```

#### 实现原理

动态函数调用使用 `FunctionProxyBindable` 实现：

```java
public class FunctionProxyBindable extends BindableInterface {
    private SqlGetVariable funcNameVariable;  // 函数名变量
    
    @Override
    public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
        // 运行时从变量获取函数名
        String functionName = context.getVariable(variableName);
        
        // 根据函数名获取实际的 Bindable
        BindableInterface bindable = getFunctionBindableByName(
            functionName, schema, inputList, likeTableName, isAsync, compileManager
        );
        
        return bindable.bind(schema, context);
    }
}
```

**特点：**
- 编译期必须指定 `LIKE` 子句（用于确定返回模式）
- 运行时解析函数名
- 不支持并发执行（`isParallelizable()` 返回 `false`）

### UDF 注册

UDF 需要注册到系统中才能被调用：

```java
// 通过 JavaFunctionUtils 注册
JavaFunctionUtils.registerTableFunction(schemaName, functionName, functionInstance);
```

### 函数查找优先级

当调用函数时，系统按以下顺序查找：

1. **Java UDF**：通过 `JavaFunctionUtils.getTableFunction()` 查找
2. **SQL 函数**：通过 `CompileManager.getSqlFunction()` 查找
3. **未找到**：抛出异常

```java
public static BindableInterface getFunctionBindableByName(String functionName, ...) {
    // 1. 查找 Java UDF
    Object javaFunctionObj = JavaFunctionUtils.getTableFunction(schema, functionName);
    if (javaFunctionObj != null) {
        return new JavaFunctionBindable(functionName, javaFunctionObj, ...);
    }
    
    // 2. 查找 SQL 函数
    SqlFunctionBindable sqlFunction = compileManager.getSqlFunction(functionName);
    if (sqlFunction != null) {
        return new CallSqlFunctionBindable(functionName, inputTables, sqlFunction, isAsync);
    }
    
    // 3. 未找到
    throw new Exception("function not find: " + functionName);
}
```

### UDF 完整示例

```java
public class DataProcessor {
    
    public CacheTable eval(
        CacheTable inputTable,
        String filterCondition,
        ExecuteContext context
    ) {
        // 获取变量
        String threshold = context.getVariable("threshold");
        
        // 处理数据
        List<Object[]> results = new ArrayList<>();
        for (Object[] row : inputTable.scan(null)) {
            if (matchesCondition(row, filterCondition, threshold)) {
                results.add(processRow(row));
            }
        }
        
        // 返回结果
        return new CacheTable("output", 
            Linq4j.asEnumerable(results), 
            inputTable.getDataFields()
        );
    }
}
```

```sql
-- 设置变量
SET 'threshold' = '100';

-- 调用 UDF
CACHE TABLE result AS
CALL data_processor(input_data, 'value > 10') LIKE input_data;
```


## 完整示例

### 数据处理流水线

```sql
-- 定义一个完整的数据处理函数
CREATE SQL FUNCTION etl_pipeline;

-- 定义输入参数
DEFINE INPUT TABLE raw_events (
    event_id VARCHAR(100),
    user_id VARCHAR(100),
    event_type VARCHAR(50),
    event_time TIMESTAMP,
    payload VARCHAR(1000)
);

DEFINE INPUT TABLE user_profiles (
    user_id VARCHAR(100),
    user_name VARCHAR(200),
    region VARCHAR(50)
);

-- 数据清洗
CACHE TABLE cleaned_events AS
SELECT 
    event_id,
    user_id,
    event_type,
    event_time,
    JSON_EXTRACT(payload, '$.value') as value
FROM raw_events
WHERE event_id IS NOT NULL AND user_id IS NOT NULL;

-- 数据关联
CACHE TABLE enriched_events AS
SELECT 
    e.*,
    p.user_name,
    p.region
FROM cleaned_events e
LEFT JOIN user_profiles p ON e.user_id = p.user_id;

-- 数据聚合
CACHE TABLE daily_summary AS
SELECT 
    region,
    event_type,
    DATE(event_time) as event_date,
    COUNT(*) as event_count,
    AVG(value) as avg_value
FROM enriched_events
GROUP BY region, event_type, DATE(event_time);

-- 返回结果
RETURN daily_summary;
```

### 调用示例

```sql
-- 准备输入数据
CACHE TABLE events AS
SELECT * FROM hive_db.raw_events WHERE dt = '2024-01-01';

CACHE TABLE profiles AS
SELECT * FROM hive_db.user_profiles;

-- 调用函数
CACHE TABLE summary AS
CALL etl_pipeline(events, profiles);

-- 使用结果
SELECT * FROM summary WHERE event_count > 100;
```

### 异步处理

```sql
-- 异步调用多个处理任务
CALL process_region('north_data') ASYNC;
CALL process_region('south_data') ASYNC;
CALL process_region('east_data') ASYNC;

-- 主流程继续执行
CACHE TABLE other_result AS
SELECT * FROM other_source;
```


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

SQLRec 将 SQL 从声明式查询语言扩展为具备完整编程能力的语言，同时保持了 SQL 的简洁性和声明式特性。
