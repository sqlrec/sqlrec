# SQLRec 编程模型

SQLRec 是一种基于 SQL 的数据处理和机器学习编程框架。它扩展了标准 SQL，引入了变量、函数、缓存表等编程概念，使得 SQL 具备了类似编程语言的能力。

本文同时面向 SQL 编写者和 SQLRec 开发者：SQL 语法、约束和示例描述可观察的使用契约；Java 类名和实现片段用于解释原理，不应被当作额外的 SQL 能力。SQLRec 中的“SQL 函数”指由多条 SQL 定义的函数，“Java UDF”则指通过 Java `evaluate` 方法注册的表函数或标量函数。

本文描述语言和运行模型；一段业务 SQL 是否可执行，还取决于部署中真实存在的表 schema、主键及连接器能力，以及已注册函数的准确签名和返回 schema。语法示例使用单引号表示字符串、反引号引用需要转义的标识符，多语句函数中的每条顶层语句以分号分隔。

## 表类型系统

SQLRec 定义了一套表类型层次结构，不同类型的表具有不同的访问特性。

### 类型层次

```
SqlRecTable (抽象基类)
├── CacheTable          -- 内存缓存表
├── SqlRecKvTable       -- KV 表（支持主键查询）
└── 其他连接器表         -- 例如 KafkaCalciteTable

VectorSearchable (接口)
└── 当前由 MilvusCalciteTable 实现；该类同时继承 SqlRecKvTable
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
- 通过 `scan()` 支持重复扫描，不提供按键随机访问能力
- 生命周期与当前 `SqlExecutor`/请求执行上下文一致；函数内部创建的缓存表局限于本次函数调用
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
- 可按连接器配置启用 Caffeine 查询缓存；未调用 `initCache` 时不缓存
- 支持批量主键查询

**核心方法：**

| 方法 | 说明 |
|------|------|
| `getPrimaryKeyIndex()` | 获取主键列索引（抽象方法，由子类实现） |
| `getByPrimaryKeyImpl(Set<Object> keySet)` | 按主键批量查询（抽象方法，由子类实现具体数据源访问逻辑） |
| `getByPrimaryKey(Set<Object> keySet)` | 按主键批量查询；若已初始化 Caffeine 缓存则优先返回命中项，未命中时调用 `getByPrimaryKeyImpl` |
| `initCache(int maxSize, long expireAfterWrite)` | 初始化查询缓存 |
| `onlyFilterByPrimaryKey()` | 传给连接器的候选过滤条件是否只保留安全的主键过滤（默认返回 `true`） |
| `invalidateCache(Object[] row)` | 根据行数据失效对应主键的缓存 |

**缓存配置：**

```java
// 初始化缓存：最大 10000 条，写入后 60 秒过期
kvTable.initCache(10000, 60);
```

### VectorSearchable 接口

`VectorSearchable` 是独立的向量检索接口。当前 Milvus 表同时继承 `SqlRecKvTable` 并实现该接口，因此兼具 KV 查询和向量搜索能力；接口本身并不继承 KV 表。

**特性：**
- 实现类可以同时具备 KV 表能力；这不是接口本身的继承保证
- 支持向量相似度搜索
- 支持 ANN（近似最近邻）查询

**核心方法：**

```java
public interface VectorSearchable {
    List<VectorSearchResult> searchByEmbeddingImpl(VectorSearchRequest request);

    // searchByEmbedding wraps the implementation with type conversion and metrics.
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

### SQLRec 直接处理的主要 SQL 类型

以下列表用于帮助开发者理解执行分发，不是面向用户的完整语法清单。具体语法应以本文后续章节和 [SQL 语法参考](sql_reference.md) 为准。

| SQL 类型 | 说明 |
|----------|------|
| `SqlCreateModel` | 创建模型 |
| `SqlDropModel` | 删除模型 |
| `SqlTrainModel` | 训练模型 |
| `SqlExportModel` | 导出模型 |
| `SqlAlterModelDropCheckpoint` | 删除模型检查点 |
| `SqlCreateService` | 创建服务 |
| `SqlDropService` | 删除服务 |
| `SqlCreateApi` | 创建 API |
| `SqlDropApi` | 删除 API |
| `SqlCreateSqlFunction` | 创建 SQL 函数 |
| `SqlDropSqlFunction` | 删除 SQL 函数 |
| `SqlCache` | 缓存表 |
| `SqlCallSqlFunction` | 调用函数 |
| `SqlAssert` | 断言 |
| `SqlIfCache` | 条件执行语句 |
| `SqlReturn` | SQL 函数返回 |
| `SqlDefineInputTable` | SQL 函数输入表定义 |
| `SqlSet` | 设置变量 |
| `SqlFlush` | 失效系统缓存 |
| `SqlShowTables` | 显示表列表 |
| `SqlShowSqlFunction` | 显示函数列表 |
| `SqlShowApi` | 显示 API 列表 |
| `SqlShowModel` | 显示模型列表 |
| `SqlShowService` | 显示服务列表 |
| `SqlShowCheckpoint` | 显示检查点列表 |
| `SqlRichDescribeTable` | 描述表结构 |
| `SqlShowCreateTable` | 显示建表语句 |

`USE`、`SHOW DATABASES` 等复用的 Flink AST 也可能由 SQLRec 直接处理，因此不要依据 Java 类列表推断一条 SQL 是否可用。

### CRUD SQL 的路由判断

对于 SELECT、INSERT、UPDATE、DELETE 等 CRUD 语句，系统会检查所有涉及的表：

```java
public static boolean isSqlTableRunnable(SqlNode sqlNode, CalciteSchema schema, String defaultSchema) {
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

不要在同一条 CRUD SQL 中混用本次执行创建的 `CacheTable` 与只能由 Flink 访问的外部表：整条语句会被转发到 Flink，而 Flink 无法看到进程内缓存表。应先把所需数据转换为 SQLRec 可访问的表，或把处理拆到明确的执行边界两侧。

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

| 表类型/能力 | SQL 过滤 | 过滤下推 | 可作 KV Join 右表 | 可作向量 Join 右表 |
|--------|----------|----------|---------|----------|
| CacheTable | ✅，扫描后过滤 | ❌ | ❌ | ❌ |
| SqlRecKvTable | ✅ | ✅，具体能力取决于连接器 | ✅ | ❌ |
| 同时实现 VectorSearchable 的表 | ✅ | ✅，具体能力取决于连接器 | ✅（若同时是 KV 表） | ✅ |

这里的“右表”是优化算子的角色，而不是普通 SQL Join 的通用能力。`CacheTable` 经常作为 KV Join 或向量 Join 的左侧输入。表是否支持写入（INSERT/UPDATE/DELETE）取决于具体实现能否完成 `ModifiableTable` 操作，不能只根据抽象表类型推断。

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

- **作用域**：顶层创建的缓存表在当前 `SqlExecutor` 中可见；函数内部创建的缓存表仅在本次函数调用的临时 schema 中可见，若要传给另一个函数必须显式作为参数传递
- **生命周期**：随当前执行器/请求结束而销毁，不写入持久化元数据
- **类型**：表类型，包含列定义和数据行
- **同名覆盖**：再次执行同名 `CACHE TABLE` 会替换当前 schema 中的表；函数会按读写依赖保证引用旧结果的节点先完成

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

`CACHE TABLE ... AS CALL ... ASYNC` 虽然能够通过语法解析，但运行时不支持，因为异步调用不能同步提供要缓存的结果。异步函数应使用独立的 `CALL ... ASYNC`。

### IF 与 ASSERT

`IF` 用一条查询或表达式决定执行哪个分支，每个分支只能包含一条语句：

```sql
IF (SELECT COUNT(*) > 0 FROM source_table) THEN (
    CACHE TABLE result AS SELECT * FROM source_table
) ELSE (
    CACHE TABLE result AS SELECT * FROM fallback_table
);
```

需要遵守以下约束：

- 条件必须恰好返回一行一列。普通模式要求 BOOLEAN，NULL 按 false 处理。
- 当前不支持在 THEN 或 ELSE 中直接嵌套另一个 IF，也不能在一个分支中放多条语句。
- 两个分支若使用 `CACHE TABLE`，必须写入同一个表名，且列数、列名和类型一致。
- 无 ELSE 且条件为 false 时，如果 THEN 是 CACHE 语句，系统会注册一个具有相同 schema 的空缓存表。
- 分支可以使用 CRUD、`CACHE TABLE`、`CALL`、`SET`、`ASSERT` 或 `RETURN`；`RETURN` 只能在 SQL 函数中使用，详细规则见“函数返回结果”。

`IF TIMEIN` 的条件返回毫秒数。超时值大于 0 时，THEN 超时或抛出异常会执行 ELSE；小于等于 0 时直接执行 THEN，此时 THEN 的异常不会回退。TIMEIN 必须提供 ELSE，且两个分支必须同为 CACHE 或同为 RETURN：

```sql
IF TIMEIN (SELECT timeout_ms FROM config_table) THEN (
    CACHE TABLE result AS CALL slow_function(input_table)
) ELSE (
    CACHE TABLE result AS SELECT * FROM fallback_table
);
```

`ASSERT` 执行查询并校验所有返回值：查询必须至少返回一行，所有列必须是 BOOLEAN，且每一行每一列都必须为 true；false 或 NULL 都会终止执行。

```sql
ASSERT SELECT COUNT(*) > 0 FROM source_table;
```

### 过滤查询

所有可扫描的 `SqlRecTable` 都可以表达 SQL `WHERE` 过滤，但不代表所有表都支持把谓词下推到数据源。`CacheTable` 会在内存扫描后过滤；实现了 `FilterableTable` 或 `ProjectableFilterableTable` 的连接器表可以使用 `FilterableTableScan` 尝试下推。

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

对于 `SqlRecKvTable`，`onlyFilterByPrimaryKey()` 控制的是传给连接器的候选过滤条件，而不是 SQL 的合法性。值为 true 时只把安全的主键等值条件用于候选行查询，完整的原始谓词仍由外层 `LogicalCalc` 再次计算，以保证最终过滤语义正确：

```java
// SqlRecKvTable.scan
List<RexNode> candidateFilters = filters;
if (onlyFilterByPrimaryKey() && filters != null) {
    candidateFilters = FilterUtils.getPrimaryKeyFilters(
        filters, getPrimaryKeyIndex()
    );
}

// SqlRecFilterTableScanRule 仍保留完整原始谓词作为外层 LogicalCalc 条件
```

**示例：**

```sql
-- 主键条件可用于高效候选行查询
SELECT * FROM kv_table WHERE primary_key = 'key123';

-- 非主键条件在语义上仍然有效，但连接器可能需要扫描更多候选行；
-- 如果具体连接器不支持无主键扫描，则会在运行时报错
SELECT * FROM kv_table WHERE other_column = 'value';
```

### KV Join

KV Join 是 SqlRecKvTable 特有的连接方式，通过主键批量查询实现高效关联。

#### 触发条件

1. 左侧必须是本地可枚举关系；通常先通过 `CACHE TABLE` 物化，避免重复访问外部存储
2. Join 条件必须是两个列引用之间的单个**等值条件**（`=`），不能是复合条件
3. 右表必须是 `SqlRecKvTable`，并应使用其主键列参与等值连接
4. 仅使用 INNER JOIN 或 LEFT JOIN；RIGHT/FULL JOIN 不受支持

```java
// SqlRecKvJoinRule 检查条件
RexNode condition = join.getCondition();
try {
    NodeUtils.getJoinKeyColIndex(condition);
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
        rightTable.getByPrimaryKey(joinKeys);
    
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

向量搜索 Join 是实现了 `VectorSearchable` 接口的表特有的连接方式，通过向量相似度进行关联。

#### 触发条件

1. 左侧必须是本地可枚举关系，推荐先物化为 `CacheTable`
2. SELECT 投影中必须直接包含 **`ip(left_embedding, right_embedding)`**，两个参数必须是左右两侧的列引用
3. Join 条件必须为恒真条件，例如 `ON TRUE` 或 `ON 1 = 1`
4. 右表必须实现 `VectorSearchable` 接口
5. 必须有 **ORDER BY ... LIMIT** 子句；对于内积相似度通常使用 DESC
6. 应使用 INNER JOIN；当前向量执行器不会为 LEFT/RIGHT/FULL JOIN 补齐未命中行
7. WHERE 中只应放可由向量右表处理的过滤条件；左表预过滤应在 Join 之前完成

```java
// SqlRecVectorJoinRule 检查条件
if (!NodeUtils.hasIpFunction(project)) {
    return; // 必须有 ip 函数
}
if (!NodeUtils.isTrueCondition(join)) {
    return; // Join 条件必须为 true
}
if (rightTable.unwrap(VectorSearchable.class) == null) {
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
INNER JOIN vector_table right ON true
WHERE right.category = 'electronics'  -- 可选的过滤条件
ORDER BY score DESC
LIMIT 10;
```

这里的 LIMIT 是**每个左侧输入行**传给 ANN 检索的上限；左表有多行时，总结果数可能大于 LIMIT。不要把它理解为普通 SQL 的全局截断。

#### 实现原理

```java
// VectorJoinExecutor.execute
public static Enumerable<Object[]> execute(
        Enumerable left,
        VectorSearchable rightTable,
        Object pushedFilter,
        int leftEmbeddingIndex,
        String rightEmbeddingField,
        int topKPerLeftRow
) {
    for (Object[] leftRow : left) {
        // 1. Extract the query vector from the left row.
        List<Float> embedding = DataTransformUtils.convertToFloatVec(leftRow[leftEmbeddingIndex]);
        
        // 2. Milvus applies the pushed filter before ANN top-K.
        VectorSearchRequest request = new VectorSearchRequest(
            leftRow, embedding, rightEmbeddingField, (RexNode) pushedFilter, topKPerLeftRow);
        List<VectorSearchResult> rightValues = rightTable.searchByEmbedding(request);
        
        // 3. Join the full right row and score with the left row.
        // ...
    }
}
```

#### 配置参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `DEFAULT_VECTOR_SEARCH_LIMIT` | 默认返回数量 | 100 |

### UNION 操作

本地 UNION 操作通过 `SqlrecEnumerableUnion` 实现，使用蛇形合并算法。

#### 实现方式

```java
// SqlrecEnumerableUnion.implement
Expression unionExp = Expressions.call(
    MergeUtils.class.getMethod("snakeMergeEnumerable", Iterable[].class), 
    inputExps
);
```

#### 蛇形合并算法

蛇形合并会轮询各输入源、交替取出一行，最后物化为一个 `List`。它不是惰性的流式输出：

```java
// MergeUtils.snakeMergeEnumerable
public static <T> Enumerable<T> snakeMergeEnumerable(Iterable<T>... sources) {
    List<T> merged = snakeMerge(sources); // snakeMerge 内部物化 ArrayList
    return Linq4j.asEnumerable(merged);
}
```

本地集合运算目前只对 UNION 做了专门识别；不要假定 INTERSECT、EXCEPT 等操作可以和进程内 `CacheTable` 一起本地执行。

当前实现没有根据 `UNION` 的 `ALL` 标志执行去重，因此编写 SQLRec 本地查询时应明确使用 `UNION ALL`。如果业务需要去重，应在合并后显式使用 `SELECT DISTINCT` 或 `GROUP BY`。无论使用哪种形式，都不应依赖合并产生的行顺序；需要稳定顺序时必须在最外层写 `ORDER BY`。

## 变量系统

SQLRec 通过 `ExecuteContext` 管理运行时变量，提供类似编程语言中变量的能力。

### 变量设置

使用 `SET` 语句或 API 设置变量。SQL 语法中的键和值都必须是字符串字面量，不能直接使用查询或任意表达式：

```sql
SET 'my_var' = 'my_value';
```

```java
context.setVariable("my_var", "my_value");
```

### 变量获取

在 `CALL` 的函数名或字符串参数位置，可以使用 `GET()` 获取变量，或使用 `GET_OR_DEFAULT()` 在变量不存在时提供默认值：

```sql
-- Java 表函数的字符串参数
CALL my_java_function(GET('config_value'));

-- 动态函数名；必须用 LIKE 声明返回 schema
CALL GET_OR_DEFAULT('function_name', 'default_function')(input_table)
LIKE FUNCTION 'default_function';
```

`GET()`/`GET_OR_DEFAULT()` 不能把字符串变量变成 SQL 函数的表参数；SQL 函数的实参仍必须是缓存表标识符。在普通 SELECT 表达式中调用同名标量 UDF 时，由于名称也是扩展关键字，推荐使用反引号：

```sql
SELECT `get_or_default`('experiment_group', 'control') AS experiment_group;
```

### 变量作用域

| 特性 | 说明 |
|------|------|
| **存储** | `ConcurrentHashMap`（线程安全） |
| **可见性** | 当前 `SqlExecutor`/请求及其嵌套函数调用可见 |
| **隔离性** | 不同执行器或请求之间变量隔离 |
| **值类型** | 字符串；设置 null 会删除变量 |

### 函数调用时的变量

函数调用时会创建新的执行上下文：

```java
ExecuteContext finalContext = context.clone();
finalContext.addFunNameToStack(funName);
```

- **变量共享**：克隆的上下文共享变量映射
- **调用栈隔离**：每个函数调用有独立的调用栈
- **执行顺序**：`SET` 和接收 `ExecuteContext` 的 Java UDF 不会并行执行，以避免变量副作用与其他节点乱序


## 函数系统

SQLRec 支持自定义 SQL 函数，函数是一组 SQL 语句的封装，类似于编程语言中的函数定义。这里的 SQL 函数不同于后文的 Java UDF：SQL 函数的显式参数只能是表，Java 表 UDF 才能接收缓存表、字符串以及自动注入的上下文。

### 函数定义

一个完整的函数定义包含以下部分：

```sql
-- 1. 函数声明；需要覆盖已有定义时使用 OR REPLACE
CREATE OR REPLACE SQL FUNCTION my_function;

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

函数定义由多条独立 SQL 依次提交，必须遵守以下阶段顺序：

1. 一条 `CREATE [OR REPLACE] SQL FUNCTION`。
2. 零到多条连续的 `DEFINE INPUT TABLE`；也可以写 `DEFINE INPUT TABLE input_data LIKE existing_table`。
3. 函数体语句。一旦开始函数体，就不能再追加 DEFINE。
4. 一条顶层 `RETURN` 结束定义；即使 IF 的所有分支都会返回，也仍需要顶层 RETURN 作为编译结束标志。

### 函数传参

SQLRec SQL 函数的显式参数是表。调用方传入的每个实参必须是 `CacheTable` 标识符；系统会在函数调用的临时 schema 中把它绑定到对应的 `DEFINE INPUT TABLE` 名称，而不会复制整张表。函数将输入表作为只读数据源使用。

#### 基本调用

```sql
CALL my_function(table1, table2);
```

#### 参数匹配

调用时传入的表必须与函数定义的输入表结构兼容。兼容规则是：

- 实参列数可以多于形参，但不能少于形参。
- 按位置比较形参要求的前 N 列，列名忽略大小写但必须一致。
- SQL 类型必须一致；CHAR/VARCHAR 等字符串类型彼此兼容。
- 不按列名重排，因此列顺序同样重要。

```sql
-- 函数定义
CREATE SQL FUNCTION process_data;
DEFINE INPUT TABLE input_data (id INT, value DOUBLE);
...
RETURN result;

-- my_table 的前两列必须依次为 id 和 value，类型分别兼容 INT 和 DOUBLE
CALL process_data(my_table);
```

#### 动态函数名

使用 `GET()` 或 `GET_OR_DEFAULT()` 动态获取函数名：

```sql
-- 从变量获取函数名
CALL GET('function_name')(table1, table2) LIKE template_table;

-- 变量不存在时调用 default_function
CALL GET_OR_DEFAULT('function_name', 'default_function')(table1, table2)
LIKE FUNCTION 'default_function';
```

动态调用函数时，需要使用 `LIKE` 子句指定结果表的模式，因为编译期无法知道调用的哪个函数，无法推断类型。

#### 异步调用

使用 `ASYNC` 关键字异步执行函数：

```sql
CALL my_function(input_table) ASYNC;
```

异步调用只负责把任务提交到后台线程并立即返回，不提供可同步消费的结果。它适用于旁路写入、日志等场景；不能放在 `CACHE TABLE ... AS CALL` 或 `RETURN CALL` 中。后续 SQL 也不应依赖异步调用产生的数据或副作用已经完成。

#### 分区调用

`PARTITION BY` 可以把某个缓存表按固定行数拆分，并发调用函数后合并各分区结果：

```sql
CALL my_function(input_table)
LIKE FUNCTION 'my_function'
PARTITION BY input_table SIZE 100;
```

分区表必须同时出现在 CALL 的实参列表中，SIZE 必须是整数字面量，建议使用正数。`LIKE` 必须写在 `PARTITION BY` 之前，`ASYNC` 必须写在最后。分区合并不保证业务排序，如需稳定顺序应在缓存结果后另行 `ORDER BY`。

### 函数返回结果

函数通过 `RETURN` 语句返回结果。RETURN 一旦在运行时执行，当前函数会立即结束，尚未执行的后续节点不会再执行。

#### 支持的返回形式

```sql
-- 返回已有缓存表
RETURN result_table;

-- 直接返回查询结果
RETURN SELECT id, score FROM result_table;

-- 返回同步函数调用结果
RETURN CALL post_process(result_table);

-- 不返回数据
RETURN;
```

表名形式要求目标是 `CacheTable`，通常是函数体中通过 `CACHE TABLE` 创建的缓存表。`RETURN SELECT` 和 `RETURN CALL` 会直接把对应语句的结果作为函数结果，不需要创建内部匿名缓存表。`RETURN CALL ... ASYNC` 不受支持。

#### 顶层结束标志与提前返回

```sql
CREATE SQL FUNCTION find_candidates;
DEFINE INPUT TABLE candidates (id BIGINT, score DOUBLE);

-- IF 内的 RETURN 是运行时提前返回，不结束函数定义
IF (SELECT COUNT(*) = 0 FROM candidates) THEN (
    RETURN SELECT id, score FROM candidates
);

-- 顶层 RETURN 仍是函数定义的结束标志，也处理 IF 条件为 false 的路径
RETURN SELECT id, score FROM candidates ORDER BY score DESC;
```

函数定义必须以顶层 `RETURN` 结束，之后不能再出现函数体语句。IF 中的 RETURN 不作为定义结束标志；它只在该分支实际执行时提前结束函数。

IF 使用 RETURN 时，必须满足以下分支结构之一：

- THEN 返回且没有 ELSE；条件为 false 时继续执行 IF 后面的语句。
- THEN 和 ELSE 都返回；两个分支的返回模式必须兼容。

不能只在 ELSE 返回，也不能让 THEN 返回而 ELSE 执行普通语句。函数内所有可能的返回点必须全部为空返回，或者具有相同的列数、列名和列类型；CHAR/VARCHAR 等字符串类型彼此兼容。

`IF TIMEIN` 也可以在两个分支中使用 RETURN。当超时时间大于 0 时，THEN 超时或抛出异常会丢弃其临时返回状态，然后由 ELSE 设置最终返回值；超时时间小于等于 0 时直接执行 THEN。

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

该能力由 `PARALLELISM_EXEC` 控制，默认开启；关闭时函数体按定义顺序串行执行。开启时，文本顺序本身不构成依赖，系统主要根据读写表集合和不可并行节点构建执行图：

- **读依赖**：语句读取某个表
- **写依赖**：语句写入某个表（如 CACHE TABLE）
- **执行屏障**：`SET`、接收 `ExecuteContext` 的 UDF、ASSERT、包含 RETURN 的 IF 等不可并行节点会约束前后顺序

```sql
-- 这两个语句可以并行执行（无依赖）
CACHE TABLE a AS SELECT * FROM source1;
CACHE TABLE b AS SELECT * FROM source2;

-- 这个语句必须等待前两个完成
CACHE TABLE c AS SELECT * FROM a UNION ALL SELECT * FROM b;
```

因此，需要先后执行的普通语句应通过明确的表读写关系表达依赖，不能只依赖书写顺序。`CALL ... ASYNC` 的完成时间不进入后续同步依赖图。

## 循环依赖检测

静态 SQL 函数调用会在编译依赖图中检测循环；动态函数名无法在编译期确定，因此还会通过运行时调用栈防止无限递归。

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

本节解释 UDF 的调用和开发模型。编写使用内置 UDF 的业务 SQL 时，还应查阅 [内置 UDF](udf.md) 中的函数签名、参数含义和输出 schema；不能仅凭函数名推断调用方式。

### UDF 定义

UDF 是一个普通的 Java 类，需要满足以下条件：

1. **必须有一个或多个 `evaluate` 方法**：这是 UDF 的入口点
2. **表 UDF 参数类型限制**：SQL 显式传入的参数只支持 `CacheTable` 和 `String`，另可自动注入 `ExecuteContext` 或 `ReadonlyContext`
3. **返回类型**：产生表结果的 UDF 返回 `CacheTable`；仅执行副作用的 UDF 可以返回 `Void`，但其结果不能用于 CACHE 或 RETURN

```java
public class MyTableFunction {
    
    public CacheTable evaluate(CacheTable inputTable, String config) {
        // 处理逻辑
        List<Object[]> results = processTable(inputTable, config);
        return new CacheTable("output", results, inputTable.getDataFields());
    }
}
```

### 方法重载

表函数支持方法重载，可以定义多个同名但参数不同的 `evaluate` 方法。系统会根据调用时传入的参数类型和数量自动选择匹配的方法。

**示例**：

```java
public class MyFunction {
    // 无参数版本
    public CacheTable evaluate() {
        // ...
    }
    
    // 单个字符串参数版本
    public CacheTable evaluate(String arg) {
        // ...
    }
    
    // 两个字符串参数版本
    public CacheTable evaluate(String arg1, String arg2) {
        // ...
    }
    
    // 接收表参数版本
    public CacheTable evaluate(CacheTable input) {
        // ...
    }
    
    // 混合参数版本
    public CacheTable evaluate(CacheTable input, String arg) {
        // ...
    }
}
```

**使用示例**：

```sql
-- 调用无参数版本
CALL my_function();

-- 调用单参数版本
CALL my_function('arg1');

-- 调用双参数版本
CALL my_function('arg1', 'arg2');

-- 调用表参数版本
CALL my_function(input_table);

-- 调用混合参数版本
CALL my_function(input_table, 'arg1');
```

::: warning 注意
如果多个方法都能匹配调用参数，系统会抛出异常。例如，同时定义了 `evaluate(String arg)` 和 `evaluate(String... args)`，调用 `my_function('arg1')` 时会报错，因为两个方法都匹配。
:::

### 可变参数

表函数支持 Java 可变参数（varargs），使用 `...` 语法定义。

**示例**：

```java
public class MyFunction {
    // 字符串可变参数
    public CacheTable evaluate(String... args) {
        for (String arg : args) {
            System.out.println(arg);
        }
        // ...
    }
    
    // 表可变参数
    public CacheTable evaluate(CacheTable... tables) {
        for (CacheTable table : tables) {
            // 处理每个表
        }
        // ...
    }
    
    // 混合参数：固定参数 + 可变参数
    public CacheTable evaluate(String prefix, CacheTable... tables) {
        // prefix 是必需参数
        // tables 是可变参数
        // ...
    }
}
```

**使用示例**：

```sql
-- 字符串可变参数
CALL my_function('arg1');
CALL my_function('arg1', 'arg2');
CALL my_function('arg1', 'arg2', 'arg3');

-- 表可变参数
CALL my_function(table1);
CALL my_function(table1, table2);
CALL my_function(table1, table2, table3);

-- 混合参数
CALL my_function('prefix', table1);
CALL my_function('prefix', table1, table2);
```

**注意事项**：
- 可变参数可以接受 0 到多个参数
- 可变参数必须是方法的最后一个参数
- 当前 SQL 调用层只支持 `String...` 或 `CacheTable...`，其他可变参数类型会被拒绝

### 参数注入

SQLRec 会根据 `evaluate` 方法的参数类型自动注入相应的值：

| 参数类型 | 注入来源 | SQL 语法 | 适用场景 |
|----------|----------|----------|----------|
| `CacheTable` | 传入的缓存表 | 标识符（如 `table_name`） | 表函数 |
| `String` | 字符串字面量或变量 | `'value'` 或 `GET('var')` | 表函数、标量函数 |
| `ExecuteContext` | 执行上下文 | 自动注入，无需在 SQL 中指定 | 表函数 |
| `ReadonlyContext` | 只读上下文 | 自动注入，无需在 SQL 中指定 | 表函数 |
| `DataContext`（运行时为 `SqlRecDataContext`） | SQLRec 数据上下文 | 由 Calcite 自动注入，无需在 SQL 中指定 | 标量函数 |

`SqlRecDataContext` 是专门为标量 UDF 设计的接口，继承自 Calcite 的 `DataContext`。它提供了访问执行上下文变量的能力：

```java
public interface SqlRecDataContext extends DataContext {
    String getVariable(String key);
}
```

在标量 UDF 中，可以通过 `SqlRecDataContext` 获取变量值：

```java
public class GetFunction {
    public static String evaluate(DataContext context, String key) {
        if (!(context instanceof SqlRecDataContext)) {
            throw new IllegalArgumentException("context must be SqlRecDataContext");
        }
        SqlRecDataContext sqlRecDataContext = (SqlRecDataContext) context;
        return sqlRecDataContext.getVariable(key);
    }
}
```

参数注入示例

```java
public class MyFunction {
    // 方法签名
    public CacheTable evaluate(
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

UDF 的返回数据模式（Schema）可以在编译期确定，有以下三种方式：

#### 1. 通过 LIKE 子句指定表模式

```sql
CALL my_function(input_table) LIKE template_table;
```

编译时，系统会从 `template_table` 获取返回数据模式：

```java
if (!StringUtils.isEmpty(likeTableName)) {
    returnDataFields = getDataTypeByLikeTableName(likeTableName, schema);
}
```

#### 2. 通过 LIKE FUNCTION 子句指定函数模式

```sql
CALL my_function(input_table) LIKE FUNCTION 'template_function';
```

编译时，系统会从指定的函数获取返回数据模式：

```java
if (likeFunctionName != null) {
    SqlFunctionBindable likeFunctionBindable = compileManager.getSqlFunction(likeFunctionName);
    returnDataFields = likeFunctionBindable.getReturnDataFields();
}
```

#### 3. 通过执行 evaluate 方法推断

如果没有 LIKE 子句，且 `evaluate` 返回 `CacheTable`，系统会在编译期执行一次该方法来推断返回模式：

```java
if (CacheTable.class.isAssignableFrom(evalMethod.getReturnType())) {
    Object outputTable = callEvalMethod(schema, new ExecuteContextImpl());
    returnDataFields = ((CacheTable) outputTable).getDataFields();
}
```

**注意**：这种方式要求 UDF 在空执行上下文和当前占位输入下能够正常执行，而且编译阶段的调用也可能触发网络请求或其他副作用。对动态函数、异步调用、依赖真实数据或具有副作用的 UDF，应显式使用 `LIKE table` 或 `LIKE FUNCTION 'function_name'`，避免为了推断 schema 而执行一次真实逻辑。

### UDF 注册

UDF 必须在 SQLRec 当前使用的元数据模式中注册才能被调用。使用 Hive Metastore（HMS）时，需要指定函数名和对应的 Java 类全限定名：

```sql
-- 在 HMS 中注册 UDF
CREATE FUNCTION my_function AS 'com.example.MyFunction';
```

使用 `SQL_SCHEMA_DIR` 本地元数据模式时，同样的 `CREATE FUNCTION` 应写入该目录下的 SQL 文件，由 SQLRec 启动时加载；该模式不要求连接 HMS，也不允许在运行中的 CLI/API 会话里直接持久化这类 DDL。

在 HMS 模式下，系统在调用函数时会通过 HMS 获取函数的类名，然后动态加载：

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
| 表变量 | 中间结果赋值 | `CACHE TABLE` |
| 字符串变量 | 请求级配置 | `SET`、`GET()`、`GET_OR_DEFAULT()` |
| 函数 | 函数定义 | `CREATE SQL FUNCTION` |
| 参数 | 函数参数 | `DEFINE INPUT TABLE` |
| 返回值 | return 语句 | `RETURN` |
| 函数调用 | 函数调用 | `CALL` |
| 动态分发 | 反射/动态加载 | `GET()` / `GET_OR_DEFAULT()` + `LIKE` |
| 并发 | 多线程 | 可配置的自动并行 + 虚拟线程 |
| 作用域 | 变量作用域 | 当前执行器/请求；函数缓存表为调用级 |
| 类型系统 | 静态类型 | 表结构检查 |
| UDF | 外部库/插件 | Java 类 + `evaluate` 方法 |
| 表类型 | 数据结构 | `SqlRecTable` 层次结构 |
| 执行路由 | 编译目标选择 | 本地执行 / 转发 Flink |
| 过滤查询 | 条件筛选 | `FilterableTableScan` + 规则优化 |
| KV Join | 主键关联查询 | `SqlRecKvJoinRule` + 主键批量查询 |
| 向量搜索 | 相似度匹配 | `SqlRecVectorJoinRule` + `ip()` 函数 |
| UNION ALL | 数据合并 | `SqlrecEnumerableUnion` + 蛇形合并算法 |

SQLRec 在声明式 SQL 之上提供了表变量、多语句函数、控制流、运行时变量和并行执行等编程能力。编写 SQL 时仍需遵守本文列出的表类型、函数参数、控制流和执行路由限制。
