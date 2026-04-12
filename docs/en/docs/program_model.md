# SQLRec Programming Model

SQLRec is a SQL-based data processing and machine learning programming framework. It extends standard SQL by introducing programming concepts such as variables, functions, and cached tables, enabling SQL to have programming language-like capabilities.

## Table Type System

SQLRec defines a table type hierarchy, where different types of tables have different access characteristics.

### Type Hierarchy

```
SqlRecTable (abstract base class)
├── CacheTable          -- Memory cache table
└── SqlRecKvTable       -- KV table (supports primary key queries)
    └── SqlRecVectorTable -- Vector table (supports vector search)
```

### SqlRecTable

`SqlRecTable` is the abstract base class for all SQLRec tables, inheriting from Calcite's `AbstractTable`. All SQLRec custom table types must inherit from this class.

```java
public abstract class SqlRecTable extends AbstractTable {
}
```

### CacheTable

`CacheTable` is an in-memory data table and the most commonly used table type in SQLRec.

**Features:**
- Data stored in memory
- Supports fast random access
- Session-level lifecycle
- Created via `CACHE TABLE` statement

**Use Cases:**
- Function parameter passing
- Intermediate calculation result storage
- Function return values

```sql
-- Create CacheTable
CACHE TABLE my_cache AS
SELECT * FROM source_table WHERE condition;
```

### SqlRecKvTable

`SqlRecKvTable` is a KV table that supports primary key queries, implementing `ModifiableTable` and `FilterableTable` interfaces.

**Features:**
- Supports efficient queries by primary key
- Built-in Caffeine cache
- Supports batch primary key queries

**Core Methods:**

| Method | Description |
|--------|-------------|
| `getPrimaryKeyIndex()` | Get primary key column index |
| `getByPrimaryKey(Set<Object> keySet)` | Batch query by primary key |
| `getByPrimaryKeyWithCache(Set<Object> keySet)` | Batch query with cache |
| `initCache(int maxSize, long expireAfterWrite)` | Initialize query cache |

**Cache Configuration:**

```java
// Initialize cache: max 10000 entries, expire 60 seconds after write
kvTable.initCache(10000, 60);
```

### SqlRecVectorTable

`SqlRecVectorTable` inherits from `SqlRecKvTable` and adds vector search capabilities.

**Features:**
- Inherits all capabilities of KV table
- Supports vector similarity search
- Supports ANN (Approximate Nearest Neighbor) queries

**Core Methods:**

```java
public abstract class SqlRecVectorTable extends SqlRecKvTable {
    public abstract List<String> getFieldNames();
    
    public List<Object[]> searchByEmbeddingWithScore(
            Object[] leftValue,      // Left table join value
            List<Float> embedding,   // Query vector
            String annFieldName,     // Vector field name
            RexNode filterCondition, // Filter condition
            int limit,               // Return count limit
            List<Integer> projectColumns  // Projection columns
    );
}
```

**Use Cases:**
- Vector similarity search
- Semantic retrieval
- Recommendation systems


## SQL Execution Routing

As a SQL gateway, SQLRec needs to decide which SQL executes locally and which forwards to backend engines (like Flink SQL Gateway).

### Routing Decision Flow

```
SQL Request
    │
    ▼
Parse SQL → Determine SQL type
    │
    ├─── SQLRec Extended Syntax ──→ Local Execution
    │    ├── CREATE MODEL / DROP MODEL / TRAIN MODEL / EXPORT MODEL
    │    ├── CREATE SERVICE / DROP SERVICE
    │    ├── CREATE API / DROP API
    │    ├── CREATE SQL FUNCTION / DROP SQL FUNCTION
    │    ├── CACHE TABLE
    │    ├── CALL
    │    └── SHOW / DESCRIBE statements
    │
    ├─── CRUD SQL ──→ Check table types
    │    │
    │    ├── All tables are SqlRecTable ──→ Local Execution
    │    │
    │    └── Contains non-SqlRecTable ──→ Forward to Flink
    │
    └─── Other SQL ──→ Forward to Flink
```

### Locally Executed SQL Types

| SQL Type | Description |
|----------|-------------|
| `SqlCreateModel` | Create model |
| `SqlDropModel` | Drop model |
| `SqlTrainModel` | Train model |
| `SqlExportModel` | Export model |
| `SqlCreateService` | Create service |
| `SqlDropService` | Drop service |
| `SqlCreateApi` | Create API |
| `SqlDropApi` | Drop API |
| `SqlCreateSqlFunction` | Create SQL function |
| `SqlDropSqlFunction` | Drop SQL function |
| `SqlCache` | Cache table |
| `SqlCallSqlFunction` | Call function |
| `SqlSet` | Set variable |
| `SqlShowTables` | Show table list |
| `SqlShowSqlFunction` | Show function list |
| `SqlShowApi` | Show API list |
| `SqlShowModel` | Show model list |
| `SqlShowService` | Show service list |
| `SqlShowCheckpoint` | Show checkpoint list |
| `SqlRichDescribeTable` | Describe table structure |
| `SqlShowCreateTable` | Show create table statement |

### CRUD SQL Routing Decision

For SELECT, INSERT, UPDATE, DELETE and other CRUD statements, the system checks all involved tables:

```java
public static boolean isSqlTableRunable(SqlNode sqlNode, CalciteSchema schema, String defaultSchema) {
    List<String> tableNames = getTableFromSqlNode(sqlNode);
    for (String tableName : tableNames) {
        Table table = getTableObj(schema, defaultSchema, tableName);
        if (!(table instanceof SqlRecTable)) {
            return false;  // Forward to Flink
        }
    }
    return true;  // Local execution
}
```

**Decision Rules:**
- All tables are `SqlRecTable` subclasses → Local execution
- Contains non-`SqlRecTable` (like Hive tables) → Forward to Flink

### UNION Statement Special Handling

UNION statements are identified as special CRUD SQL:

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


## SQL Execution Logic

SQLRec supports different SQL query capabilities based on different table types. This section introduces the query operations supported by each table type and their implementation principles.

### Table Type and Query Capability Matrix

| Table Type | Filter Query | Primary Key Query | KV Join | Vector Search |
|------------|--------------|-------------------|---------|---------------|
| CacheTable | ✅ | ❌ | ❌ | ❌ |
| SqlRecKvTable | ✅ | ✅ | ✅ | ❌ |
| SqlRecVectorTable | ✅ | ✅ | ✅ | ✅ |

> **Write Operation Support**: Whether a table supports write operations (INSERT/UPDATE/DELETE) depends on whether it implements the `ModifiableTable` interface, not the table type itself. For example, both `SqlRecKvTable` and `KafkaCalciteTable` implement `ModifiableTable`, so they support write operations.

### CACHE TABLE Statement

`CACHE TABLE` is the most core statement in SQLRec, used to create memory cache tables, similar to variable assignment in programming languages.

#### Basic Syntax

```sql
CACHE TABLE table_name AS
SELECT * FROM source_table WHERE condition;
```

This line of code means:
1. Execute the `SELECT` query
2. Store the result in a memory table named `table_name`
3. Subsequent SQL can reference this table

#### Cache Table Features

Cache tables can be viewed as "table variables" with the following features:

- **Scope**: Globally visible in the current session
- **Lifecycle**: Automatically destroyed when the session ends
- **Type**: Table type, containing column definitions and data rows

#### Chain Processing

```sql
CACHE TABLE step1 AS
SELECT user_id, COUNT(*) as cnt FROM events GROUP BY user_id;

CACHE TABLE step2 AS
SELECT * FROM step1 WHERE cnt > 10;

CACHE TABLE final_result AS
SELECT * FROM step2 ORDER BY cnt DESC;
```

#### Created via Function Call

Cache tables can be created via function calls:

```sql
CACHE TABLE processed_data AS
CALL process_function(raw_data, config_table);
```

### Filter Query

All SqlRecTable subclasses support filter queries. The system implements filter condition pushdown through the `FilterableTableScan` node.

#### Filter Condition Pushdown Rules

```java
// SqlRecFilterTableScanRule
public static boolean test(TableScan scan) {
    final RelOptTable table = scan.getTable();
    return table.unwrap(FilterableTable.class) != null
            || table.unwrap(ProjectableFilterableTable.class) != null;
}
```

#### KV Table Primary Key Filter Optimization

For `SqlRecKvTable`, if `onlyFilterByPrimaryKey()` is set to true, only primary key filtering is supported:

```java
private boolean shouldFilterByPrimaryKey(SqlRecTable sqlRecTable) {
    if (sqlRecTable == null) return false;
    if (!(sqlRecTable instanceof SqlRecKvTable)) return false;
    SqlRecKvTable kvTable = (SqlRecKvTable) sqlRecTable;
    return kvTable.onlyFilterByPrimaryKey();
}
```

**Example:**

```sql
-- For KV tables with onlyFilterByPrimaryKey=true, the following query is valid
SELECT * FROM kv_table WHERE primary_key = 'key123';

-- The following query is invalid (non-primary key filter)
SELECT * FROM kv_table WHERE other_column = 'value';
```

### KV Join

KV Join is a join method unique to SqlRecKvTable, implementing efficient association through batch primary key queries.

#### Trigger Conditions

1. **Left table must be CacheTable** (in-memory data, can be traversed)
2. Join condition must be **equality condition** (`=`)
3. Right table must be `SqlRecKvTable`

```java
// SqlRecKvJoinRule check conditions
RexNode condition = join.getCondition();
try {
    KvJoinUtils.getJoinKeyColIndex(condition);
} catch (Exception e) {
    return; // Non-equality condition, don't apply this rule
}
```

#### Implementation Principle

The core of KV Join is batch querying the right table data via primary key:

```java
// KvJoinUtils.kvJoin
public static Enumerable kvJoin(
        Enumerable left,
        SqlRecKvTable rightTable,
        RexNode condition,
        JoinRelType joinType
) {
    // 1. Extract all Join Keys from left table
    Set<Object> joinKeys = new HashSet<>();
    for (Object[] leftValue : leftValues) {
        Object leftJoinKey = leftValue[leftJoinKeyColIndex];
        joinKeys.add(leftJoinKey);
    }
    
    // 2. Batch query right table data (using cache)
    Map<Object, List<Object[]>> rightValuesMap = 
        rightTable.getByPrimaryKeyWithCache(joinKeys);
    
    // 3. Associate left and right table data
    // ...
}
```

#### Supported Join Types

| Join Type | Description |
|-----------|-------------|
| INNER JOIN | Only return matching rows |
| LEFT JOIN | Return all left table rows, fill NULL when right table has no match |

**Example:**

```sql
-- KV Join example
SELECT o.*, u.user_name
FROM orders o
LEFT JOIN user_kv_table u ON o.user_id = u.user_id;
```

### Vector Search Join

Vector search Join is a join method unique to SqlRecVectorTable, associating via vector similarity.

#### Trigger Conditions

1. **Left table must be CacheTable** (in-memory data, can be traversed)
2. Project must contain **`ip()` function** (vector inner product)
3. Join condition must be **true** (unconditional join)
4. Right table must be `SqlRecVectorTable`
5. Must have **ORDER BY ... LIMIT** clause

```java
// SqlRecVectorJoinRule check conditions
if (!VectorJoinUtils.hasIpFunction(project)) {
    return; // Must have ip function
}
if (!VectorJoinUtils.isTrueCondition(join)) {
    return; // Join condition must be true
}
if (rightTable.unwrap(SqlRecVectorTable.class) == null) {
    return; // Right table must be vector table
}
```

#### Query Pattern

Typical query pattern for vector search Join:

```sql
SELECT 
    left.*,
    ip(left.embedding, right.embedding) as score
FROM left_table left
JOIN vector_table right ON true
WHERE right.category = 'electronics'  -- Optional filter condition
ORDER BY score DESC
LIMIT 10;
```

#### Implementation Principle

```java
// VectorJoinUtils.vectorJoin
public static Enumerable vectorJoin(
        Enumerable left,
        SqlRecVectorTable rightTable,
        RexNode filterCondition,      // Right table filter condition
        int leftEmbeddingColIndex,    // Left table vector column index
        String rightEmbeddingColName, // Right table vector column name
        int limit,                    // Return count
        List<Integer> projectColumns  // Projection columns
) {
    for (Object[] leftValue : leftValues) {
        // 1. Extract query vector from left table
        List<Float> embedding = DataTransformUtils.convertToFloatVec(
            leftValue[leftEmbeddingColIndex]
        );
        
        // 2. Vector similarity search
        List<Object[]> rightValues = rightTable.searchByEmbeddingWithScore(
            leftValue,
            embedding,
            rightEmbeddingColName,
            filterCondition,
            limit,
            vectorProjectColumns
        );
        
        // 3. Associate results
        // ...
    }
}
```

#### Configuration Parameters

| Parameter | Description | Default Value |
|-----------|-------------|---------------|
| `DEFAULT_VECTOR_SEARCH_LIMIT` | Default return count | Configuration item |

### UNION Operation

UNION operation is implemented through `EnumerableUnion`, using snake merge algorithm.

#### Implementation

```java
// EnumerableUnion.implement
Expression unionExp = Expressions.call(
    MergeUtils.class.getMethod("snakeMergeEnumerable", Iterable[].class), 
    inputExps
);
```

#### Snake Merge Algorithm

Snake merge is an efficient streaming merge algorithm suitable for merging multiple data sources:

```java
// MergeUtils.snakeMergeEnumerable
public static List<Object[]> snakeMergeEnumerable(Iterable<Object[]>... iterables) {
    // Snake traverse all input sources, output alternately
}
```

## Variable System

SQLRec manages runtime variables through `ExecuteContext`, providing programming language-like variable capabilities.

### Variable Setting

Use `SET` statement or API to set variables:

```sql
SET 'my_var' = 'my_value';
```

```java
context.setVariable("my_var", "my_value");
```

### Variable Retrieval

Use `GET()` expression to get variables:

```sql
-- Use in function call
CALL my_function(GET('table_name'));
```

### Variable Scope

| Feature | Description |
|---------|-------------|
| **Storage** | `ConcurrentHashMap` (thread-safe) |
| **Visibility** | Globally visible in current session |
| **Isolation** | Variables isolated between different sessions |

### Variables During Function Calls

A new execution context is created during function calls:

```java
ExecuteContext finalContext = context.clone();
finalContext.addFunNameToStack(funName);
```

- **Variable Sharing**: Cloned context shares variable mapping
- **Call Stack Isolation**: Each function call has an independent call stack


## Function System

SQLRec supports custom SQL functions. Functions are encapsulations of a group of SQL statements, similar to function definitions in programming languages.

### Function Definition

A complete function definition includes the following parts:

```sql
-- 1. Function declaration
CREATE SQL FUNCTION my_function;

-- 2. Parameter definition (optional, can define multiple)
DEFINE INPUT TABLE input_data (
    id INT,
    name VARCHAR(100),
    score DOUBLE
);

DEFINE INPUT TABLE config_table (
    threshold DOUBLE
);

-- 3. Function body (multiple SQL statements)
CACHE TABLE filtered AS
SELECT * FROM input_data WHERE score > (SELECT threshold FROM config_table LIMIT 1);

CACHE TABLE result AS
SELECT id, name, score FROM filtered ORDER BY score DESC;

-- 4. Return statement
RETURN result;
```

### Function Parameter Passing

SQLRec functions use **pass-by-value**, where parameters are tables (CacheTable) or variables.

#### Basic Call

```sql
CALL my_function(table1, table2);
```

#### Parameter Matching

The tables passed during the call must be compatible with the input table structure defined by the function:

```sql
-- Function definition
CREATE SQL FUNCTION process_data;
DEFINE INPUT TABLE input_data (id INT, value DOUBLE);
...
RETURN result;

-- When calling, my_table structure must be compatible with input_data
CALL process_data(my_table);
```

#### Dynamic Function Name

Use `GET()` expression to dynamically get function name:

```sql
-- Get function name from variable
CALL GET('function_name')(table1, table2) LIKE template_table;
```

When calling functions dynamically, you need to use the `LIKE` clause to specify the result table schema, because the function being called cannot be known at compile time, so the type cannot be inferred.

#### Asynchronous Call

Use the `ASYNC` keyword to execute functions asynchronously:

```sql
CALL my_function(input_table) ASYNC;
```

Asynchronous calls return immediately, and the function executes in a background thread. Suitable for scenarios where immediate results are not needed.

### Function Return Results

Functions return results through the `RETURN` statement.

#### Basic Return

```sql
RETURN result_table;
```

The return must be a CacheTable, usually a cache table created in the function body.

#### Empty Return

```sql
RETURN;
```

Functions can return no result, in which case the function only executes side effects (like writing data).

#### Using Return Values

Function return values can be used directly or stored in cache tables:

```sql
-- Use return value directly (in CACHE TABLE)
CACHE TABLE output AS
CALL my_function(input_table);

-- Return value is an enumerable result set
```


## Concurrency Model

SQLRec has built-in automatic parallel execution capabilities, automatically analyzing dependencies between SQL statements and executing them in parallel.

- **Read Dependency**: Statement reads a table
- **Write Dependency**: Statement writes to a table (like CACHE TABLE)
- **Variable Dependency**: Variable dependency exists between SET statements, UDFs using ExecuteContext, and function calls using variables

```sql
-- These two statements can be executed in parallel (no dependency)
CACHE TABLE a AS SELECT * FROM source1;
CACHE TABLE b AS SELECT * FROM source2;

-- This statement must wait for the first two to complete
CACHE TABLE c AS SELECT * FROM a UNION ALL SELECT * FROM b;
```

## Circular Dependency Detection

The system detects circular dependencies between functions through the call stack, preventing infinite recursion. 

When a function is called, the function name is pushed onto the call stack:

```java
ExecuteContext finalContext = context.clone();
finalContext.addFunNameToStack(funName);
```

Before calling a new function, check if the function already exists in the call stack:

```java
if (funNameStack.contains(funName)) {
    throw new RuntimeException("Circular dependency detected: " + funName);
}
```

## UDF

SQLRec supports implementing user-defined functions (UDF) through Java, which can be called directly in SQL.

### UDF Definition

A UDF is a regular Java class that needs to meet the following conditions:

1. **Must have an `eval` method**: This is the UDF entry point
2. **`eval` method can only have one**: Overloading is not supported
3. **Parameter type restrictions**: Only supports `CacheTable`, `String`, `ExecuteContext` three types

```java
public class MyTableFunction {
    
    public CacheTable eval(CacheTable inputTable, String config) {
        // Processing logic
        List<Object[]> results = processTable(inputTable, config);
        return new CacheTable("output", results, inputTable.getDataFields());
    }
}
```

### Parameter Injection

SQLRec automatically injects corresponding values based on the `eval` method parameter types:

| Parameter Type | Injection Source | SQL Syntax | Use Case |
|----------------|------------------|------------|----------|
| `CacheTable` | Passed cache table | Identifier (like `table_name`) | Table function |
| `String` | String literal or variable | `'value'` or `GET('var')` | Table function, Scalar function |
| `ExecuteContext` | Execution context | Auto-injected, no need to specify in SQL | Table function |
| `ConfigContext` | Configuration context | Auto-injected, no need to specify in SQL | Table function |
| `SqlRecDataContext` | SQLRec data context | Auto-injected, no need to specify in SQL | Scalar function |

`SqlRecDataContext` is an interface specifically designed for scalar UDFs, inheriting from Calcite's `DataContext`. It provides the ability to access execution context variables:

```java
public interface SqlRecDataContext extends DataContext {
    String getVariable(String key);
}
```

In scalar UDFs, you can retrieve variable values through `SqlRecDataContext`:

```java
public class GetFunction {
    public static String eval(DataContext context, String key) {
        if (!(context instanceof SqlRecDataContext)) {
            throw new IllegalArgumentException("context must be SqlRecDataContext");
        }
        SqlRecDataContext sqlRecDataContext = (SqlRecDataContext) context;
        return sqlRecDataContext.getVariable(key);
    }
}
```

Parameter injection example

```java
public class MyFunction {
    // Method signature
    public CacheTable eval(
        CacheTable input1,      // First parameter: table
        CacheTable input2,      // Second parameter: table
        String config,          // Third parameter: string
        ExecuteContext context  // Execution context (auto-injected)
    ) {
        // ...
    }
}
```

```sql
-- When calling, parameters match in order
CALL my_function(table1, table2, 'config_value');

-- Use GET to get string parameter
CALL my_function(table1, table2, GET('config_var'));
```

### Compile-time Return Data Schema Resolution

UDF return data schema (Schema) can be determined at compile time in three ways:

#### 1. Specify via LIKE Clause with Table

```sql
CALL my_function(input_table) LIKE template_table;
```

At compile time, the system gets the return data schema from `template_table`:

```java
if (!StringUtils.isEmpty(likeTableName)) {
    returnDataFields = getDataTypeByLikeTableName(likeTableName, schema);
}
```

#### 2. Specify via LIKE FUNCTION Clause

```sql
CALL my_function(input_table) LIKE FUNCTION 'template_function';
```

At compile time, the system gets the return data schema from the specified function:

```java
if (likeFunctionName != null) {
    SqlFunctionBindable likeFunctionBindable = compileManager.getSqlFunction(likeFunctionName);
    returnDataFields = likeFunctionBindable.getReturnDataFields();
}
```

#### 3. Infer via Executing eval Method

If there's no LIKE clause, the system executes the `eval` method once at compile time to infer the return schema:

```java
if (!isAsync && CacheTable.class.isAssignableFrom(evalMethod.getReturnType())) {
    Object outputTable = callEvalMethod(schema, new ExecuteContextImpl());
    returnDataFields = ((CacheTable) outputTable).getDataFields();
}
```

**Note**: This method requires the UDF to execute normally at compile time and cannot be an asynchronous call.

### UDF Registration

UDFs need to be registered in Hive Metastore (HMS) before they can be called. When registering, you need to specify the function name and the corresponding Java class fully qualified name:

```sql
-- Register UDF in HMS
CREATE FUNCTION my_function AS 'com.example.MyFunction';
```

When the system calls a function, it gets the function's class name via HMS and then dynamically loads it:

```java
// Get function object from HMS
org.apache.hadoop.hive.metastore.api.Function functionObj = HmsClient.getFunctionObj(db, funName);
// Get class name and load
String className = functionObj.getClassName();
Class<?> clazz = Class.forName(className);
```

### Function Lookup Priority

When calling a function, the system looks up in the following order:

1. **Java UDF**: Look up via `JavaFunctionUtils.getTableFunction()`
2. **SQL Function**: Look up via `CompileManager.getSqlFunction()`
3. **Not Found**: Throw exception

## Programming Model Summary

| Concept | Traditional Programming Analogy | SQLRec Implementation |
|---------|--------------------------------|----------------------|
| Variable | Variable assignment | `CACHE TABLE` |
| Function | Function definition | `CREATE SQL FUNCTION` |
| Parameter | Function parameters | `DEFINE INPUT TABLE` |
| Return value | return statement | `RETURN` |
| Function call | Function call | `CALL` |
| Dynamic dispatch | Reflection/dynamic loading | `GET()` |
| Concurrency | Multi-threading | Auto parallelism + virtual threads |
| Scope | Variable scope | Session level |
| Type system | Static typing | Table structure checking |
| UDF | External libraries/plugins | Java class + `eval` method |
| Table type | Data structure | `SqlRecTable` hierarchy |
| Execution routing | Compile target selection | Local execution / Forward to Flink |
| Filter query | Conditional filtering | `FilterableTableScan` + rule optimization |
| KV Join | Primary key association query | `SqlRecKvJoinRule` + batch primary key query |
| Vector search | Similarity matching | `SqlRecVectorJoinRule` + `ip()` function |
| UNION | Data merging | `EnumerableUnion` + snake merge algorithm |

SQLRec extends SQL from a declarative query language to a language with complete programming capabilities while maintaining SQL's simplicity and declarative nature.
