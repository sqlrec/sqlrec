# SQLRec Programming Model

SQLRec is a SQL-based data processing and machine learning programming framework. It extends standard SQL by introducing programming concepts such as variables, functions, and cached tables, enabling SQL to have programming language-like capabilities.

This document serves both SQL authors and SQLRec developers. SQL syntax, constraints, and examples describe the observable usage contract; Java class names and implementation snippets explain the internals and do not imply additional SQL capabilities. In this document, an "SQL function" is a function defined by multiple SQL statements, while a "Java UDF" is a table or scalar function registered through a Java `evaluate` method.

This document describes the language and runtime model. Whether business SQL is executable also depends on the deployed table schemas, primary keys, connector capabilities, and exact signatures and return schemas of registered functions. Examples use single quotes for strings and backticks for identifiers that require quoting; top-level statements in a multi-statement function are separated by semicolons.

## Table Type System

SQLRec defines a table type hierarchy, where different types of tables have different access characteristics.

### Type Hierarchy

```
SqlRecTable (abstract base class)
├── CacheTable          -- In-memory cache table
├── SqlRecKvTable       -- KV table (supports primary-key lookup)
└── Other connector tables -- for example, KafkaCalciteTable

VectorSearchable (interface)
└── Currently implemented by MilvusCalciteTable, which also extends SqlRecKvTable
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
- Supports repeated scans through `scan()`; it does not provide keyed random access
- Its lifecycle follows the current `SqlExecutor`/request context; a table created inside a function is scoped to that invocation
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
- Can enable a Caffeine query cache through connector configuration; no cache is used unless `initCache` is called
- Supports batch primary key queries

**Core Methods:**

| Method | Description |
|--------|-------------|
| `getPrimaryKeyIndex()` | Get primary key column index (abstract, implemented by subclasses) |
| `getByPrimaryKeyImpl(Set<Object> keySet)` | Batch query by primary key (abstract, implemented by subclasses for specific data source access) |
| `getByPrimaryKey(Set<Object> keySet)` | Batch lookup by primary key; when the Caffeine cache is initialized, returns hits first and calls `getByPrimaryKeyImpl` for misses |
| `initCache(int maxSize, long expireAfterWrite)` | Initialize query cache |
| `onlyFilterByPrimaryKey()` | Whether connector candidate predicates retain only safe primary-key filters (defaults to `true`) |
| `invalidateCache(Object[] row)` | Invalidate cache entry for the primary key of the given row |

**Cache Configuration:**

```java
// Initialize cache: max 10000 entries, expire 60 seconds after write
kvTable.initCache(10000, 60);
```

### VectorSearchable Interface

`VectorSearchable` is an independent vector-search interface. The current Milvus table both extends `SqlRecKvTable` and implements this interface, so it provides both KV lookup and vector search; the interface itself does not inherit KV-table capabilities.

**Features:**
- An implementation may also provide KV-table capabilities; this is not guaranteed by the interface itself
- Supports vector similarity search
- Supports ANN (Approximate Nearest Neighbor) queries

**Core Methods:**

```java
public interface VectorSearchable {
    List<VectorSearchResult> searchByEmbeddingImpl(VectorSearchRequest request);

    // searchByEmbedding wraps the implementation with type conversion and metrics.
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

### Major SQL Types Handled Directly by SQLRec

The following list helps developers understand execution dispatch; it is not a complete user-facing syntax list. See the later sections and the [SQL Syntax Reference](sql_reference.md) for authoritative syntax.

| SQL Type | Description |
|----------|-------------|
| `SqlCreateModel` | Create model |
| `SqlDropModel` | Drop model |
| `SqlTrainModel` | Train model |
| `SqlExportModel` | Export model |
| `SqlAlterModelDropCheckpoint` | Drop a model checkpoint |
| `SqlCreateService` | Create service |
| `SqlDropService` | Drop service |
| `SqlCreateApi` | Create API |
| `SqlDropApi` | Drop API |
| `SqlCreateSqlFunction` | Create SQL function |
| `SqlDropSqlFunction` | Drop SQL function |
| `SqlCache` | Cache table |
| `SqlCallSqlFunction` | Call function |
| `SqlAssert` | Assert |
| `SqlIfCache` | Conditional statement |
| `SqlReturn` | Return from an SQL function |
| `SqlDefineInputTable` | Define an SQL-function input table |
| `SqlSet` | Set variable |
| `SqlFlush` | Invalidate system caches |
| `SqlShowTables` | Show table list |
| `SqlShowSqlFunction` | Show function list |
| `SqlShowApi` | Show API list |
| `SqlShowModel` | Show model list |
| `SqlShowService` | Show service list |
| `SqlShowCheckpoint` | Show checkpoint list |
| `SqlRichDescribeTable` | Describe table structure |
| `SqlShowCreateTable` | Show create table statement |

Statements such as `USE` and `SHOW DATABASES` reuse Flink AST classes and may also be handled directly by SQLRec. Do not infer SQL availability solely from this Java class list.

### CRUD SQL Routing Decision

For SELECT, INSERT, UPDATE, DELETE and other CRUD statements, the system checks all involved tables:

```java
public static boolean isSqlTableRunnable(SqlNode sqlNode, CalciteSchema schema, String defaultSchema) {
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

Do not mix a `CacheTable` created by the current execution with an external table that only Flink can access in one CRUD statement. The entire statement is forwarded to Flink, which cannot see the in-process cache table. Convert the required data into a SQLRec-accessible table first, or split processing across an explicit execution boundary.

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

| Table type/capability | SQL filtering | Filter pushdown | KV Join right side | Vector Join right side |
|------------|--------------|-------------------|---------|---------------|
| CacheTable | ✅, after scanning | ❌ | ❌ | ❌ |
| SqlRecKvTable | ✅ | ✅, connector-dependent | ✅ | ❌ |
| A table implementing VectorSearchable | ✅ | ✅, connector-dependent | ✅ if it is also a KV table | ✅ |

"Right side" describes the role of an optimized operator, not general SQL Join support. A `CacheTable` is commonly used as the left input of a KV Join or Vector Join. Whether INSERT/UPDATE/DELETE works depends on the concrete implementation's `ModifiableTable` operations and cannot be inferred solely from the abstract table type.

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

- **Scope**: A top-level cache table is visible in the current `SqlExecutor`; a cache table created inside a function is visible only in that invocation's temporary schema and must be passed explicitly to another function
- **Lifecycle**: Destroyed with the current executor/request and never persisted as metadata
- **Type**: Table type, containing column definitions and data rows
- **Same-name replacement**: Running `CACHE TABLE` with the same name replaces that table in the current schema; function read/write dependencies ensure consumers of the previous result finish first

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

`CACHE TABLE ... AS CALL ... ASYNC` can be parsed but is rejected at runtime because an asynchronous call cannot synchronously provide a result to cache. Use a standalone `CALL ... ASYNC` instead.

### IF and ASSERT

`IF` evaluates one query or expression and executes one branch. Each branch can contain exactly one statement:

```sql
IF (SELECT COUNT(*) > 0 FROM source_table) THEN (
    CACHE TABLE result AS SELECT * FROM source_table
) ELSE (
    CACHE TABLE result AS SELECT * FROM fallback_table
);
```

The following constraints apply:

- The condition must return exactly one row and one column. Normal mode requires BOOLEAN; NULL is treated as false.
- Directly nesting another IF in THEN or ELSE is not supported, and a branch cannot contain multiple statements.
- If both branches use `CACHE TABLE`, they must write the same table name with identical column count, names, and types.
- If ELSE is omitted and the condition is false, a CACHE branch causes an empty cache table with the same schema to be registered.
- A branch may contain CRUD, `CACHE TABLE`, `CALL`, `SET`, `ASSERT`, or `RETURN`; `RETURN` is valid only inside an SQL function and follows the rules in “Function Return Results.”

The condition of `IF TIMEIN` returns milliseconds. For a positive timeout, a timeout or exception in THEN falls back to ELSE. A non-positive timeout executes THEN directly, and exceptions do not fall back in that case. TIMEIN requires ELSE, and both branches must either be CACHE statements or RETURN statements:

```sql
IF TIMEIN (SELECT timeout_ms FROM config_table) THEN (
    CACHE TABLE result AS CALL slow_function(input_table)
) ELSE (
    CACHE TABLE result AS SELECT * FROM fallback_table
);
```

`ASSERT` runs a query and validates every returned value. The query must return at least one row, every column must be BOOLEAN, and every value must be true; false or NULL terminates execution.

```sql
ASSERT SELECT COUNT(*) > 0 FROM source_table;
```

### Filter Query

Every scannable `SqlRecTable` can express SQL `WHERE` filtering, but this does not mean every table can push a predicate into its data source. `CacheTable` filters after an in-memory scan; connector tables implementing `FilterableTable` or `ProjectableFilterableTable` can use `FilterableTableScan` to attempt pushdown.

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

For `SqlRecKvTable`, `onlyFilterByPrimaryKey()` controls candidate predicates sent to the connector; it does not determine whether the SQL predicate is legal. When true, only a safe primary-key equality is used to retrieve candidate rows, while an outer `LogicalCalc` evaluates the complete original predicate to preserve SQL semantics:

```java
// SqlRecKvTable.scan
List<RexNode> candidateFilters = filters;
if (onlyFilterByPrimaryKey() && filters != null) {
    candidateFilters = FilterUtils.getPrimaryKeyFilters(
        filters, getPrimaryKeyIndex()
    );
}

// SqlRecFilterTableScanRule keeps the complete predicate in an outer LogicalCalc
```

**Example:**

```sql
-- A primary-key predicate enables efficient candidate lookup
SELECT * FROM kv_table WHERE primary_key = 'key123';

-- A non-primary-key predicate remains semantically valid but may scan more rows;
-- a connector that cannot scan without a primary key may fail at runtime
SELECT * FROM kv_table WHERE other_column = 'value';
```

### KV Join

KV Join is a join method unique to SqlRecKvTable, implementing efficient association through batch primary key queries.

#### Trigger Conditions

1. The left side must be a locally enumerable relation; materializing it with `CACHE TABLE` is recommended to avoid repeated external access
2. The Join condition must be one **equality** (`=`) between two column references, not a compound predicate
3. The right side must be a `SqlRecKvTable`, and its primary-key column should participate in the equality
4. Use only INNER JOIN or LEFT JOIN; RIGHT/FULL JOIN is unsupported

```java
// SqlRecKvJoinRule check conditions
RexNode condition = join.getCondition();
try {
    NodeUtils.getJoinKeyColIndex(condition);
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
        rightTable.getByPrimaryKey(joinKeys);
    
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

Vector search Join is a join method unique to tables implementing `VectorSearchable`, associating via vector similarity.

#### Trigger Conditions

1. The left side must be a locally enumerable relation and should normally be materialized as a `CacheTable`
2. The SELECT projection must directly contain **`ip(left_embedding, right_embedding)`**, whose operands are column references from opposite sides
3. The Join condition must be constant true, such as `ON TRUE` or `ON 1 = 1`
4. The right table must implement `VectorSearchable`
5. An **ORDER BY ... LIMIT** clause is required; inner-product similarity normally uses DESC
6. Use INNER JOIN; the current vector executor does not null-extend misses for LEFT/RIGHT/FULL JOIN
7. WHERE should contain only filters handled by the vector right side; filter the left input before the Join

```java
// SqlRecVectorJoinRule check conditions
if (!NodeUtils.hasIpFunction(project)) {
    return; // Must have ip function
}
if (!NodeUtils.isTrueCondition(join)) {
    return; // Join condition must be true
}
if (rightTable.unwrap(VectorSearchable.class) == null) {
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
INNER JOIN vector_table right ON true
WHERE right.category = 'electronics'  -- Optional filter condition
ORDER BY score DESC
LIMIT 10;
```

LIMIT is passed to ANN search **for each left input row**. With multiple left rows, the total result count may exceed LIMIT; it is not a normal global SQL limit.

#### Implementation Principle

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
        // 1. Extract query vector from left table
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

#### Configuration Parameters

| Parameter | Description | Default Value |
|-----------|-------------|---------------|
| `DEFAULT_VECTOR_SEARCH_LIMIT` | Default return count | 100 |

### UNION Operation

Local UNION is implemented through `SqlrecEnumerableUnion`, using a snake merge algorithm.

#### Implementation

```java
// SqlrecEnumerableUnion.implement
Expression unionExp = Expressions.call(
    MergeUtils.class.getMethod("snakeMergeEnumerable", Iterable[].class), 
    inputExps
);
```

#### Snake Merge Algorithm

Snake merge polls the input sources and alternates one row from each, then materializes all rows into a `List`. It is not lazy streaming output:

```java
// MergeUtils.snakeMergeEnumerable
public static <T> Enumerable<T> snakeMergeEnumerable(Iterable<T>... sources) {
    List<T> merged = snakeMerge(sources); // snakeMerge materializes an ArrayList
    return Linq4j.asEnumerable(merged);
}
```

Local set-operation routing currently recognizes only UNION. Do not assume INTERSECT or EXCEPT can execute locally together with an in-process `CacheTable`.

The current implementation does not deduplicate according to the `UNION` ALL flag, so local SQLRec queries should explicitly use `UNION ALL`. If deduplication is required, apply `SELECT DISTINCT` or `GROUP BY` after merging. Never rely on merge output order; add an outermost `ORDER BY` when stable ordering is required.

## Variable System

SQLRec manages runtime variables through `ExecuteContext`, providing programming language-like variable capabilities.

### Variable Setting

Use `SET` or the API to set variables. In SQL syntax, both key and value must be string literals; queries and arbitrary expressions are not accepted directly:

```sql
SET 'my_var' = 'my_value';
```

```java
context.setVariable("my_var", "my_value");
```

### Variable Retrieval

In a `CALL` function-name or string-argument position, use `GET()` to retrieve a variable or `GET_OR_DEFAULT()` to provide a fallback:

```sql
-- A string argument of a Java table function
CALL my_java_function(GET('config_value'));

-- Dynamic function name; LIKE is required to declare the return schema
CALL GET_OR_DEFAULT('function_name', 'default_function')(input_table)
LIKE FUNCTION 'default_function';
```

`GET()`/`GET_OR_DEFAULT()` cannot turn a string variable into an SQL-function table argument; SQL-function arguments must still be cache-table identifiers. When invoking the scalar UDF with the same name in a normal SELECT expression, backticks are recommended because the name is also an extension keyword:

```sql
SELECT `get_or_default`('experiment_group', 'control') AS experiment_group;
```

### Variable Scope

| Feature | Description |
|---------|-------------|
| **Storage** | `ConcurrentHashMap` (thread-safe) |
| **Visibility** | Visible to the current `SqlExecutor`/request and nested function calls |
| **Isolation** | Isolated between executors or requests |
| **Value type** | String; setting null removes the variable |

### Variables During Function Calls

A new execution context is created during function calls:

```java
ExecuteContext finalContext = context.clone();
finalContext.addFunNameToStack(funName);
```

- **Variable Sharing**: Cloned context shares variable mapping
- **Call Stack Isolation**: Each function call has an independent call stack
- **Execution order**: `SET` and Java UDFs accepting `ExecuteContext` are not parallelized, preventing variable side effects from being reordered with other nodes


## Function System

SQLRec supports custom SQL functions, which encapsulate a group of SQL statements. They differ from the Java UDFs described later: explicit SQL-function parameters are tables only, whereas a Java table UDF can accept cache tables, strings, and automatically injected contexts.

### Function Definition

A complete function definition includes the following parts:

```sql
-- 1. Function declaration; use OR REPLACE to overwrite an existing definition
CREATE OR REPLACE SQL FUNCTION my_function;

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

A function definition is submitted as multiple independent SQL statements in this strict stage order:

1. One `CREATE [OR REPLACE] SQL FUNCTION` statement.
2. Zero or more consecutive `DEFINE INPUT TABLE` statements; `DEFINE INPUT TABLE input_data LIKE existing_table` is also supported.
3. Function-body statements. DEFINE cannot be added after the body has started.
4. One top-level `RETURN` terminates the definition. It is still required when every IF branch returns.

### Function Parameter Passing

Explicit parameters of an SQLRec SQL function are tables. Every argument must be a `CacheTable` identifier. The caller's table is bound to the corresponding `DEFINE INPUT TABLE` name in a temporary schema without copying the entire table, and the function uses the input as a read-only data source.

#### Basic Call

```sql
CALL my_function(table1, table2);
```

#### Parameter Matching

Passed tables must be compatible with the input schema declared by the function. Compatibility means:

- The actual table may have more columns than the declaration, but not fewer.
- The first N columns are compared by position; names are case-insensitive but must match.
- SQL types must match, except that CHAR/VARCHAR and other string types are mutually compatible.
- Columns are not reordered by name, so order matters.

```sql
-- Function definition
CREATE SQL FUNCTION process_data;
DEFINE INPUT TABLE input_data (id INT, value DOUBLE);
...
RETURN result;

-- The first two columns of my_table must be id and value, compatible with INT and DOUBLE
CALL process_data(my_table);
```

#### Dynamic Function Name

Use `GET()` or `GET_OR_DEFAULT()` to obtain a function name dynamically:

```sql
-- Get function name from variable
CALL GET('function_name')(table1, table2) LIKE template_table;

-- Call default_function when the variable is absent
CALL GET_OR_DEFAULT('function_name', 'default_function')(table1, table2)
LIKE FUNCTION 'default_function';
```

When calling functions dynamically, you need to use the `LIKE` clause to specify the result table schema, because the function being called cannot be known at compile time, so the type cannot be inferred.

#### Asynchronous Call

Use the `ASYNC` keyword to execute functions asynchronously:

```sql
CALL my_function(input_table) ASYNC;
```

An asynchronous call only submits work to a background thread and returns immediately; it exposes no synchronously consumable result. Use it for side-channel writes or logging. It cannot appear in `CACHE TABLE ... AS CALL` or `RETURN CALL`, and later SQL must not assume its data or side effects have completed.

#### Partitioned Call

`PARTITION BY` splits a cache table into fixed-size row groups, invokes the function concurrently, and merges partition results:

```sql
CALL my_function(input_table)
LIKE FUNCTION 'my_function'
PARTITION BY input_table SIZE 100;
```

The partition table must also be an argument of CALL. SIZE must be an integer literal and should be positive. `LIKE` must precede `PARTITION BY`, and `ASYNC` must be last. Partition merging does not guarantee business ordering; cache the result and apply a separate `ORDER BY` when stable ordering is needed.

### Function Return Results

Functions return results through the `RETURN` statement. Once a RETURN executes at runtime, the current function finishes immediately and later nodes that have not started are skipped.

#### Supported Return Forms

```sql
-- Return an existing cache table
RETURN result_table;

-- Return a query directly
RETURN SELECT id, score FROM result_table;

-- Return a synchronous function invocation
RETURN CALL post_process(result_table);

-- Return no data
RETURN;
```

The table-name form requires a `CacheTable`, usually created with `CACHE TABLE` in the function body. `RETURN SELECT` and `RETURN CALL` use the statement result directly and do not create an internal anonymous cache table. `RETURN CALL ... ASYNC` is not supported.

#### Top-level Terminator and Early Return

```sql
CREATE SQL FUNCTION find_candidates;
DEFINE INPUT TABLE candidates (id BIGINT, score DOUBLE);

-- RETURN inside IF exits at runtime but does not terminate the definition
IF (SELECT COUNT(*) = 0 FROM candidates) THEN (
    RETURN SELECT id, score FROM candidates
);

-- The top-level RETURN still terminates the definition and handles false
RETURN SELECT id, score FROM candidates ORDER BY score DESC;
```

A function definition must end with a top-level `RETURN`, and no function-body statement may follow it. A RETURN inside IF is not the definition terminator; it exits only when that branch executes.

An IF that uses RETURN must have one of these branch shapes:

- THEN returns and ELSE is omitted; a false condition continues after the IF.
- Both THEN and ELSE return, with compatible result schemas.

An ELSE-only return and a returning THEN paired with an ordinary ELSE statement are rejected. Across the whole function, all possible return points must either be empty, or have identical column counts, names, and types; CHAR/VARCHAR and other string types are mutually compatible.

`IF TIMEIN` can also use RETURN in both branches. When the timeout is positive, a timeout or exception in THEN discards its temporary return state and ELSE supplies the final result; a non-positive timeout executes THEN directly.

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

This behavior is controlled by `PARALLELISM_EXEC` and is enabled by default. When disabled, the function body runs in definition order. When enabled, textual order alone is not a dependency; SQLRec builds an execution graph mainly from table reads/writes and non-parallelizable nodes:

- **Read Dependency**: Statement reads a table
- **Write Dependency**: Statement writes to a table (like CACHE TABLE)
- **Execution Barrier**: `SET`, UDFs accepting `ExecuteContext`, ASSERT, and an IF containing RETURN constrain ordering around non-parallelizable nodes

```sql
-- These two statements can be executed in parallel (no dependency)
CACHE TABLE a AS SELECT * FROM source1;
CACHE TABLE b AS SELECT * FROM source2;

-- This statement must wait for the first two to complete
CACHE TABLE c AS SELECT * FROM a UNION ALL SELECT * FROM b;
```

Statements that require ordering should therefore express it through explicit table read/write dependencies rather than source order alone. Completion of `CALL ... ASYNC` is not part of the subsequent synchronous dependency graph.

## Circular Dependency Detection

Static SQL-function calls are checked in the compile-time dependency graph. A dynamic function name cannot be resolved at compile time, so the runtime call stack also prevents infinite recursion.

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

This section explains the UDF invocation and development model. When writing business SQL with built-in UDFs, also consult [Built-in UDFs](udf.md) for exact signatures, parameter meanings, and output schemas; a function name alone is not enough to infer a valid call.

### UDF Definition

A UDF is a regular Java class that needs to meet the following conditions:

1. **Must have one or more `evaluate` methods**: This is the UDF entry point
2. **Table-UDF parameter restrictions**: Explicit SQL arguments support only `CacheTable` and `String`; `ExecuteContext` or `ReadonlyContext` may be injected automatically
3. **Return type**: A UDF producing table data returns `CacheTable`; a side-effect-only UDF may return `Void`, but its result cannot be used by CACHE or RETURN

```java
public class MyTableFunction {
    
    public CacheTable evaluate(CacheTable inputTable, String config) {
        // Processing logic
        List<Object[]> results = processTable(inputTable, config);
        return new CacheTable("output", results, inputTable.getDataFields());
    }
}
```

### Method Overloading

Table functions support method overloading, allowing you to define multiple `evaluate` methods with the same name but different parameters. The system automatically selects the matching method based on the parameter types and count passed during invocation.

**Example**:

```java
public class MyFunction {
    // No-argument version
    public CacheTable evaluate() {
        // ...
    }
    
    // Single string argument version
    public CacheTable evaluate(String arg) {
        // ...
    }
    
    // Two string arguments version
    public CacheTable evaluate(String arg1, String arg2) {
        // ...
    }
    
    // Table argument version
    public CacheTable evaluate(CacheTable input) {
        // ...
    }
    
    // Mixed arguments version
    public CacheTable evaluate(CacheTable input, String arg) {
        // ...
    }
}
```

**Usage Example**:

```sql
-- Call no-argument version
CALL my_function();

-- Call single-argument version
CALL my_function('arg1');

-- Call two-argument version
CALL my_function('arg1', 'arg2');

-- Call table-argument version
CALL my_function(input_table);

-- Call mixed-argument version
CALL my_function(input_table, 'arg1');
```

::: warning Note
If multiple methods can match the call parameters, the system will throw an exception. For example, if you define both `evaluate(String arg)` and `evaluate(String... args)`, calling `my_function('arg1')` will cause an error because both methods match.
:::

### Variable Arguments (Varargs)

Table functions support Java variable arguments (varargs), defined using `...` syntax.

**Example**:

```java
public class MyFunction {
    // String varargs
    public CacheTable evaluate(String... args) {
        for (String arg : args) {
            System.out.println(arg);
        }
        // ...
    }
    
    // Table varargs
    public CacheTable evaluate(CacheTable... tables) {
        for (CacheTable table : tables) {
            // Process each table
        }
        // ...
    }
    
    // Mixed arguments: fixed parameter + varargs
    public CacheTable evaluate(String prefix, CacheTable... tables) {
        // prefix is a required parameter
        // tables is a variable argument
        // ...
    }
}
```

**Usage Example**:

```sql
-- String varargs
CALL my_function('arg1');
CALL my_function('arg1', 'arg2');
CALL my_function('arg1', 'arg2', 'arg3');

-- Table varargs
CALL my_function(table1);
CALL my_function(table1, table2);
CALL my_function(table1, table2, table3);

-- Mixed arguments
CALL my_function('prefix', table1);
CALL my_function('prefix', table1, table2);
```

**Notes**:
- Varargs can accept 0 to multiple arguments
- Varargs must be the last parameter of the method
- The SQL invocation layer currently supports only `String...` or `CacheTable...`; other vararg types are rejected

### Parameter Injection

SQLRec automatically injects corresponding values based on the `evaluate` method parameter types:

| Parameter Type | Injection Source | SQL Syntax | Use Case |
|----------------|------------------|------------|----------|
| `CacheTable` | Passed cache table | Identifier (like `table_name`) | Table function |
| `String` | String literal or variable | `'value'` or `GET('var')` | Table function, Scalar function |
| `ExecuteContext` | Execution context | Auto-injected, no need to specify in SQL | Table function |
| `ReadonlyContext` | Readonly context | Auto-injected, no need to specify in SQL | Table function |
| `DataContext` (a `SqlRecDataContext` at runtime) | SQLRec data context | Injected by Calcite, no need to specify in SQL | Scalar function |

`SqlRecDataContext` is an interface specifically designed for scalar UDFs, inheriting from Calcite's `DataContext`. It provides the ability to access execution context variables:

```java
public interface SqlRecDataContext extends DataContext {
    String getVariable(String key);
}
```

In scalar UDFs, you can retrieve variable values through `SqlRecDataContext`:

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

Parameter injection example

```java
public class MyFunction {
    // Method signature
    public CacheTable evaluate(
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

#### 3. Infer via Executing evaluate Method

If there is no LIKE clause and `evaluate` returns `CacheTable`, the system executes that method once at compile time to infer the return schema:

```java
if (CacheTable.class.isAssignableFrom(evalMethod.getReturnType())) {
    Object outputTable = callEvalMethod(schema, new ExecuteContextImpl());
    returnDataFields = ((CacheTable) outputTable).getDataFields();
}
```

**Note**: This requires the UDF to run successfully with an empty execution context and placeholder inputs, and compilation may trigger network requests or other side effects. For dynamic functions, asynchronous calls, functions that depend on real data, or side-effecting UDFs, explicitly use `LIKE table` or `LIKE FUNCTION 'function_name'` to avoid executing real logic solely for schema inference.

### UDF Registration

UDFs must be registered in SQLRec's active metadata mode before they can be called. With Hive Metastore (HMS), specify the function name and fully qualified Java class name:

```sql
-- Register UDF in HMS
CREATE FUNCTION my_function AS 'com.example.MyFunction';
```

With `SQL_SCHEMA_DIR` local metadata mode, place the same `CREATE FUNCTION` in an SQL file under that directory so SQLRec loads it at startup. This mode does not require HMS and does not allow the running CLI/API session to persist this kind of DDL directly.

In HMS mode, the system gets the function's class name from HMS and dynamically loads it:

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
| Table variable | Intermediate-result assignment | `CACHE TABLE` |
| String variable | Request-level configuration | `SET`, `GET()`, `GET_OR_DEFAULT()` |
| Function | Function definition | `CREATE SQL FUNCTION` |
| Parameter | Function parameters | `DEFINE INPUT TABLE` |
| Return value | return statement | `RETURN` |
| Function call | Function call | `CALL` |
| Dynamic dispatch | Reflection/dynamic loading | `GET()` / `GET_OR_DEFAULT()` + `LIKE` |
| Concurrency | Multi-threading | Configurable auto parallelism + virtual threads |
| Scope | Variable scope | Current executor/request; function cache tables are invocation-scoped |
| Type system | Static typing | Table structure checking |
| UDF | External libraries/plugins | Java class + `evaluate` method |
| Table type | Data structure | `SqlRecTable` hierarchy |
| Execution routing | Compile target selection | Local execution / Forward to Flink |
| Filter query | Conditional filtering | `FilterableTableScan` + rule optimization |
| KV Join | Primary key association query | `SqlRecKvJoinRule` + batch primary key query |
| Vector search | Similarity matching | `SqlRecVectorJoinRule` + `ip()` function |
| UNION ALL | Data merging | `SqlrecEnumerableUnion` + snake merge algorithm |

SQLRec adds table variables, multi-statement functions, control flow, runtime variables, and parallel execution to declarative SQL. SQL authors must still follow the table-type, function-argument, control-flow, and execution-routing constraints described in this document.
