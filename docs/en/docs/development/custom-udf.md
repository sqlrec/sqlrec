# Custom UDFs

When built-in functions are not enough, implement a Java UDF and register it with `CREATE FUNCTION`. First decide whether the requirement is a row-by-row scalar calculation or a table-level operation.

## Prerequisites

The custom UDF JAR must be on SQLRec's runtime classpath. A table-function project needs the `sqlrec-common` version used by the deployment:

```xml
<dependency>
  <groupId>org.sqlrec</groupId>
  <artifactId>sqlrec-common</artifactId>
  <version>${sqlrec.version}</version>
</dependency>
```

## Implement a Scalar Function

Create a public Java class with a public method named `evaluate`. Its arguments receive SQL values and its return value becomes the function result:

```java
package com.example.udf;

public class PrefixFunction {
    public String evaluate(String value, String prefix) {
        return value == null ? null : prefix + value;
    }
}
```

After registration, use it in SQL expressions:

```sql
CREATE FUNCTION add_prefix AS 'com.example.udf.PrefixFunction';

SELECT add_prefix(CAST(item_id AS VARCHAR), 'item-')
FROM item_info;
```

## Implement a Table Function

A table function also uses `evaluate` as its entry point. Explicit SQL arguments support `CacheTable` and `String`. To access execution variables, declare an `ExecuteContext` or `ReadonlyContext` argument and SQLRec will inject it automatically.

```java
package com.example.udf;

import com.sqlrec.common.schema.CacheTable;

public class PassThroughFunction {
    public CacheTable evaluate(CacheTable input) {
        return input;
    }
}
```

After registration, invoke it with `CALL`:

```sql
CREATE FUNCTION pass_through AS 'com.example.udf.PassThroughFunction';

CACHE TABLE output AS
CALL pass_through(input_table);
```

A table function may overload `evaluate` and may use `String...` or `CacheTable...` as its final argument. Do not define overloads that can match the same call, because SQLRec cannot choose between them.

## Register and Deploy

1. Put the JAR containing the UDF and its dependencies on SQLRec's runtime classpath, then restart the affected processes.
2. Register the function name and fully qualified Java class name:

```sql
CREATE FUNCTION my_function AS 'com.example.udf.MyFunction';
```

3. Use minimal input to verify argument types, `NULL` behavior, and result fields.

With Hive Metastore, run `CREATE FUNCTION` in a session. With local `SQL_SCHEMA_DIR` metadata, put the same statement in an SQL file under that directory so SQLRec loads it at startup.

## Define the Result Schema

SQLRec must know a table function's result fields at compile time. For functions that need real data, call a network service, or have side effects, specify the result schema explicitly so compilation does not execute the function just to infer its schema:

```sql
CALL my_function(input_table) LIKE output_template;

CALL my_function(input_table)
LIKE FUNCTION 'another_function';
```

`LIKE output_template` reuses a template table's schema, while `LIKE FUNCTION` reuses another function's result schema. Without either clause, a function returning `CacheTable` may be invoked once during compilation to infer its fields.

## Pre-release Checklist

- The JAR is on every SQLRec process's classpath and has no dependency-version conflicts.
- The class and `evaluate` method are public, and the registered class name includes its package.
- Table functions expose only `CacheTable` and `String` as SQL arguments; users do not pass context arguments.
- `NULL`, empty-table, invalid-argument, and error behavior is defined.
- A table function with dynamic fields or side effects uses `LIKE` to fix its result schema.

Runnable minimal examples are available under `sqlrec-demo/src/main/java/com/sqlrec/demo/udf/`. See [Writing a Recommendation Flow](../guides/recommendation-flow.md#calling-functions) for SQL calls, dynamic result schemas, and asynchronous calls.
