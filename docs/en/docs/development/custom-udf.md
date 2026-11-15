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

A table function uses `evaluate` as its entry point. Explicit SQL arguments support `CacheTable`, `String`, `Integer`, `Long`, `Double`, `Boolean`, and the corresponding primitive types. SQLRec automatically injects declared `ExecuteContext` or `ReadonlyContext` parameters; callers do not pass them.

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

`evaluate` may be overloaded and may end in a varargs parameter of a supported type. Overload selection prefers exact types, then numeric widening from INT to BIGINT/DOUBLE or BIGINT to DOUBLE. Equally good matches are ambiguous. Use a typed default with `GET`, such as `GET('limit', 100)`, for numeric or boolean arguments.

## Register and Deploy

1. Put the JAR containing the UDF and its dependencies on SQLRec's runtime classpath, then restart the affected processes.
2. Register the function name and fully qualified Java class name:

```sql
CREATE FUNCTION my_function AS 'com.example.udf.MyFunction';
```

3. Use minimal input to verify argument types, `NULL` behavior, and result fields.

With remote metadata, register functions through Beeline or JDBC. In SQL-file mode, put the definition under `SQL_SCHEMA_DIR` and restart, or enable `SQL_SCHEMA_DDL_ENABLED` for process-local runtime registration. Deploy the JAR in either case.

## Define the Result Schema

SQLRec must know a table function's result fields at compile time. For functions that need real data, call a network service, or have side effects, specify the result schema explicitly so compilation does not execute the function just to infer its schema:

```sql
CALL my_function(input_table) LIKE output_template;

CALL my_function(input_table)
LIKE FUNCTION 'another_function';
```

`LIKE output_template` reuses a template table's schema, while `LIKE FUNCTION` reuses another function's result schema. Without either clause, a function returning `CacheTable` may be invoked once during compilation to infer its fields. Set the process environment variable `ENABLE_JAVA_FUNCTION_SCHEMA_INFERENCE=false` to require an explicit schema.

## Pre-release Checklist

- The JAR is on every SQLRec process's classpath and has no dependency-version conflicts.
- The class and `evaluate` method are public, and the registered class name includes its package.
- Table functions use supported SQL argument types; callers do not pass context arguments.
- `NULL`, empty-table, invalid-argument, and error behavior is defined.
- A table function with dynamic fields or side effects uses `LIKE` to fix its result schema.

Runnable minimal examples are available under `sqlrec-demo/src/main/java/com/sqlrec/demo/udf/`. See [Writing a Recommendation Flow](../guides/recommendation-flow.md#calling-functions) for SQL calls, dynamic result schemas, and asynchronous calls.
