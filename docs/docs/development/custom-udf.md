# 自定义 UDF

当内置函数无法满足需求时，可以编写 Java UDF 并通过 `CREATE FUNCTION` 注册。建议先确认需求应实现为逐行计算的标量函数，还是处理整张表的表函数。

## 开发准备

自定义 UDF 的 JAR 必须在 SQLRec 运行时类路径中。开发表函数时，需要依赖与部署环境版本一致的 `sqlrec-common`：

```xml
<dependency>
  <groupId>org.sqlrec</groupId>
  <artifactId>sqlrec-common</artifactId>
  <version>${sqlrec.version}</version>
</dependency>
```

## 实现标量函数

创建一个公共 Java 类，并提供名为 `evaluate` 的公共方法。方法参数是 SQL 输入值，返回值是函数结果：

```java
package com.example.udf;

public class PrefixFunction {
    public String evaluate(String value, String prefix) {
        return value == null ? null : prefix + value;
    }
}
```

注册后可以在 SQL 表达式中使用：

```sql
CREATE FUNCTION add_prefix AS 'com.example.udf.PrefixFunction';

SELECT add_prefix(CAST(item_id AS VARCHAR), 'item-')
FROM item_info;
```

## 实现表函数

表函数同样以 `evaluate` 作为入口。SQL 显式传入的参数支持 `CacheTable` 和 `String`；需要访问执行变量时，还可以声明由 SQLRec 自动注入的 `ExecuteContext` 或 `ReadonlyContext`。

```java
package com.example.udf;

import com.sqlrec.common.schema.CacheTable;

public class PassThroughFunction {
    public CacheTable evaluate(CacheTable input) {
        return input;
    }
}
```

注册后使用 `CALL` 调用：

```sql
CREATE FUNCTION pass_through AS 'com.example.udf.PassThroughFunction';

CACHE TABLE output AS
CALL pass_through(input_table);
```

表函数可以重载 `evaluate`，也可以在最后一个参数使用 `String...` 或 `CacheTable...`。不要同时定义会匹配同一次调用的重载，否则 SQLRec 无法确定应调用哪个方法。

## 注册与部署

1. 将包含 UDF 及其依赖的 JAR 放入 SQLRec 运行时类路径，并重启相关进程。
2. 使用函数名和 Java 类全限定名注册：

```sql
CREATE FUNCTION my_function AS 'com.example.udf.MyFunction';
```

3. 用最小输入验证函数的参数类型、`NULL` 行为和返回字段。

使用 Hive Metastore 时，可以在会话中执行 `CREATE FUNCTION`。使用 `SQL_SCHEMA_DIR` 本地元数据时，应把同一条语句写入该目录下的 SQL 文件，由 SQLRec 启动时加载。

## 返回表结构

SQLRec 需要在编译期知道表函数的返回字段。对于依赖真实数据、调用网络服务或有副作用的 UDF，建议显式指定结果结构，避免编译期为了推断结构而执行函数：

```sql
CALL my_function(input_table) LIKE output_template;

CALL my_function(input_table)
LIKE FUNCTION 'another_function';
```

`LIKE output_template` 复用模板表结构；`LIKE FUNCTION` 复用另一个函数的返回结构。不指定时，返回 `CacheTable` 的函数可能在编译期被调用一次以推断字段。

## 发布前检查

- JAR 已部署到所有 SQLRec 进程使用的类路径，依赖没有版本冲突。
- 类和 `evaluate` 方法是公共的，注册的类名包含完整包名。
- 表函数只把 `CacheTable`、`String` 作为 SQL 参数，上下文参数不需要用户传入。
- 明确定义 `NULL`、空表、非法参数和异常的处理方式。
- 对动态返回结构或有副作用的表函数使用 `LIKE` 固定返回字段。

项目内可运行的最小示例见 `sqlrec-demo/src/main/java/com/sqlrec/demo/udf/`。SQL 中的函数调用、动态返回结构和异步调用方式见[编写推荐流程](../guides/recommendation-flow.md#调用函数)。
