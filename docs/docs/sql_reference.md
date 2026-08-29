# SQLRec SQL 语法参考

本文档介绍 SQLRec 支持的扩展 SQL 语法。

## 模型管理

### CREATE MODEL

创建一个新的机器学习模型定义。

**语法：**

```sql
CREATE MODEL [IF NOT EXISTS] model_name 
    [(column_name column_type [, ...])]
    [WITH (property_name = property_value [, ...])]
```

**参数：**

| 参数 | 描述 |
|------|------|
| `IF NOT EXISTS` | 可选。如果模型已存在，则不创建新模型，也不报错 |
| `model_name` | 模型名称，必须是有效的标识符 |
| `column_name` | 列名 |
| `column_type` | 列数据类型 |
| `property_name` | 属性名 |
| `property_value` | 属性值 |

**示例：**

```sql
CREATE MODEL my_model (
    id INT,
    name VARCHAR(100),
    score DOUBLE
) WITH (
    model_type = 'classification',
    version = '1.0'
);

CREATE MODEL IF NOT EXISTS my_model;
```


### DROP MODEL

删除一个已存在的模型。

**语法：**

```sql
DROP MODEL [IF EXISTS] model_name
```

**参数：**

| 参数 | 描述 |
|------|------|
| `IF EXISTS` | 可选。如果模型不存在，则不报错 |
| `model_name` | 要删除的模型名称 |

**示例：**

```sql
DROP MODEL my_model;

DROP MODEL IF EXISTS my_model;
```


### TRAIN MODEL

训练一个模型并创建检查点。

**语法：**

```sql
TRAIN MODEL model_name CHECKPOINT = 'checkpoint_name'
    ON data_source
    [WHERE condition]
    [FROM 'existing_checkpoint']
    [WITH (property_name = property_value [, ...])]
```

**参数：**

| 参数 | 描述 |
|------|------|
| `model_name` | 要训练的模型名称 |
| `checkpoint_name` | 检查点名称，用于标识训练结果 |
| `data_source` | 训练数据源表名 |
| `condition` | 可选。WHERE 条件，用于过滤训练数据 |
| `existing_checkpoint` | 可选。基于已有检查点继续训练 |
| `property_name` | 可选。训练属性名 |
| `property_value` | 可选。训练属性值 |

**示例：**

```sql
TRAIN MODEL my_model CHECKPOINT = 'v1.0'
    ON training_data
    WHERE status = 'active';

TRAIN MODEL my_model CHECKPOINT = 'v2.0'
    ON training_data
    FROM 'v1.0'
    WITH (epochs = 100, learning_rate = 0.01);
```


### EXPORT MODEL

导出模型的训练结果。

**语法：**

```sql
EXPORT MODEL model_name CHECKPOINT = 'checkpoint_name'
    [ON data_source]
    [WHERE condition]
    [WITH (property_name = property_value [, ...])]
```

**参数：**

| 参数 | 描述 |
|------|------|
| `model_name` | 要导出的模型名称 |
| `checkpoint_name` | 检查点名称 |
| `data_source` | 可选。导出目标数据源 |
| `condition` | 可选。WHERE 条件 |
| `property_name` | 可选。导出属性名 |
| `property_value` | 可选。导出属性值 |

**示例：**

```sql
EXPORT MODEL my_model CHECKPOINT = 'v1.0'
    ON export_table;

EXPORT MODEL my_model CHECKPOINT = 'v1.0'
    ON export_table
    WHERE status = 'valid'
    WITH (format = 'parquet');
```


### SHOW MODELS

显示所有模型列表。

**语法：**

```sql
SHOW MODELS
```

**示例：**

```sql
SHOW MODELS;
```


### DESCRIBE MODEL

显示模型的创建语句或检查点信息。

**语法：**

```sql
{DESCRIBE | DESC} [FORMATTED] MODEL model_name [CHECKPOINT = 'checkpoint_name']
```

**参数：**

| 参数 | 描述 |
|------|------|
| `FORMATTED` | 可选。以格式化表格形式显示详细信息，包括模型信息、输入字段、输出字段和模型参数 |
| `model_name` | 模型名称 |
| `checkpoint_name` | 可选。检查点名称，如果指定则显示该检查点的详细信息 |

**示例：**

```sql
DESCRIBE MODEL my_model;

DESC MODEL my_model CHECKPOINT = 'v1.0';

DESCRIBE FORMATTED MODEL my_model;

DESCRIBE FORMATTED MODEL my_model CHECKPOINT = 'v1.0';
```


### SHOW CHECKPOINTS

显示指定模型的所有检查点列表。

**语法：**

```sql
SHOW CHECKPOINTS model_name
```

**参数：**

| 参数 | 描述 |
|------|------|
| `model_name` | 模型名称 |

**示例：**

```sql
SHOW CHECKPOINTS my_model;
```


### ALTER MODEL DROP CHECKPOINT

删除模型的指定检查点。

**语法：**

```sql
ALTER MODEL model_name DROP [IF EXISTS] CHECKPOINT = 'checkpoint_name'
```

**参数：**

| 参数 | 描述 |
|------|------|
| `model_name` | 模型名称 |
| `IF EXISTS` | 可选。如果检查点不存在，则不报错 |
| `checkpoint_name` | 要删除的检查点名称 |

**示例：**

```sql
ALTER MODEL my_model DROP CHECKPOINT = 'v1.0';

ALTER MODEL my_model DROP IF EXISTS CHECKPOINT = 'v1.0';
```


## 服务管理

### CREATE SERVICE

创建一个模型服务。

**语法：**

```sql
CREATE SERVICE [IF NOT EXISTS] service_name
    ON MODEL model_name
    [CHECKPOINT = 'checkpoint_name']
    [WITH (property_name = property_value [, ...])]
```

**参数：**

| 参数 | 描述 |
|------|------|
| `IF NOT EXISTS` | 可选。如果服务已存在，则不创建新服务，也不报错 |
| `service_name` | 服务名称 |
| `model_name` | 关联的模型名称 |
| `checkpoint_name` | 可选。使用的检查点名称 |
| `property_name` | 可选。服务属性名 |
| `property_value` | 可选。服务属性值 |

**示例：**

```sql
CREATE SERVICE my_service
    ON MODEL my_model
    CHECKPOINT = 'v1.0';

CREATE SERVICE IF NOT EXISTS my_service
    ON MODEL my_model
    CHECKPOINT = 'v1.0'
    WITH (port = 8080, replicas = 3);
```


### DROP SERVICE

删除一个已存在的服务。

**语法：**

```sql
DROP SERVICE [IF EXISTS] service_name
```

**参数：**

| 参数 | 描述 |
|------|------|
| `IF EXISTS` | 可选。如果服务不存在，则不报错 |
| `service_name` | 要删除的服务名称 |

**示例：**

```sql
DROP SERVICE my_service;

DROP SERVICE IF EXISTS my_service;
```


### SHOW SERVICES

显示所有服务列表。

**语法：**

```sql
SHOW SERVICES
```

**示例：**

```sql
SHOW SERVICES;
```


### DESCRIBE SERVICE

显示服务的创建语句。

**语法：**

```sql
{DESCRIBE | DESC} [FORMATTED] SERVICE service_name
```

**参数：**

| 参数 | 描述 |
|------|------|
| `FORMATTED` | 可选。以格式化表格形式显示详细信息，包括服务信息、关联模型信息和模型字段 |
| `service_name` | 服务名称 |

**示例：**

```sql
DESCRIBE SERVICE my_service;

DESC SERVICE my_service;

DESCRIBE FORMATTED SERVICE my_service;
```


## API 管理

### CREATE API

创建一个 API 接口，关联到指定的 SQL 函数。

**语法：**

```sql
CREATE [OR REPLACE] API api_name WITH function_name
```

**参数：**

| 参数 | 描述 |
|------|------|
| `OR REPLACE` | 可选。如果 API 已存在，则替换现有定义 |
| `api_name` | API 名称 |
| `function_name` | 关联的 SQL 函数名称 |

**示例：**

```sql
CREATE API my_api WITH my_function;

CREATE OR REPLACE API my_api WITH my_function;
```


### DROP API

删除一个已存在的 API。

**语法：**

```sql
DROP API [IF EXISTS] api_name
```

**参数：**

| 参数 | 描述 |
|------|------|
| `IF EXISTS` | 可选。如果 API 不存在，则不报错 |
| `api_name` | 要删除的 API 名称 |

**示例：**

```sql
DROP API my_api;

DROP API IF EXISTS my_api;
```


### SHOW APIS

显示所有 API 列表。

**语法：**

```sql
SHOW APIS
```

**示例：**

```sql
SHOW APIS;
```


### DESCRIBE API

显示 API 的创建语句。

**语法：**

```sql
{DESCRIBE | DESC} API api_name
```

**参数：**

| 参数 | 描述 |
|------|------|
| `api_name` | API 名称 |

**示例：**

```sql
DESCRIBE API my_api;

DESC API my_api;
```


## SQL 函数管理

### CREATE SQL FUNCTION

创建一个自定义 SQL 函数。

**语法：**

```sql
CREATE [OR REPLACE] SQL FUNCTION function_name
```

**描述：**

此语句开始一个函数定义块，后续需要输入函数体的 SQL 语句，直到函数编译完成。

**参数：**

| 参数 | 描述 |
|------|------|
| `OR REPLACE` | 可选。如果函数已存在，则替换现有定义 |
| `function_name` | 函数名称 |

**示例：**

```sql
CREATE SQL FUNCTION my_function;

CREATE OR REPLACE SQL FUNCTION my_function;
```


### DEFINE INPUT TABLE

定义一个输入表的结构，用于在 SQL 函数中声明输入参数的表结构。

**语法：**

```sql
DEFINE INPUT TABLE table_name (
    column_name1 column_type1,
    column_name2 column_type2,
    ...
)

-- 或者使用 LIKE 子句复制已有表的结构
DEFINE INPUT TABLE table_name LIKE existing_table
```

**参数：**

| 参数 | 描述 |
|------|------|
| `table_name` | 输入表名称 |
| `column_name` | 列名 |
| `column_type` | 列数据类型 |
| `existing_table` | 已存在的表名，用于复制其表结构 |

**描述：**

`DEFINE INPUT TABLE` 支持两种方式定义输入表结构：
1. **显式定义**：直接指定列名和列类型
2. **LIKE 子句**：复制已有表的结构，包括所有列名和类型

**示例：**

```sql
-- 显式定义列
DEFINE INPUT TABLE input_data (
    id INT,
    name VARCHAR(100),
    score DOUBLE,
    created_at TIMESTAMP
);

-- 使用 LIKE 子句复制表结构
DEFINE INPUT TABLE input_data LIKE source_table;
```


### RETURN

从 SQL 函数中返回结果并提前结束本次函数执行。`RETURN` 可以直接返回缓存表，也可以执行 `SELECT` 或同步 `CALL` 并返回其结果。

**语法：**

```sql
RETURN
RETURN table_name
RETURN select_statement
RETURN CALL function_name([arg1, arg2, ...]) [LIKE {like_table | FUNCTION 'function_name'}] [PARTITION BY table_name SIZE partition_size]
```

**参数：**

| 参数 | 描述 |
|------|------|
| `table_name` | 要返回的缓存表（`CacheTable`）名称，不能是普通外部表 |
| `select_statement` | 作为函数结果返回的 SELECT 查询 |
| `CALL ...` | 调用 SQL/Java 函数并返回其结果；动态调用仍需通过 `LIKE` 指定返回模式 |

**规则：**

- `RETURN;` 表示函数正常结束但不返回数据。
- 顶层 `RETURN` 是 SQL 函数定义的结束标志；即使函数体中的 `IF` 已包含提前返回，函数定义末尾仍需提供顶层 `RETURN`。顶层 `RETURN` 之后不能再定义其他函数体语句。
- `IF` 分支内的 `RETURN` 只在运行时提前结束函数，不会在编译时结束函数定义。
- 函数内所有可能执行的 `RETURN` 必须具有一致的返回模式：要么全部为空返回，要么全部返回列数、列名和列类型一致的数据。
- `RETURN CALL ... ASYNC` 不受支持，因为异步调用无法作为当前函数的同步返回值。

**示例：**

```sql
-- 空返回
RETURN;

-- 返回已有缓存表
RETURN result_table;

-- 直接返回查询结果，无需先创建匿名缓存表
RETURN SELECT id, score FROM candidates ORDER BY score DESC;

-- 返回函数调用结果
RETURN CALL rerank(candidates);

-- IF 中提前返回；最后一条顶层 RETURN 仍是函数定义结束标志，
-- 并在条件为 false 时作为后备返回
IF (SELECT COUNT(*) = 0 FROM candidates) THEN (
    RETURN SELECT CAST(NULL AS BIGINT) AS id WHERE FALSE
);
RETURN SELECT id FROM candidates;
```


### DROP SQL FUNCTION

删除一个已存在的 SQL 函数。

**语法：**

```sql
DROP SQL FUNCTION [IF EXISTS] function_name
```

**参数：**

| 参数 | 描述 |
|------|------|
| `IF EXISTS` | 可选。如果函数不存在，则不报错 |
| `function_name` | 要删除的函数名称 |

**注意：** 如果函数被 API 引用，则无法删除。

**示例：**

```sql
DROP SQL FUNCTION my_function;

DROP SQL FUNCTION IF EXISTS my_function;
```


### SHOW SQL FUNCTIONS

显示所有 SQL 函数列表。

**语法：**

```sql
SHOW SQL FUNCTIONS
```

**示例：**

```sql
SHOW SQL FUNCTIONS;
```


### DESCRIBE SQL FUNCTION

显示 SQL 函数的创建语句。

**语法：**

```sql
{DESCRIBE | DESC} SQL FUNCTION function_name
```

**参数：**

| 参数 | 描述 |
|------|------|
| `function_name` | 函数名称 |

**示例：**

```sql
DESCRIBE SQL FUNCTION my_function;

DESC SQL FUNCTION my_function;
```


## 缓存管理

### CACHE TABLE

缓存查询结果或函数调用结果到指定表。

**语法：**

```sql
CACHE TABLE table_name AS 
    {CALL function_name([arg1, arg2, ...]) [LIKE {like_table | FUNCTION 'function_name'}] [PARTITION BY table_name SIZE partition_size] [ASYNC]
     | select_statement}
```

**参数：**

| 参数 | 描述 |
|------|------|
| `table_name` | 缓存表名称 |
| `function_name` | 要调用的函数名称，可以是标识符或 `GET()` 表达式 |
| `arg1, arg2, ...` | 函数参数，可以是标识符、`GET()` 表达式或字符串字面量 |
| `like_table` | 可选。指定结果表的模板表 |
| `FUNCTION 'function_name'` | 可选。指定结果表的模式与某个函数的输出模式相同 |
| `PARTITION BY table_name SIZE partition_size` | 可选。按指定输入表进行分区并发执行，`table_name` 必须是函数的输入表之一，`partition_size` 为每个分区的最大行数 |
| `ASYNC` | 可选。异步执行（仅在独立的 CALL 语句中支持，CACHE TABLE 中暂不支持） |
| `select_statement` | SELECT 查询语句 |

**示例：**

```sql
CACHE TABLE cached_result AS
SELECT * FROM source_table WHERE status = 'active';

CACHE TABLE cached_result AS
CALL my_function('param1', 'param2');

CACHE TABLE cached_result AS
CALL my_function(GET('var1'), 'param2') LIKE template_table;

CACHE TABLE cached_result AS
CALL my_function(GET('var1'), 'param2') LIKE FUNCTION 'template_function';

CACHE TABLE cached_result AS
CALL my_function(t1) PARTITION BY t1 SIZE 100;

CACHE TABLE cached_result AS
CALL my_function(t1) LIKE t1 PARTITION BY t1 SIZE 100;
```

::: warning 注意
`ASYNC` 关键字在 `CACHE TABLE` 语法中会被解析，但运行时会抛出异常（`async function not support in cache`）。如需异步执行，请使用独立的 `CALL` 语句。
:::


### IF

条件执行语句。

**语法：**

```sql
IF [TIMEIN] (condition) THEN (statement) [ELSE (statement)]
```

**参数：**

| 参数 | 描述 |
|------|------|
| `TIMEIN` | 可选。指定为超时模式，条件返回超时时间（毫秒） |
| `condition` | 条件表达式。普通模式返回布尔值，超时模式返回数值（毫秒） |
| `statement` | 可执行语句：`CACHE TABLE`、`SELECT`/`INSERT`/`UPDATE`/`DELETE`、`ASSERT`、`CALL`、`SET` 或 `RETURN` |
| `ELSE` | 可选。普通模式下可选，超时模式下必需 |

**描述：**

IF 语句支持两种执行模式：

1. **普通模式**：评估条件表达式，如果返回 true 则执行 THEN 子句，否则执行 ELSE 子句（如果存在）
2. **超时模式**（TIMEIN）：条件表达式必须返回数值类型的超时时间（毫秒）
   - 如果超时时间 > 0，执行 THEN 子句并设置超时；如果超时则回退到 ELSE 子句
   - 如果超时时间 <= 0，立即执行 THEN 子句
   - 当超时时间 > 0 时，THEN 执行超时或抛出异常会丢弃其临时 `RETURN` 状态，再执行 ELSE，因而不会把失败分支的结果提交给函数

**注意：**
- 条件查询必须恰好返回一行一列。普通模式下该值必须是 BOOLEAN（NULL 按 false 处理）；TIMEIN 模式下必须是数值。
- THEN 和 ELSE 子句必须同为 CACHE 语句，或同为非 CACHE 语句，不允许混用
- 两分支均为 CACHE 语句时：必须写入相同的表名，且表结构必须兼容
- 两分支均为非 CACHE 语句时：两个分支的返回字段结构必须兼容
- IF 包含 `RETURN` 时，仅支持两种结构：THEN 返回且没有 ELSE；或者 THEN 和 ELSE 都返回。不能只在 ELSE 返回，也不能将返回分支与非返回 ELSE 混用
- 两个分支都返回时，返回模式必须兼容；函数中其他返回点也必须采用相同模式
- THEN 返回且没有 ELSE 时，如果条件为 false，函数继续执行 IF 后面的语句
- TIMEIN 模式必须提供 ELSE；两个分支必须同为 CACHE，或同为 RETURN
- 当前不支持在 THEN 或 ELSE 中直接嵌套另一个 IF
- 无 ELSE 子句且条件为 false 时：若 THEN 子句为 CACHE 语句，对应的缓存表会被注册为空表（若尚不存在），保证后续语句可正常引用

**示例：**

```sql
IF (SELECT COUNT(*) > 100 FROM source_table) THEN (
    CACHE TABLE result AS SELECT * FROM source_table
) ELSE (
    CACHE TABLE result AS SELECT * FROM backup_table
);

IF TIMEIN (SELECT timeout_ms FROM config_table) THEN (
    CACHE TABLE result AS CALL slow_function('param')
) ELSE (
    CACHE TABLE result AS SELECT * FROM default_table
);

-- 分支为任意可执行语句
IF (SELECT COUNT(*) > 0 FROM source_table) THEN (
    SELECT * FROM source_table
) ELSE (
    SELECT * FROM backup_table
);

IF (SELECT COUNT(*) > 0 FROM source_table) THEN (
    ASSERT SELECT COUNT(*) > 0 FROM source_table
);

-- SQL 函数中提前返回：无 ELSE 时，条件为 false 会继续执行
IF (SELECT COUNT(*) = 0 FROM source_table) THEN (
    RETURN SELECT CAST(NULL AS BIGINT) AS id WHERE FALSE
);
RETURN SELECT id FROM source_table;

-- 两个分支都返回，且返回模式一致
IF (SELECT use_primary FROM config_table) THEN (
    RETURN SELECT id, score FROM primary_result
) ELSE (
    RETURN SELECT id, score FROM fallback_result
);
-- 即使两个分支都会返回，仍需使用顶层 RETURN 结束函数定义
RETURN SELECT id, score FROM fallback_result;

-- TIMEIN 同样支持 RETURN；超时或异常时执行 ELSE
IF TIMEIN (SELECT 1000) THEN (
    RETURN SELECT id, score FROM slow_result
) ELSE (
    RETURN SELECT id, score FROM fallback_result
);
RETURN SELECT id, score FROM fallback_result;
```


### ASSERT

执行 SELECT 查询并断言结果为真。如果查询结果的任何字段不为 `true`，则抛出异常中止执行。

**语法：**

```sql
ASSERT select_statement
```

**参数：**

| 参数 | 描述 |
|------|------|
| `select_statement` | SELECT 查询语句，返回的字段必须全部为布尔类型 |

**描述：**

ASSERT 语句执行指定的 SELECT 查询，并对返回结果进行断言校验：
1. 查询返回的字段必须全部为布尔（BOOLEAN）类型，否则在编译期抛出异常
2. 查询必须至少返回一行数据，否则抛出异常
3. 每一行、每一列的值必须为 `true`，若存在 `false` 或 `null` 值则抛出异常

**注意：**
- ASSERT 算子不支持并发执行
- 断言失败时会抛出 `RuntimeException`，包含失败的具体行号和列号信息

**示例：**

```sql
ASSERT SELECT COUNT(*) > 0 FROM source_table;

ASSERT SELECT COUNT(*) > 100 FROM source_table WHERE status = 'active';

ASSERT SELECT COUNT(*) > 0, COUNT(*) >= 10 FROM source_table;
```


### FLUSH

强制失效系统中的所有缓存。

**语法：**

```sql
FLUSH
```

**描述：**

`FLUSH` 语句会立即失效 SQLRec 进程内的全部缓存，强制后续查询重新从元数据库加载最新数据。该命令会失效以下缓存：

- `CalciteSchemaFactory`：数据库列表与表结构（schema）缓存
- `JavaFunctionUtils`：Java 函数不存在性缓存
- `CompileManager`：已编译的 SQL 函数绑定（`SqlFunctionBindable`）与 API 缓存
- `ServiceManager`：服务配置缓存

**示例：**

```sql
FLUSH;
```

::: warning 注意
`FLUSH` 会失效所有缓存，可能导致短时间内元数据重新加载带来的开销。通常在元数据变更（例如外部修改了表结构、函数或服务定义）后手动执行，以确保后续查询看到最新状态。
:::


## 函数调用

### CALL

调用一个 SQL 函数。

**语法：**

```sql
CALL function_name([arg1, arg2, ...]) [LIKE {like_table | FUNCTION 'function_name'}] [PARTITION BY table_name SIZE partition_size] [ASYNC]
```

**参数：**

| 参数 | 描述 |
|------|------|
| `function_name` | 函数名称，可以是标识符或 `GET()` 表达式 |
| `arg1, arg2, ...` | 函数参数，可以是标识符、`GET()` 表达式或字符串字面量 |
| `like_table` | 可选。指定结果表的模板表 |
| `FUNCTION 'function_name'` | 可选。指定结果表的模式与某个函数的输出模式相同 |
| `PARTITION BY table_name SIZE partition_size` | 可选。按指定输入表进行分区并发执行，`table_name` 必须是函数的输入表之一，`partition_size` 为每个分区的最大行数 |
| `ASYNC` | 可选。异步执行 |

**示例：**

```sql
CALL my_function('param1', 'param2');

CALL my_function(GET('var1'), 'param2') LIKE template_table;

CALL my_function(GET('var1'), 'param2') LIKE FUNCTION 'template_function';

CALL my_function('param1') ASYNC;

CALL GET('fun1')(GET('id'), t1, '10') LIKE t1;

CALL my_function(t1) PARTITION BY t1 SIZE 100;

CALL my_function(t1) LIKE t1 PARTITION BY t1 SIZE 100 ASYNC;
```


### GET

获取运行时变量的值。

**语法：**

```sql
GET('variable_name')
```

**参数：**

| 参数 | 描述 |
|------|------|
| `variable_name` | 变量名称，必须是字符串字面量 |

**示例：**

```sql
GET('my_variable');

CALL my_function(GET('input_table'));
```
