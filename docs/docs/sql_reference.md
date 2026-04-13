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
```

**参数：**

| 参数 | 描述 |
|------|------|
| `table_name` | 输入表名称 |
| `column_name` | 列名 |
| `column_type` | 列数据类型 |

**示例：**

```sql
DEFINE INPUT TABLE input_data (
    id INT,
    name VARCHAR(100),
    score DOUBLE,
    created_at TIMESTAMP
);
```


### RETURN

从函数中返回结果。

**语法：**

```sql
RETURN [table_name]
```

**参数：**

| 参数 | 描述 |
|------|------|
| `table_name` | 可选。要返回的表名 |

**示例：**

```sql
RETURN;

RETURN result_table;
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
    {CALL function_name([arg1, arg2, ...]) [LIKE {like_table | FUNCTION 'function_name'}] [ASYNC]
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
| `ASYNC` | 可选。异步执行 |
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
CALL my_function('param1') ASYNC;
```


## 函数调用

### CALL

调用一个 SQL 函数。

**语法：**

```sql
CALL function_name([arg1, arg2, ...]) [LIKE {like_table | FUNCTION 'function_name'}] [ASYNC]
```

**参数：**

| 参数 | 描述 |
|------|------|
| `function_name` | 函数名称，可以是标识符或 `GET()` 表达式 |
| `arg1, arg2, ...` | 函数参数，可以是标识符、`GET()` 表达式或字符串字面量 |
| `like_table` | 可选。指定结果表的模板表 |
| `FUNCTION 'function_name'` | 可选。指定结果表的模式与某个函数的输出模式相同 |
| `ASYNC` | 可选。异步执行 |

**示例：**

```sql
CALL my_function('param1', 'param2');

CALL my_function(GET('var1'), 'param2') LIKE template_table;

CALL my_function(GET('var1'), 'param2') LIKE FUNCTION 'template_function';

CALL my_function('param1') ASYNC;

CALL GET('fun1')(GET('id'), t1, '10') LIKE t1;
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
