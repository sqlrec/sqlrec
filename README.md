# SqlRec
一个支持完全使用SQL进行开发的全流程推荐引擎，可以用来快速搭建生产可用的推荐系统。

sqlRec有下述几个模块：
- 大数据底座
- sql执行引擎，包括API接口
- 模型训练工具

## Sql语法
SqlRec使用的sql语法基于flinksql扩展而来，同时元数据复用HMS，能很方便和已有的大数据系统打通。
当SqlRec服务器收到一个sql执行请求时，会判断是否需要在服务器本地执行，否则转发到Flink sql gateway作为flinksql进行执行。
当满足以下几类条件时，sql会基于calcite在服务器本地执行：
- 仅读写kv、MQ、向量表
- 使用扩展语句

### 扩展语句
扩展的语句包括以下几类：
- cache语句，将select查询语句缓存为一个临时表
  ```
  cache table t2 as select * from t1 where id=1;
  ```
- create sql function, 创建sql函数，语法如下所示：
  ```
  create sql function f1;
  define input table t1 (c1 string, c2 string);
  cache table t2 as select * from t1 where id=1
  return t2;
  ```
- sql函数函数调用，仅支持在cache语句中使用
  ```
  cache table t3 as call_sql_func('f1', t2);
  ```
- API接口绑定，将sql函数绑定到某个http接口
  ```
  # expose post api on /api/v1/rec1
  create api rec1 with t1;
  ```
- 模型定义、训练、推理相关的语法

### 建表语句
建表完全通过flinkSQL执行，所以使用flinkSQL的建表语法即可。

SqlRec的表类型一般有4类，如下所示：
- 离线表，目前支持hive表，主要用于离线特征计算
- MQ表，目前支持卡夫卡，主要用于实时数据存储
- KV表，目前支持redis，主要用于保持正排、倒排数据
- 向量化表，目前支持Milvus，主要用于向量化召回

### 模型相关语法
模型定义：

模型实例化：

模型训练：

模型保存：

模型上线：

模型调用：

模型下线：


## 最佳实践


## 系统架构


