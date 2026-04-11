# 快速开始
这里通过个简单Demo介绍SQLRec的使用，部署可以参考[服务部署](/docs/deployment)。

## 连接SQLRec服务

### 使用beeline
SQLRec实现了hive thrift接口，你可以使用beeline连接SQLRec服务，然后像使用hive一样使用它。
```bash
bash ./bin/beeline.sh
```

### 使用python
可以在Jupyter Notebook中使用python连接SQLRec服务，并使用python工具分析推荐数据，参考下述代码：
- 使用deploy目录的脚本部署Jupyter
```bash
cd deploy
bash ./jupyter/deploy.sh
# wait pod ready
```
- 浏览器打开Jupyter Notebook，比如`http://127.0.0.1:30280`，使用env.sh中的账号密码登录
- 新建python3 notebook
- 安装依赖
```bash
%pip install pandas
%pip install pyhive
%pip install sasl
%pip install thrift
%pip install thrift-sasl
```
- 连接SQLRec服务，运行sql语句
```python
from pyhive import hive
import pandas as pd

conn = hive.Connection(host='192.168.49.2',port=30300,auth='NOSASL')
pd.read_sql("select * from `user_interest_category1` where `user_id` = 1000001", conn)
```

## SQL开发
使用beeline连接SQLRec服务，参考下述流程开发推荐需要的数据表、SQL函数、API接口等：

### 初始化数据表
参考下述SQL，注意可以通过`kubectl get node -o wide`命令获取minikube节点的ip地址，你可能需要替换下述代码的ip地址
```sql
SET table.sql-dialect = default;

CREATE TABLE IF NOT EXISTS `user_interest_category1` (
  `user_id` BIGINT,
  `category1` STRING,
  `score` FLOAT,
  PRIMARY KEY (user_id)  NOT ENFORCED
) WITH (
  'connector' = 'redis',
  'data-structure' = 'list',
  'url' = 'redis://192.168.49.2:32379/0'
);

CREATE TABLE IF NOT EXISTS `category1_hot_item` (
  `category1` STRING,
  `item_id` BIGINT,
  `score` FLOAT,
  PRIMARY KEY (category1)  NOT ENFORCED
) WITH (
  'connector' = 'redis',
  'data-structure' = 'list',
  'url' = 'redis://192.168.49.2:32379/0'
);

CREATE TABLE IF NOT EXISTS `user_exposure_item` (
  `user_id` BIGINT,
  `item_id` BIGINT,
  `bhv_time` BIGINT,
  PRIMARY KEY (user_id)  NOT ENFORCED
) WITH (
  'connector' = 'redis',
  'data-structure' = 'list',
  'url' = 'redis://192.168.49.2:32379/0',
  'cache-ttl' = '0'
);

CREATE TABLE IF NOT EXISTS `rec_log_kafka` (
  `user_id` BIGINT,
  `item_id` BIGINT,
  `item_name` STRING,
  `rec_reason` STRING,
  `req_time` BIGINT,
  `req_id` STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'rec_log',
  'properties.bootstrap.servers' = '192.168.49.2:32092',
  'format' = 'json'
);
```

### 写入测试数据
```sql
INSERT INTO `user_interest_category1` VALUES
(1000001, 'pc', 100),
(1000001, 'phone', 100);

INSERT INTO `category1_hot_item` VALUES
('pc', 1000001, 100),
('pc', 1000002, 100),
('pc', 1000003, 100),
('pc', 1000004, 100),
('pc', 1000005, 100),
('phone', 1000011, 100),
('phone', 1000012, 100),
('phone', 1000013, 100),
('phone', 1000014, 100),
('phone', 1000015, 100);

select * from `user_interest_category1` where `user_id` = 1000001;

select * from `category1_hot_item` where `category1` = 'pc';
```

### 开发sql函数
```sql
-- define function save rec data to kafka and redis
create or replace sql function save_rec_item;

define input table final_recall_item(
  `user_id` BIGINT,
  `item_id` BIGINT,
  `item_name` STRING,
  `rec_reason` STRING,
  `req_time` BIGINT,
  `req_id` STRING
);

insert into rec_log_kafka
select * from final_recall_item;

insert into user_exposure_item
select user_id, item_id, req_time from final_recall_item;

return;



-- define function test rec
create or replace sql function test_rec;

-- define input param
define input table user_info(id bigint);

-- query exposed item for deduplication
cache table exposured_item as
select item_id
from
user_info join user_exposure_item on user_id = user_info.id;

-- query user interest category1
cache table cur_user_interest_category1 as
select category1
from
user_info join user_interest_category1 on user_id = user_info.id
limit 10;

-- query category1 hot item
cache table category1_recall as
select item_id as item_id, 'user_category1_interest_recall:' || cur_user_interest_category1.category1 as rec_reason
from
cur_user_interest_category1 join category1_hot_item
on category1_hot_item.category1 = cur_user_interest_category1.category1
limit 300;

-- dedup category1 recall
cache table dedup_category1_recall as call dedup(category1_recall, exposured_item, 'item_id', 'item_id');

-- truncate to rec item num
cache table final_recall_item as
select item_id, rec_reason
from dedup_category1_recall
limit 2;

-- gen rec meta data
cache table request_meta as select
user_info.id as user_id,
cast(CURRENT_TIMESTAMP as BIGINT) as req_time,
uuid() as req_id
from user_info;

-- gen final rec data
cache table final_rec_data as
select
request_meta.user_id as user_id,
item_id,
cast('XXX' as VARCHAR) as item_name,
rec_reason,
request_meta.req_time as req_time,
request_meta.req_id as req_id
from
request_meta join final_recall_item on 1=1;

-- save rec data to kafka and redis
call save_rec_item(final_rec_data) async;

return final_rec_data;
```
上面SQL定义了推荐函数test_rec，可以发现SQL函数定义语法是：
- `create or replace sql function`加函数名开头
- `define input table`定义输入参数，可以为空或者定义多个
- `cache table`缓存中间计算结果，可以缓存SELECT语句、SQL函数调用的执行结果
- `call`调用其他函数, 可以通过async关键字异步调用
- `return`返回计算结果，可以为空


可以直接在beeline命令行测试函数，如下所示
```sql
0: jdbc:hive2://192.168.49.2:30300/default> cache table t1 as select cast(1000001 as bigint) as id;
+-------------+--------+
| table_name  | count  |
+-------------+--------+
| t1          | 1      |
+-------------+--------+
1 row selected (0.006 seconds)
0: jdbc:hive2://192.168.49.2:30300/default> desc t1;
+-------+---------+
| name  |  type   |
+-------+---------+
| id    | BIGINT  |
+-------+---------+
1 row selected (0.002 seconds)
0: jdbc:hive2://192.168.49.2:30300/default> call test_rec(t1);
+----------+----------+------------+---------------------------------------+----------------+---------------------------------------+
| user_id  | item_id  | item_name  |              rec_reason               |    req_time    |                req_id                 |
+----------+----------+------------+---------------------------------------+----------------+---------------------------------------+
| 1000001  | 1000015  | XXX        | user_category1_interest_recall:phone  | 1775366030516  | ee073e63-b74a-4c7e-8fea-60459729099c  |
| 1000001  | 1000005  | XXX        | user_category1_interest_recall:pc     | 1775366030516  | ee073e63-b74a-4c7e-8fea-60459729099c  |
+----------+----------+------------+---------------------------------------+----------------+---------------------------------------+
2 rows selected (0.006 seconds)
0: jdbc:hive2://192.168.49.2:30300/default> call test_rec(t1);
+----------+----------+------------+---------------------------------------+----------------+---------------------------------------+
| user_id  | item_id  | item_name  |              rec_reason               |    req_time    |                req_id                 |
+----------+----------+------------+---------------------------------------+----------------+---------------------------------------+
| 1000001  | 1000014  | XXX        | user_category1_interest_recall:phone  | 1775366045908  | 37116c4c-9e7e-4dcc-9913-14f9628a8467  |
| 1000001  | 1000004  | XXX        | user_category1_interest_recall:pc     | 1775366045908  | 37116c4c-9e7e-4dcc-9913-14f9628a8467  |
+----------+----------+------------+---------------------------------------+----------------+---------------------------------------+
2 rows selected (0.003 seconds)
```
可以发现，召回、推荐理由、去重都已经生效。

### 创建API接口
参考下述SQL将SQL函数暴露为API接口：
```sql
create or replace api test_rec with test_rec;
```

## 推荐测试
使用下述命令进行推荐测试：
```bash
yi@debian12:~$ curl -X POST http://192.168.49.2:30301/api/v1/test_rec \
-H "Content-Type: application/json" \
-d '{"inputs":{"user_info":[{"id": 1000001}]}}'
{"data":[{"user_id":1000001,"item_id":1000013,"item_name":"XXX","rec_reason":"user_category1_interest_recall:phone","req_time":1775367428357,"req_id":"f014bd2d-41f8-4de5-93e0-3507cdae2542"},{"user_id":1000001,"item_id":1000003,"item_name":"XXX","rec_reason":"user_category1_interest_recall:pc","req_time":1775367428357,"req_id":"f014bd2d-41f8-4de5-93e0-3507cdae2542"}]}
`````