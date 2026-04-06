# Quick Start

This section introduces SQLRec usage through a simple demo. For deployment, refer to [Service Deployment](/en/docs/deployment).

## Connecting to SQLRec Service

### Using beeline

SQLRec implements the Hive Thrift interface. You can use beeline to connect to the SQLRec service and use it like Hive.

```bash
bash ./bin/beeline.sh
```

### Using Python

You can connect to the SQLRec service in Jupyter Notebook using Python and use Python tools to analyze recommendation data. Refer to the following code:

- Deploy Jupyter using scripts in the deploy directory

```bash
cd deploy
bash ./jupyter/deploy.sh
# wait pod ready
```

- Open Jupyter Notebook in your browser, e.g., `http://127.0.0.1:30280`, and log in using the credentials from env.sh
- Create a new Python3 notebook
- Install dependencies

```bash
!pip install pandas --user
!pip install pyhive --user
!pip install sasl --user
!pip install thrift --user
!pip install thrift-sasl --user
```

- Connect to SQLRec service and run SQL statements

```python
from pyhive import hive
import pandas as pd

conn = hive.Connection(host='192.168.49.2',port=30300,auth='NOSASL')
pd.read_sql("select * from `user_interest_category1` where `user_id` = 1000001", conn)
```

## SQL Development

Connect to the SQLRec service using beeline and develop the data tables, SQL functions, and API interfaces needed for recommendations following the process below:

### Initialize Data Tables

Refer to the following SQL. Note that you can get the minikube node IP address using the `kubectl get node -o wide` command. You may need to replace the IP address in the code below.

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

### Write Test Data

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

### Develop SQL Functions

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

The SQL above defines the recommendation function `test_rec`. The SQL function definition syntax is:
- Start with `create or replace sql function` followed by the function name
- `define input table` defines input parameters, can be empty or define multiple
- `cache table` caches intermediate calculation results, can cache SELECT statement and SQL function call execution results
- `call` calls other functions, can use the `async` keyword for asynchronous calls
- `return` returns calculation results, can be empty

You can test the function directly in the beeline command line as follows:

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

As you can see, recall, recommendation reasons, and deduplication are all working.

### Create API Interface

Refer to the following SQL to expose SQL functions as API interfaces:

```sql
create or replace api test_rec with test_rec;
```

## Recommendation Testing

Test recommendations using the following command:

```bash
yi@debian12:~$ curl -X POST http://192.168.49.2:30301/api/v1/test_rec \
-H "Content-Type: application/json" \
-d '{"inputs":{"user_info":[{"id": 1000001}]}}'
{"data":[{"user_id":1000001,"item_id":1000013,"item_name":"XXX","rec_reason":"user_category1_interest_recall:phone","req_time":1775367428357,"req_id":"f014bd2d-41f8-4de5-93e0-3507cdae2542"},{"user_id":1000001,"item_id":1000003,"item_name":"XXX","rec_reason":"user_category1_interest_recall:pc","req_time":1775367428357,"req_id":"f014bd2d-41f8-4de5-93e0-3507cdae2542"}]}
```
