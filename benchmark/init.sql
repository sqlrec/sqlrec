SET table.sql-dialect = default;

CREATE TABLE IF NOT EXISTS `user_table` (
  `id` BIGINT,
  `name` STRING,
  `country` STRING,
  `age` INT,
  `os` STRING,
  `network` STRING,
  PRIMARY KEY (id)  NOT ENFORCED
) WITH (
  'connector' = 'redis',
  'url' = 'redis://${NODE_IP}:${REDIS_PORT}/0'
);

CREATE TABLE IF NOT EXISTS `item_table` (
  `id` BIGINT,
  `name` STRING,
  `price` FLOAT,
  `brand` STRING,
  `category1` STRING,
  `category2` STRING,
  `category3` STRING,
  `category4` STRING,
  PRIMARY KEY (id)  NOT ENFORCED
) WITH (
  'connector' = 'redis',
  'url' = 'redis://${NODE_IP}:${REDIS_PORT}/0'
);

CREATE TABLE IF NOT EXISTS `global_hot_item` (
  `invert_key` STRING,
  `id` BIGINT,
  `score` FLOAT,
  PRIMARY KEY (invert_key)  NOT ENFORCED
) WITH (
  'connector' = 'redis',
  'data-structure' = 'list',
  'url' = 'redis://${NODE_IP}:${REDIS_PORT}/0'
);

CREATE TABLE IF NOT EXISTS `user_interest_category1` (
  `user_id` BIGINT,
  `category1` STRING,
  `score` FLOAT,
  PRIMARY KEY (user_id)  NOT ENFORCED
) WITH (
  'connector' = 'redis',
  'data-structure' = 'list',
  'url' = 'redis://${NODE_IP}:${REDIS_PORT}/0'
);

CREATE TABLE IF NOT EXISTS `category1_hot_item` (
  `category1` STRING,
  `item_id` BIGINT,
  `score` FLOAT,
  PRIMARY KEY (category1)  NOT ENFORCED
) WITH (
  'connector' = 'redis',
  'data-structure' = 'list',
  'url' = 'redis://${NODE_IP}:${REDIS_PORT}/0'
);

CREATE TABLE IF NOT EXISTS `user_recent_click_item` (
  `user_id` BIGINT,
  `item_id` BIGINT,
  `bhv_time` BIGINT,
  PRIMARY KEY (user_id)  NOT ENFORCED
) WITH (
  'connector' = 'redis',
  'data-structure' = 'list',
  'url' = 'redis://${NODE_IP}:${REDIS_PORT}/0'
);

CREATE TABLE IF NOT EXISTS `user_exposure_item` (
  `user_id` BIGINT,
  `item_id` BIGINT,
  `bhv_time` BIGINT,
  PRIMARY KEY (user_id)  NOT ENFORCED
) WITH (
  'connector' = 'redis',
  'data-structure' = 'list',
  'url' = 'redis://${NODE_IP}:${REDIS_PORT}/0',
  'cache-ttl' = '0'
);

CREATE TABLE IF NOT EXISTS `itemcf_i2i` (
  `item_id1` BIGINT,
  `item_id2` BIGINT,
  `score` FLOAT,
  PRIMARY KEY (item_id1)  NOT ENFORCED
) WITH (
  'connector' = 'redis',
  'data-structure' = 'list',
  'url' = 'redis://${NODE_IP}:${REDIS_PORT}/0'
);

CREATE TABLE IF NOT EXISTS `item_embedding` (
  `id` BIGINT,
  `embedding` ARRAY<FLOAT>,
  `name` STRING,
  PRIMARY KEY (id)  NOT ENFORCED
) WITH (
  'connector' = 'milvus',
  'url' = 'http://${NODE_IP}:${MILVUS_PORT}',
  'token' = 'root:Milvus',
  'database' = 'default',
  'collection' = 'item_embedding'
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
  'properties.bootstrap.servers' = '${NODE_IP}:${KAFKA_PORT}',
  'format' = 'json'
);


-- save final_recall_item to kafka
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


-- main_rec function
create or replace sql function main_rec;

define input table user_info(id bigint);

cache table recall_item_schema as select
 cast(0 as BIGINT) as item_id,
 cast('' as varchar) as rec_reason;

cache table recall_item as call get('recall_fun')(user_info) like recall_item_schema;

cache table rec_item as
select item_id, category1, name, rec_reason
from
recall_item join item_table on id = item_id;

cache table diversify_rec_item as call window_diversify(rec_item, 'category1', '3', '1', '10');

cache table request_meta as select
user_info.id as user_id,
CURRENT_TIMESTAMP as req_time,
uuid() as req_id
from user_info;

cache table final_rec_item as
select
request_meta.user_id as user_id,
item_id,
diversify_rec_item.name as item_name,
rec_reason,
request_meta.req_time as req_time,
request_meta.req_id as req_id
from
request_meta join diversify_rec_item on 1=1;

call save_rec_item(final_rec_item) async;

return final_rec_item;


-- recall function
create or replace sql function recall_fun;

define input table user_info(id bigint);

cache table exposured_item as
select item_id
from
user_info join user_exposure_item on user_id = user_info.id;

cache table cur_recent_click_item as
select item_id
from
user_info join user_recent_click_item on user_id = user_info.id
limit 10;

cache table i2i_recall as
select item_id2 as item_id, 'itemcf_recall' as rec_reason
from
cur_recent_click_item join itemcf_i2i on item_id1 = cur_recent_click_item.item_id
limit 300;

cache table global_hot_recall as
select id as item_id, 'global_hot_recall' as rec_reason
from
global_hot_item where invert_key = 'global'
limit 300;

cache table cur_user_interest_category1 as
select category1
from
user_info join user_interest_category1 on user_id = user_info.id
limit 10;

cache table category1_recall as
select item_id as item_id, 'user_category1_interest_recall' as rec_reason
from
cur_user_interest_category1 join category1_hot_item
on category1_hot_item.category1 = cur_user_interest_category1.category1
limit 300;

cache table dedup_i2i_recall as call dedup(i2i_recall, exposured_item, 'item_id', 'item_id');
cache table dedup_global_hot_recall as call dedup(global_hot_recall, exposured_item, 'item_id', 'item_id');
cache table dedup_category1_recall as call dedup(category1_recall, exposured_item, 'item_id', 'item_id');

cache table all_recall_item as
select * from dedup_i2i_recall
union all
select * from dedup_global_hot_recall
union all
select * from dedup_category1_recall;

cache table truncate_recall_item as
select item_id, rec_reason
from all_recall_item
limit 300;

cache table final_recall_item as
select item_id, LISTAGG(distinct rec_reason) as rec_reason
from truncate_recall_item
group by item_id;

return final_recall_item;


-- main_rec api
create or replace api main_rec with main_rec;
