-- A self-contained recommendation function for the Docker quick start.
create or replace sql function test_rec;

define input table user_info(id bigint);

-- Query previously exposed items for deduplication.
cache table exposured_item as
select item_id
from user_info join exposure_item on user_id = user_info.id;

-- Find the user's preferred categories.
cache table cur_user_interest_category1 as
select category1
from user_info join user_interest_category1 on user_id = user_info.id
limit 10;

-- Recall hot items from each preferred category.
cache table category1_recall as
select
  item_id,
  'user_category1_interest_recall:' || cur_user_interest_category1.category1 as rec_reason
from cur_user_interest_category1 join category1_hot_item
on category1_hot_item.category1 = cur_user_interest_category1.category1
limit 300;

cache table dedup_category1_recall as
call dedup(category1_recall, exposured_item, 'item_id', 'item_id');

cache table final_recall_item as
select item_id, rec_reason
from dedup_category1_recall
limit 2;

cache table request_meta as
select
  user_info.id as user_id,
  cast(CURRENT_TIMESTAMP as BIGINT) as req_time,
  uuid() as req_id
from user_info;

cache table final_rec_data as
select
  request_meta.user_id as user_id,
  item_id,
  cast('XXX' as VARCHAR) as item_name,
  rec_reason,
  request_meta.req_time as req_time,
  request_meta.req_id as req_id
from request_meta join final_recall_item on 1=1;

-- Filesystem connector writes remain in memory and need no external service.
insert into exposure_item
select user_id, item_id, req_time
from final_rec_data;

return final_rec_data;
