create or replace sql function save_rec_item;

define input table final_recall_item(
  `user_id` BIGINT,
  `movie_id` BIGINT,
  `item_name` STRING,
  `rec_reason` STRING,
  `req_time` BIGINT,
  `req_id` STRING
);

insert into rec_log_kafka
select * from final_recall_item;

insert into user_exposure_item
select user_id, movie_id, req_time
from final_recall_item;

return;
