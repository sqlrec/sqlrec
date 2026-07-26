-- save_rec_item: persists final recommendations to kafka and exposure log.
create or replace sql function save_rec_item;

define input table final_recall_item(
  `user_id` BIGINT,
  `movie_id` BIGINT,
  `item_name` STRING,
  `rec_reason` STRING,
  `req_time` BIGINT,
  `req_id` STRING
);

-- Write the full recommendation log to kafka for offline analysis.
insert into rec_log_kafka
select * from final_recall_item;

-- Track user exposure to dedup future recalls.
insert into user_exposure_item
select user_id, movie_id, req_time
from final_recall_item;

return;
