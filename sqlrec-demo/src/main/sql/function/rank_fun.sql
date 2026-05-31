create or replace sql function rank_fun;

define input table user_info(
  user_id bigint,
  gender string,
  age int,
  occupation int,
  zip_code string
);

define input table recall_item(
  movie_id bigint,
  rec_reason string
);

cache table recall_item_with_info as
select
    recall_item.movie_id,
    recall_item.rec_reason,
    item_table.genres,
    item_table.title
from
    recall_item join item_table on recall_item.movie_id = item_table.movie_id;

cache table rank_feature as
select
    user_info.user_id,
    recall_item_with_info.movie_id,
    recall_item_with_info.genres,
    user_info.gender,
    user_info.age,
    user_info.occupation,
    user_info.zip_code
from
    recall_item_with_info join user_info on 1=1;

cache table ranked_item as call call_service('rank_service', rank_feature);

cache table rec_item as
select 
    ranked_item.movie_id, 
    recall_item_with_info.genres, 
    recall_item_with_info.title, 
    recall_item_with_info.rec_reason
from ranked_item
join recall_item_with_info on ranked_item.movie_id = recall_item_with_info.movie_id
order by ranked_item.probs desc;

return rec_item;
