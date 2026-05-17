-- recall function
create or replace sql function recall_fun;

define input table user_info(
  user_id bigint,
  gender string,
  age int,
  occupation int,
  zip_code string
);

cache table user_embedding as call call_service('recall_service_user', user_info);

cache table vector_recall as
select item_embedding.id as movie_id
from
user_embedding join item_embedding on 1=1
order by ip(user_embedding.user_tower_emb, item_embedding.embedding)
limit 300;

cache table vector_recall as
select movie_id, 'vector_recall' as rec_reason
from vector_recall;

cache table exposured_item as
select movie_id
from
user_info join user_exposure_item on user_exposure_item.user_id = user_info.user_id
where bhv_time > cast(CURRENT_TIMESTAMP as BIGINT) - 3600000
group by movie_id;

cache table cur_recent_click_item as
select movie_id
from
user_info join user_recent_click_item on user_recent_click_item.user_id = user_info.user_id
group by movie_id
order by MAX(bhv_time) desc
limit 10;

cache table i2i_recall as
select movie_id2 as movie_id, 'itemcf_recall' as rec_reason
from
cur_recent_click_item join itemcf_i2i on movie_id1 = cur_recent_click_item.movie_id
limit 300;

cache table global_hot_recall as
select movie_id, 'global_hot_recall' as rec_reason
from
global_hot_item where invert_key = 'global'
limit 300;

cache table cur_user_interest_genre as
select genre
from
user_info join user_interest_genre on user_interest_genre.user_id = user_info.user_id
group by genre
order by MAX(score) desc
limit 10;

cache table genre_recall as
select movie_id, 'user_genre_interest_recall' as rec_reason
from
cur_user_interest_genre join genre_hot_item
on genre_hot_item.genre = cur_user_interest_genre.genre
limit 300;

cache table dedup_i2i_recall as call dedup(i2i_recall, exposured_item, 'movie_id', 'movie_id');
cache table dedup_global_hot_recall as call dedup(global_hot_recall, exposured_item, 'movie_id', 'movie_id');
cache table dedup_genre_recall as call dedup(genre_recall, exposured_item, 'movie_id', 'movie_id');
cache table dedup_vector_recall as call dedup(vector_recall, exposured_item, 'movie_id', 'movie_id');

cache table all_recall_item as
select * from dedup_i2i_recall
union all
select * from dedup_global_hot_recall
union all
select * from dedup_genre_recall
union all
select * from dedup_vector_recall;

cache table truncate_recall_item as
select movie_id, rec_reason
from all_recall_item
limit 300;

cache table dedup_recall_item as
select movie_id, LISTAGG(distinct rec_reason) as rec_reason
from truncate_recall_item
group by movie_id;

return dedup_recall_item;



-- rank function
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



-- diversify function
create or replace sql function diversify_fun;

define input table rec_item(
  movie_id bigint,
  genres array<string>,
  title string,
  rec_reason string
);

cache table diversify_rec_item as call window_diversify(rec_item, 'genres', '3', '1', '10');

return diversify_rec_item;



-- save final_recall_item to kafka
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



-- main_rec function
create or replace sql function main_rec;

define input table user_info(
  user_id bigint
);

cache table full_user_info as
select user_info.user_id, gender, age, occupation, zip_code
from user_info join user_table on user_info.user_id = user_table.user_id;

cache table recall_item as
call get_or_default('recall_fun', 'recall_fun')(full_user_info)
like function 'recall_fun';

cache table rec_item as
call get_or_default('rank_fun', 'rank_fun')(full_user_info, recall_item)
like function 'rank_fun';

cache table diversify_rec_item as
call get_or_default('diversify_fun', 'diversify_fun')(rec_item)
like function 'diversify_fun';

cache table request_meta as
select
  user_info.user_id,
  cast(CURRENT_TIMESTAMP as BIGINT) as req_time,
  uuid() as req_id
from user_info;

cache table final_rec_item as
select
    request_meta.user_id as user_id,
    movie_id,
    diversify_rec_item.title as item_name,
    rec_reason,
    request_meta.req_time as req_time,
    request_meta.req_id as req_id
from
    request_meta join diversify_rec_item on 1=1;

call save_rec_item(final_rec_item) async;

return final_rec_item;



-- main_rec api
create or replace api main_rec with main_rec;
