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
