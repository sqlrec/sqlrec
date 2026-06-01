create or replace sql function rank_fun_simple;

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

cache table rec_item as
select
    recall_item.movie_id,
    item_table.genres,
    item_table.title,
    recall_item.rec_reason
from
    recall_item join item_table on recall_item.movie_id = item_table.movie_id;

return rec_item;
