-- rank_fun_simple: a no-op ranker used as a fallback when the rank service is unavailable.
-- It simply joins recall items with item metadata without scoring.
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

-- Straight join: recall_item <- item_table (no model scoring).
cache table rec_item as
select
    recall_item.movie_id,
    item_table.genres,
    item_table.title,
    recall_item.rec_reason
from
    recall_item join item_table on recall_item.movie_id = item_table.movie_id;

return rec_item;
