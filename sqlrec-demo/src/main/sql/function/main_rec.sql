-- main_rec: top-level recommendation pipeline function.
-- It chains recall -> rank -> diversify, then persists the result.
create or replace sql function main_rec;

-- Input: only the user id is required at the API boundary.
define input table user_info(
  user_id bigint
);

-- Enrich user_info with demographic fields from user_table.
cache table full_user_info as
select user_info.user_id, gender, age, occupation, zip_code
from user_info join user_table on user_info.user_id = user_table.user_id;

-- Recall stage; note the fallback via get_or_default().
cache table recall_item as
call get_or_default('recall_fun', 'recall_fun')(full_user_info)
like function 'recall_fun';

-- Ranking stage.
cache table rec_item as
call get_or_default('rank_fun', 'rank_fun')(full_user_info, recall_item)
like function 'rank_fun';

-- Diversification stage.
cache table diversify_rec_item as
call get_or_default('diversify_fun', 'diversify_fun')(rec_item)
like function 'diversify_fun';

-- Per-request metadata used for tagging the final result rows.
cache table request_meta as
select
  user_info.user_id,
  cast(CURRENT_TIMESTAMP as BIGINT) as req_time,
  uuid() as req_id
from user_info;

-- Final join of metadata + diversified items.
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

-- Async persistence: save_rec_item is fire-and-forget.
call save_rec_item(final_rec_item) async;

return final_rec_item;
