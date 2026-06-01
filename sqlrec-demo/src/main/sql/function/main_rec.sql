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
call get_or_default('rank_fun', 'rank_fun_simple')(full_user_info, recall_item)
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
