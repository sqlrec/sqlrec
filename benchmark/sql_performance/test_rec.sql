cache table t1 as select cast(1 as bigint) as user_id;
call main_rec(t1);
set rank_fun=rank_fun;
set use_recall_service=true;
call main_rec(t1);