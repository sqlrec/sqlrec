create model if not exists `recall_model_item`
(
 `movie_id` BIGINT,
 `genres` ARRAY<STRING>
)
with (
'model'='external',
'output_columns'='item_tower_emb:ARRAY<FLOAT>'
);
