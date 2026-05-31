create model if not exists `recall_model_user`
(
 `user_id` BIGINT,
 `gender` STRING,
 `age` INT,
 `occupation` INT,
 `zip_code` STRING
)
with (
'model'='external',
'output_columns'='user_tower_emb:ARRAY<FLOAT>'
);
