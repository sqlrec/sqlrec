/*
 * rank_model: Wide & Deep ranking model (tzrec.wide_and_deep).
 * Inputs: user features + item features; label: rating.
 * Architecture and training parameters follow benchmark/model_quality/config.py
 * (movielens100k, wide_and_deep); retain the demo's MovieLens-1M features.
 */
create model if not exists `rank_model`
(
 `user_id` BIGINT,
 `movie_id` BIGINT,
 `genres` ARRAY<STRING>,
 `gender` STRING,
 `age` INT,
 `occupation` INT,
 `zip_code` STRING
)
with (
'model'='tzrec.wide_and_deep',
'label_columns'='rating',
'random_seed'='123',
'embedding_dim'='16',
'hidden_units'='64,32',
'batch_size'='256',
'num_workers'='1',
'num_epochs'='6',
'sparse_lr'='0.03'
);
