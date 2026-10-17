/*
 * recall_model: DSSM two-tower recall model (tzrec.dssm).
 * Trained jointly on user and item features; serves two towers via
 * recall_service_user and recall_service_item.
 * Architecture and training parameters follow benchmark/model_quality/config.py
 * (movielens100k, dssm); retain the demo's MovieLens-1M features.
 */
create model if not exists `recall_model`
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
'model'='tzrec.dssm',
'label_columns'='rating',
'random_seed'='123',
'embedding_dim'='16',
'batch_size'='256',
'num_workers'='1',
'num_epochs'='3',
'sparse_lr'='0.01',
'user_hidden_units'='64,32',
'item_hidden_units'='64,32',
'output_dim'='32',
'negative_sampling'='pool',
'negative_sample_count'='256',
'negative_pool_size'='4096',
'item_id_column'='movie_id',
'user_features'='user_id,gender,age,occupation,zip_code',
'item_features'='movie_id,genres'
);
