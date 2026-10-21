-- Architecture and training parameters follow benchmark/model_quality/config.py
-- (movielens100k); keep the MovieLens-1M data and feature schema used here.
-- rating is supplied by the training table and is never an input feature.
create model if not exists `rank_model`
(
 `user_id` BIGINT,
 `movie_id` BIGINT,
 `genres` ARRAY<STRING>,
 `gender` STRING,
 `age` INT,
 `occupation` INT,
 `zip_code` STRING,
 `age_years` FLOAT,
 `genre_count` FLOAT
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
'sparse_lr'='0.03',
'column.age_years.normalizer'='method=minmax,min=0,max=100',
'column.genre_count.normalizer'='method=minmax,min=0,max=18'
);

train model rank_model checkpoint='v1' on ml_sample
with (
'NAMESPACE'='${NAMESPACE}',
'pod_cpu_cores'='2',
'pod_memory'='4Gi'
);

-- train model rank_model checkpoint='v2' on ml_sample from 'v1'
-- with (
-- 'NAMESPACE'='${NAMESPACE}',
-- 'pod_cpu_cores'='2',
-- 'pod_memory'='4Gi'
-- );

export model rank_model checkpoint='v1' on ml_sample
with (
'NAMESPACE'='${NAMESPACE}',
'pod_cpu_cores'='2',
'pod_memory'='4Gi'
);

create service `rank-service` on model rank_model checkpoint='v1_export'
with (
'NAMESPACE'='${NAMESPACE}'
);

call sleep('3000');

CACHE TABLE t1 AS
SELECT
    1 AS user_id,
    100 AS movie_id,
    ARRAY['Action', 'Adventure', 'Sci-Fi'] AS genres,
    'M' AS gender,
    25 AS age,
    10 AS occupation,
    '10001' AS zip_code,
    CAST(25 AS FLOAT) AS age_years,
    CAST(3 AS FLOAT) AS genre_count;

call call_service('rank-service', t1);

create model if not exists `recall_model`
(
 `user_id` BIGINT,
 `movie_id` BIGINT,
 `genres` ARRAY<STRING>,
 `gender` STRING,
 `age` INT,
 `occupation` INT,
 `zip_code` STRING,
 `age_years` FLOAT,
 `genre_count` FLOAT
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
'user_features'='user_id,gender,age,occupation,zip_code,age_years',
'item_features'='movie_id,genres,genre_count',
'column.age_years.normalizer'='method=minmax,min=0,max=100',
'column.genre_count.normalizer'='method=minmax,min=0,max=18'
);

train model recall_model checkpoint='v1' on ml_recall_sample
with (
'NAMESPACE'='${NAMESPACE}',
'pod_cpu_cores'='2',
'pod_memory'='4Gi'
);

export model recall_model checkpoint='v1' on ml_recall_sample
with (
'NAMESPACE'='${NAMESPACE}',
'pod_cpu_cores'='2',
'pod_memory'='4Gi'
);

create service `recall-service-user` on model recall_model checkpoint='v1_export/user'
with (
'NAMESPACE'='${NAMESPACE}'
);

create service `recall-service-item` on model recall_model checkpoint='v1_export/item'
with (
'NAMESPACE'='${NAMESPACE}'
);

call sleep('3000');

CACHE TABLE tmp_user AS
SELECT
    1 AS user_id,
    'M' AS gender,
    25 AS age,
    10 AS occupation,
    '10001' AS zip_code,
    CAST(25 AS FLOAT) AS age_years;

call call_service('recall-service-user', tmp_user);

CACHE TABLE tmp_item AS
SELECT
    100 AS movie_id,
    ARRAY['Action', 'Adventure', 'Sci-Fi'] AS genres,
    CAST(3 AS FLOAT) AS genre_count;

call call_service('recall-service-item', tmp_item);

create model if not exists `rank_model_proxy`
(
 `user_id` BIGINT,
 `movie_id` BIGINT,
 `genres` ARRAY<STRING>,
 `gender` STRING,
 `age` INT,
 `occupation` INT,
 `zip_code` STRING,
 `age_years` FLOAT,
 `genre_count` FLOAT
)
with (
'model'='external',
'output_columns'='probs:FLOAT'
);

create service `rank-service-proxy` on model rank_model_proxy
with (
'url'='http://rank-service.${NAMESPACE}.svc.cluster.local:80/predict'
);

call call_service('rank-service-proxy', t1);
