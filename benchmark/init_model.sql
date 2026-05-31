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
'label_columns'='rating'
);

train model rank_model checkpoint='v1' on ml_sample
with (
'NAMESPACE'='sqlrec',
'pod_memory'='8Gi',
'batch_size'='1024'
);

-- train model rank_model checkpoint='v2' on ml_sample from 'v1'
-- with (
-- 'NAMESPACE'='sqlrec',
-- 'batch_size'='1024'
-- );

export model rank_model checkpoint='v1' on ml_sample
with (
'NAMESPACE'='sqlrec',
'pod_memory'='8Gi'
);

create service rank_service on model rank_model checkpoint='v1_export'
with (
'NAMESPACE'='sqlrec'
);

CACHE TABLE t1 AS
SELECT
    1 AS user_id,
    100 AS movie_id,
    ARRAY['Action', 'Adventure', 'Sci-Fi'] AS genres,
    'M' AS gender,
    25 AS age,
    10 AS occupation,
    '10001' AS zip_code;

call call_service('rank_service', t1);

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
'item_features'='movie_id,genres'
);

train model recall_model checkpoint='v1' on ml_recall_sample
with (
'NAMESPACE'='sqlrec',
'pod_memory'='8Gi',
'batch_size'='1024'
);

export model recall_model checkpoint='v1' on ml_recall_sample
with (
'NAMESPACE'='sqlrec',
'pod_memory'='8Gi'
);

create service recall_service_user on model recall_model checkpoint='v1_export/user'
with (
'NAMESPACE'='sqlrec'
);

create service recall_service_item on model recall_model checkpoint='v1_export/item'
with (
'NAMESPACE'='sqlrec'
);

CACHE TABLE tmp_user AS
SELECT
    1 AS user_id,
    'M' AS gender,
    25 AS age,
    10 AS occupation,
    '10001' AS zip_code;

call call_service('recall_service_user', tmp_user);

CACHE TABLE tmp_item AS
SELECT
    100 AS movie_id,
    ARRAY['Action', 'Adventure', 'Sci-Fi'] AS genres;

call call_service('recall_service_item', tmp_item);

create model if not exists `rank_model_proxy`
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
'model'='external',
'output_columns'='probs:FLOAT'
);

create service rank_service_proxy on model rank_model_proxy
with (
'url'='http://rank-service.sqlrec.svc.cluster.local:80/predict'
);

call call_service('rank_service_proxy', t1);