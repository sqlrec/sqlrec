create model `test_model` if not exists
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

train model test_model checkpoint='test' on ml_sample
with (
'NAMESPACE'='sqlrec',
'batch_size'='1024'
);

-- train model test_model checkpoint='test2' on ml_sample from 'test'
-- with (
-- 'NAMESPACE'='sqlrec',
-- 'batch_size'='1024'
-- );

export model test_model checkpoint='test' on ml_sample
with (
'NAMESPACE'='sqlrec'
);

create service test_service on model test_model checkpoint='test_export'
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

call call_service('test_service', t1);

create model `test_recall_model` if not exists
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

train model test_recall_model checkpoint='test' on ml_recall_sample
with (
'NAMESPACE'='sqlrec',
'batch_size'='1024'
);

export model test_recall_model checkpoint='test' on ml_recall_sample
with (
'NAMESPACE'='sqlrec'
);

create service test_recall_service_user on model test_recall_model checkpoint='test_export/user'
with (
'NAMESPACE'='sqlrec'
);

create service test_recall_service_item on model test_recall_model checkpoint='test_export/item'
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

call call_service('test_recall_service_user', tmp_user);

CACHE TABLE tmp_item AS
SELECT
    100 AS movie_id,
    ARRAY['Action', 'Adventure', 'Sci-Fi'] AS genres;

call call_service('test_recall_service_item', tmp_item);
