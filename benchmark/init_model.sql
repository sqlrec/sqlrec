create model `test_model`
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
'batch_size'='128'
);

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