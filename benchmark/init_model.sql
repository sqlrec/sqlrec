create model `test_model`
(
 `user_id` BIGINT,
 `user_name` STRING,
 `user_country` STRING,
 `user_age` INT,
 `user_os` STRING,
 `user_network` STRING,
 `item_id` BIGINT,
 `item_name` STRING,
 `item_brand` STRING,
 `item_category1` STRING,
 `item_category2` STRING,
 `item_category3` STRING,
 `item_category4` STRING
)
with (
'model'='torch_easy_rec.wide_and_deep',
'label_fields'='is_click'
);

train model test_model checkpoint='test' on behavior_sample
with (
'NAMESPACE'='sqlrec',
'batch_size'='128'
);

export model test_model checkpoint='test' on behavior_sample
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
    'Zhang' AS user_name,
    'China' AS user_country,
    28 AS user_age,
    'Android 14' AS user_os,
    '5G' AS user_network,
    2 AS item_id,
    'Smart Watch' AS item_name,
    'xm' AS item_brand,
    'Digital Products' AS item_category1,
    'Smart Wearables' AS item_category2,
    'Watch' AS item_category3,
    'Smart Watch' AS item_category4;

call call_service('test_service', t1);