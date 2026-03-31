create model `test_model`
(
 `user_id` BIGINT,
 `user_name` STRING,
 `user_country` STRING,
 `user_age` INT,
 `user_os` STRING,
 `user_network` STRING,
 `user_clicked_items` ARRAY<BIGINT>,
 `item_id` BIGINT,
 `item_name` STRING,
 `item_brand` STRING,
 `item_category1` STRING,
 `item_category2` STRING,
 `item_category3` STRING,
 `item_category4` STRING,
 `item_tags` ARRAY<STRING>
)
with (
'model'='tzrec.wide_and_deep',
'label_columns'='is_click'
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
    ARRAY[100, 200, 300, 400] AS user_clicked_items,
    2 AS item_id,
    'Smart Watch' AS item_name,
    'xm' AS item_brand,
    'Digital Products' AS item_category1,
    'Smart Wearables' AS item_category2,
    'Watch' AS item_category3,
    'Smart Watch' AS item_category4,
    ARRAY['hot', 'new', 'sale', 'trending'] AS item_tags;

call call_service('test_service', t1);