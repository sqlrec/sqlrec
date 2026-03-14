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