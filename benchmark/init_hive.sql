CREATE TABLE IF NOT EXISTS `behavior_sample` (
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
 `item_tags` ARRAY<STRING>,
 `bhv_time` BIGINT,
 `is_click` FLOAT
) PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET;

ALTER TABLE `behavior_sample` ADD IF NOT EXISTS PARTITION (`dt` = '2024-01-01');
