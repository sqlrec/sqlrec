-- Quick-start hot items are written to memory at runtime.
CREATE TABLE IF NOT EXISTS `demo_category_hot_item` (
  `category` STRING,
  `item_id` BIGINT,
  `score` FLOAT,
  PRIMARY KEY (item_id) NOT ENFORCED
) WITH (
  'connector' = 'filesystem'
);
