-- Seeded from CSV, then held in memory for this process.
CREATE TABLE IF NOT EXISTS `demo_category_hot_item` (
  `category` STRING,
  `item_id` BIGINT,
  `score` FLOAT,
  PRIMARY KEY (item_id) NOT ENFORCED
) WITH (
  'connector' = 'filesystem',
  'path' = '${SQL_SCHEMA_DIR}/quick_start/data/demo_category_hot_item.csv',
  'format' = 'csv'
);
