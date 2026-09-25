-- Seeded from CSV. user_id is a lookup key with three category rows per user.
CREATE TABLE IF NOT EXISTS `demo_user_interest_category` (
  `user_id` BIGINT,
  `category` STRING,
  `score` FLOAT,
  PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
  'connector' = 'filesystem',
  'path' = '${SQL_SCHEMA_DIR}/quick_start/data/demo_user_interest_category.csv',
  'format' = 'csv'
);
