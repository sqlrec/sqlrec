-- Quick-start user interests are written to memory at runtime.
CREATE TABLE IF NOT EXISTS `demo_user_interest_category` (
  `user_id` BIGINT,
  `category` STRING,
  `score` FLOAT,
  PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
  'connector' = 'filesystem'
);
