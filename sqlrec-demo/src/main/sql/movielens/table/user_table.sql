-- user_table: user profile (demographics), keyed by user_id in redis.
CREATE TABLE IF NOT EXISTS `user_table` (
  `user_id` BIGINT,
  `gender` STRING,
  `age` INT,
  `occupation` INT,
  `zip_code` STRING,
  PRIMARY KEY (user_id)  NOT ENFORCED
) WITH (
  'connector' = 'redis',
  'url' = 'redis://${DEFAULT_TEST_IP}:30017/0'
);
