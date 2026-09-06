-- user_interest_genre: per-user genre affinity scores.
CREATE TABLE IF NOT EXISTS `user_interest_genre` (
  `user_id` BIGINT,
  `genre` STRING,
  `score` FLOAT,
  PRIMARY KEY (user_id)  NOT ENFORCED
) WITH (
  'connector' = 'redis',
  'data-structure' = 'list',
  'url' = 'redis://${DEFAULT_TEST_IP}:30017/0'
);
