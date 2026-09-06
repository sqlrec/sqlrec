-- global_hot_item: globally hot items, keyed by an invert_key (e.g. 'global').
CREATE TABLE IF NOT EXISTS `global_hot_item` (
  `invert_key` STRING,
  `movie_id` BIGINT,
  `score` FLOAT,
  PRIMARY KEY (invert_key)  NOT ENFORCED
) WITH (
  'connector' = 'redis',
  'data-structure' = 'list',
  'url' = 'redis://${DEFAULT_TEST_IP}:30017/0'
);
