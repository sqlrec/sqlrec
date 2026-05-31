CREATE TABLE IF NOT EXISTS `global_hot_item` (
  `invert_key` STRING,
  `movie_id` BIGINT,
  `score` FLOAT,
  PRIMARY KEY (invert_key)  NOT ENFORCED
) WITH (
  'connector' = 'redis',
  'data-structure' = 'list',
  'url' = 'redis://192.168.1.5:32379/0'
);
