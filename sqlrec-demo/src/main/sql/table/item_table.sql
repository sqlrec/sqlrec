CREATE TABLE IF NOT EXISTS `item_table` (
  `movie_id` BIGINT,
  `title` STRING,
  `genres` ARRAY<STRING>,
  PRIMARY KEY (movie_id)  NOT ENFORCED
) WITH (
  'connector' = 'redis',
  'url' = 'redis://192.168.1.5:32379/0'
);
