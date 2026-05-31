CREATE TABLE IF NOT EXISTS `itemcf_i2i` (
  `movie_id1` BIGINT,
  `movie_id2` BIGINT,
  `score` FLOAT,
  PRIMARY KEY (movie_id1)  NOT ENFORCED
) WITH (
  'connector' = 'redis',
  'data-structure' = 'list',
  'url' = 'redis://192.168.1.5:32379/0'
);
