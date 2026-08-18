-- itemcf_i2i: item-to-item co-occurrence table (item-cf).
-- movie_id1 -> movie_id2 with a co-occurrence score.
CREATE TABLE IF NOT EXISTS `itemcf_i2i` (
  `movie_id1` BIGINT,
  `movie_id2` BIGINT,
  `score` FLOAT,
  PRIMARY KEY (movie_id1)  NOT ENFORCED
) WITH (
  'connector' = 'redis',
  'data-structure' = 'list',
  'url' = 'redis://192.168.1.5:30017/0'
);
