-- genre_hot_item: hot items per genre, stored as a redis list keyed by genre.
CREATE TABLE IF NOT EXISTS `genre_hot_item` (
  `genre` STRING,
  `movie_id` BIGINT,
  `score` FLOAT,
  PRIMARY KEY (genre)  NOT ENFORCED
) WITH (
  'connector' = 'redis',
  'data-structure' = 'list',
  'url' = 'redis://192.168.1.5:32379/0'
);
