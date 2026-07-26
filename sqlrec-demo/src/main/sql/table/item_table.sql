-- Item table: stores movie metadata; sourced from redis
-- Note: semicolons inside comments should NOT split statements ; select 1
CREATE TABLE IF NOT EXISTS `item_table` (
  `movie_id` BIGINT,           -- unique movie identifier
  `title` STRING,              -- movie title
  `genres` ARRAY<STRING>,      -- genre list
  PRIMARY KEY (movie_id)  NOT ENFORCED
) WITH (
  'connector' = 'redis',
  'url' = 'redis://192.168.1.5:32379/0'
);
/* end of item_table definition */
