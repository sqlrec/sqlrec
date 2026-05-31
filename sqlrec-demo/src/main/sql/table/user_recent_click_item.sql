CREATE TABLE IF NOT EXISTS `user_recent_click_item` (
  `user_id` BIGINT,
  `movie_id` BIGINT,
  `bhv_time` BIGINT,
  PRIMARY KEY (user_id)  NOT ENFORCED
) WITH (
  'connector' = 'redis',
  'data-structure' = 'list',
  'url' = 'redis://192.168.1.5:32379/0'
);
