CREATE TABLE IF NOT EXISTS `user_table` (
  `user_id` BIGINT,
  `gender` STRING,
  `age` INT,
  `occupation` INT,
  `zip_code` STRING,
  PRIMARY KEY (user_id)  NOT ENFORCED
) WITH (
  'connector' = 'redis',
  'url' = 'redis://192.168.1.5:32379/0'
);
