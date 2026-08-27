-- rec_log_kafka: kafka sink for recommendation logs (JSON format).
CREATE TABLE IF NOT EXISTS `rec_log_kafka` (
  `user_id` BIGINT,
  `movie_id` BIGINT,
  `title` STRING,
  `rec_reason` STRING,
  `req_time` BIGINT,
  `req_id` STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'rec_log',
  'properties.bootstrap.servers' = '192.168.1.5:30021',
  'format' = 'json'
);
