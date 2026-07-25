SET table.sql-dialect = default;

CREATE TABLE IF NOT EXISTS `user_table` (
  `user_id` BIGINT,
  `gender` STRING,
  `age` INT,
  `occupation` INT,
  `zip_code` STRING,
  PRIMARY KEY (user_id)  NOT ENFORCED
) WITH (
  'connector' = 'redis',
  'url' = 'redis://${NODE_IP}:${REDIS_PORT}/0'
);

CREATE TABLE IF NOT EXISTS `item_table` (
  `movie_id` BIGINT,
  `title` STRING,
  `genres` ARRAY<STRING>,
  PRIMARY KEY (movie_id)  NOT ENFORCED
) WITH (
  'connector' = 'redis',
  'url' = 'redis://${NODE_IP}:${REDIS_PORT}/0'
);

CREATE TABLE IF NOT EXISTS `global_hot_item` (
  `invert_key` STRING,
  `movie_id` BIGINT,
  `score` FLOAT,
  PRIMARY KEY (invert_key)  NOT ENFORCED
) WITH (
  'connector' = 'redis',
  'data-structure' = 'list',
  'url' = 'redis://${NODE_IP}:${REDIS_PORT}/0'
);

CREATE TABLE IF NOT EXISTS `user_interest_genre` (
  `user_id` BIGINT,
  `genre` STRING,
  `score` FLOAT,
  PRIMARY KEY (user_id)  NOT ENFORCED
) WITH (
  'connector' = 'redis',
  'data-structure' = 'list',
  'url' = 'redis://${NODE_IP}:${REDIS_PORT}/0'
);

CREATE TABLE IF NOT EXISTS `genre_hot_item` (
  `genre` STRING,
  `movie_id` BIGINT,
  `score` FLOAT,
  PRIMARY KEY (genre)  NOT ENFORCED
) WITH (
  'connector' = 'redis',
  'data-structure' = 'list',
  'url' = 'redis://${NODE_IP}:${REDIS_PORT}/0'
);

CREATE TABLE IF NOT EXISTS `user_recent_click_item` (
  `user_id` BIGINT,
  `movie_id` BIGINT,
  `bhv_time` BIGINT,
  PRIMARY KEY (user_id)  NOT ENFORCED
) WITH (
  'connector' = 'redis',
  'data-structure' = 'list',
  'url' = 'redis://${NODE_IP}:${REDIS_PORT}/0'
);

CREATE TABLE IF NOT EXISTS `user_exposure_item` (
  `user_id` BIGINT,
  `movie_id` BIGINT,
  `bhv_time` BIGINT,
  PRIMARY KEY (user_id)  NOT ENFORCED
) WITH (
  'connector' = 'redis',
  'data-structure' = 'list',
  'url' = 'redis://${NODE_IP}:${REDIS_PORT}/0',
  'cache-ttl' = '0',
  'ttl' = '3600'
);

CREATE TABLE IF NOT EXISTS `itemcf_i2i` (
  `movie_id1` BIGINT,
  `movie_id2` BIGINT,
  `score` FLOAT,
  PRIMARY KEY (movie_id1)  NOT ENFORCED
) WITH (
  'connector' = 'redis',
  'data-structure' = 'list',
  'url' = 'redis://${NODE_IP}:${REDIS_PORT}/0'
);

CREATE TABLE IF NOT EXISTS `item_embedding` (
  `id` BIGINT,
  `title` STRING,
  `genres` ARRAY<STRING>,
  `embedding` ARRAY<DOUBLE>,
  PRIMARY KEY (id)  NOT ENFORCED
) WITH (
  'connector' = 'milvus',
  'url' = 'http://${NODE_IP}:${MILVUS_PORT}',
  'token' = 'root:Milvus',
  'database' = 'default',
  'collection' = 'item_embedding'
);

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
  'properties.bootstrap.servers' = '${NODE_IP}:${KAFKA_PORT}',
  'format' = 'json'
);
