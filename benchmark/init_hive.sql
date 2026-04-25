CREATE TABLE IF NOT EXISTS `ml_users` (
 `user_id` BIGINT,
 `gender` STRING,
 `age` INT,
 `occupation` INT,
 `zip_code` STRING
) PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS `ml_movies` (
 `movie_id` BIGINT,
 `title` STRING,
 `genres` ARRAY<STRING>
) PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS `ml_ratings` (
 `user_id` BIGINT,
 `movie_id` BIGINT,
 `rating` FLOAT,
 `timestamp` BIGINT
) PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS `offline_global_hot_item` (
 `invert_key` STRING,
 `movie_id` BIGINT,
 `score` FLOAT
) PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS `offline_user_interest_genre` (
 `user_id` BIGINT,
 `genre` STRING,
 `score` FLOAT
) PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS `offline_genre_hot_item` (
 `genre` STRING,
 `movie_id` BIGINT,
 `score` FLOAT
) PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS `offline_user_recent_click_item` (
 `user_id` BIGINT,
 `movie_id` BIGINT,
 `bhv_time` BIGINT
) PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS `offline_itemcf_i2i` (
 `movie_id1` BIGINT,
 `movie_id2` BIGINT,
 `score` FLOAT
) PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS `ml_sample` (
 `user_id` BIGINT,
 `movie_id` BIGINT,
 `rating` FLOAT,
 `timestamp` BIGINT,
 `genres` ARRAY<STRING>,
 `gender` STRING,
 `age` INT,
 `occupation` INT,
 `zip_code` STRING
) PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET;

ALTER TABLE `ml_users` ADD IF NOT EXISTS PARTITION (`dt` = '2024-01-01');
ALTER TABLE `ml_movies` ADD IF NOT EXISTS PARTITION (`dt` = '2024-01-01');
ALTER TABLE `ml_ratings` ADD IF NOT EXISTS PARTITION (`dt` = '2024-01-01');
