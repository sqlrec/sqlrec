SET table.sql-dialect = default;

CREATE TEMPORARY FUNCTION random_vec AS 'com.sqlrec.udf.scalar.RandomVecFunction';

INSERT INTO user_table
SELECT user_id, gender, age, occupation, zip_code
FROM ml_users WHERE dt = '2024-01-01';

INSERT INTO item_table
SELECT movie_id, title, genres
FROM ml_movies WHERE dt = '2024-01-01';

INSERT INTO global_hot_item
SELECT invert_key, movie_id, score
FROM offline_global_hot_item WHERE dt = '2024-01-01';

INSERT INTO user_interest_genre
SELECT user_id, genre, score
FROM offline_user_interest_genre WHERE dt = '2024-01-01';

INSERT INTO genre_hot_item
SELECT genre, movie_id, score
FROM offline_genre_hot_item WHERE dt = '2024-01-01';

INSERT INTO user_recent_click_item
SELECT user_id, movie_id, bhv_time
FROM offline_user_recent_click_item WHERE dt = '2024-01-01';

INSERT INTO itemcf_i2i
SELECT movie_id1, movie_id2, score
FROM offline_itemcf_i2i WHERE dt = '2024-01-01';

INSERT INTO item_embedding
SELECT movie_id, title, genres, random_vec('64')
FROM ml_movies WHERE dt = '2024-01-01';