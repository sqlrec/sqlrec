SET table.sql-dialect = default;

INSERT INTO sink_user_table
SELECT user_id, gender, age, occupation, zip_code
FROM source_user_table WHERE dt = '2024-01-01';

INSERT INTO sink_item_table
SELECT movie_id, title, genres
FROM source_item_table WHERE dt = '2024-01-01';

INSERT INTO sink_global_hot_item
SELECT invert_key, movie_id, score
FROM source_global_hot_item WHERE dt = '2024-01-01';

INSERT INTO sink_user_interest_genre
SELECT user_id, genre, score
FROM source_user_interest_genre WHERE dt = '2024-01-01';

INSERT INTO sink_genre_hot_item
SELECT genre, movie_id, score
FROM source_genre_hot_item WHERE dt = '2024-01-01';

INSERT INTO sink_user_recent_click_item
SELECT user_id, movie_id, bhv_time
FROM source_user_recent_click_item WHERE dt = '2024-01-01';

INSERT INTO sink_itemcf_i2i
SELECT movie_id1, movie_id2, score
FROM source_itemcf_i2i WHERE dt = '2024-01-01';
