SET table.sql-dialect = default;

CREATE TEMPORARY FUNCTION random_vec AS 'com.sqlrec.udf.scalar.RandomVecFunction';
CREATE TEMPORARY FUNCTION batch_call_service AS 'com.sqlrec.udf.udtf.BatchCallServiceUDTF';

INSERT INTO user_table
SELECT user_id, gender, age, occupation, zip_code
FROM ml_users WHERE dt = '2024-01-01';

INSERT INTO item_table
SELECT movie_id, title, genres
FROM ml_movies WHERE dt = '2024-01-01';

INSERT INTO global_hot_item
SELECT invert_key, movie_id, score
FROM offline_global_hot_item WHERE dt = '2024-01-01'
ORDER BY invert_key, score;

INSERT INTO user_interest_genre
SELECT user_id, genre, score
FROM offline_user_interest_genre WHERE dt = '2024-01-01'
ORDER BY user_id, score;

INSERT INTO genre_hot_item
SELECT genre, movie_id, score
FROM offline_genre_hot_item WHERE dt = '2024-01-01'
ORDER BY genre, score;

INSERT INTO user_recent_click_item
SELECT user_id, movie_id, bhv_time
FROM offline_user_recent_click_item WHERE dt = '2024-01-01';

INSERT INTO itemcf_i2i
SELECT movie_id1, movie_id2, score
FROM offline_itemcf_i2i WHERE dt = '2024-01-01'
ORDER BY movie_id1, score;

INSERT INTO item_embedding
SELECT 
    r.long_map['movie_id'] AS id,
    r.string_map['title'] AS title,
    r.string_array_map['genres'] AS genres,
    r.double_array_map['item_tower_emb'] AS embedding
FROM ml_movies, LATERAL TABLE(batch_call_service(
    'http://recall-service-item.sqlrec.svc.cluster.local:80/predict',
    128, 
    'movie_id', movie_id, 
    'title', title, 
    'genres', genres
)) AS r
WHERE dt = '2024-01-01';