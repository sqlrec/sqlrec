SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE offline_global_hot_item PARTITION(dt='2024-01-01')
SELECT
    'global' as invert_key,
    movie_id,
    score
FROM (
    SELECT
        movie_id,
        SUM(rating) as score,
        ROW_NUMBER() OVER (ORDER BY SUM(rating) DESC) as rn
    FROM ml_ratings
    WHERE dt = '2024-01-01'
    GROUP BY movie_id
) t
WHERE rn <= 1000;

INSERT OVERWRITE TABLE offline_user_interest_genre PARTITION(dt='2024-01-01')
SELECT
    u.user_id,
    genre,
    SUM(r.rating) as score
FROM ml_ratings r
JOIN ml_movies m ON r.movie_id = m.movie_id AND m.dt = '2024-01-01'
JOIN ml_users u ON r.user_id = u.user_id AND u.dt = '2024-01-01'
LATERAL VIEW explode(m.genres) g AS genre
WHERE r.dt = '2024-01-01'
GROUP BY u.user_id, genre;

INSERT OVERWRITE TABLE offline_genre_hot_item PARTITION(dt='2024-01-01')
SELECT
    genre,
    movie_id,
    score
FROM (
    SELECT
        genre,
        movie_id,
        SUM(rating) as score,
        ROW_NUMBER() OVER (PARTITION BY genre ORDER BY SUM(rating) DESC) as rn
    FROM (
        SELECT
            m.movie_id,
            genre,
            r.rating
        FROM ml_ratings r
        JOIN ml_movies m ON r.movie_id = m.movie_id AND m.dt = '2024-01-01'
        LATERAL VIEW explode(m.genres) g AS genre
        WHERE r.dt = '2024-01-01'
    ) t1
    GROUP BY movie_id, genre
) t2
WHERE rn <= 1000;

INSERT OVERWRITE TABLE offline_user_recent_click_item PARTITION(dt='2024-01-01')
SELECT
    user_id,
    movie_id,
    timestamp as bhv_time
FROM (
    SELECT
        user_id,
        movie_id,
        timestamp,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY timestamp DESC) as rn
    FROM ml_ratings
    WHERE dt = '2024-01-01'
) t
WHERE rn <= 100;

CACHE TABLE movie_user_count AS
SELECT
    movie_id,
    COUNT(DISTINCT user_id) as user_count
FROM ml_ratings
WHERE dt = '2024-01-01'
GROUP BY movie_id;

INSERT OVERWRITE TABLE offline_itemcf_i2i PARTITION(dt='2024-01-01')
SELECT
    movie_id1,
    movie_id2,
    score
FROM (
    SELECT
        r1.movie_id as movie_id1,
        r2.movie_id as movie_id2,
        COUNT(*) / (SQRT(uc1.user_count) * SQRT(uc2.user_count)) as score,
        ROW_NUMBER() OVER (PARTITION BY r1.movie_id ORDER BY COUNT(*) / (SQRT(uc1.user_count) * SQRT(uc2.user_count)) DESC) as rn
    FROM ml_ratings r1
    JOIN ml_ratings r2 ON r1.user_id = r2.user_id AND r1.movie_id != r2.movie_id
    JOIN movie_user_count uc1 ON r1.movie_id = uc1.movie_id
    JOIN movie_user_count uc2 ON r2.movie_id = uc2.movie_id
    WHERE r1.dt = '2024-01-01' AND r2.dt = '2024-01-01'
    GROUP BY r1.movie_id, r2.movie_id, uc1.user_count, uc2.user_count
    HAVING COUNT(*) >= 3
) t
WHERE rn <= 100;

INSERT OVERWRITE TABLE ml_sample PARTITION(dt='2024-01-01')
SELECT
    r.user_id,
    r.movie_id,
    r.rating,
    r.timestamp,
    m.genres,
    u.gender,
    u.age,
    u.occupation,
    u.zip_code
FROM ml_ratings r
JOIN ml_movies m ON r.movie_id = m.movie_id AND m.dt = '2024-01-01'
JOIN ml_users u ON r.user_id = u.user_id AND u.dt = '2024-01-01'
WHERE r.dt = '2024-01-01';
