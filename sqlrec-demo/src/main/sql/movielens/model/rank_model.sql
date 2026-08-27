/*
 * rank_model: Wide & Deep ranking model (tzrec.wide_and_deep).
 * Inputs: user features + item features; label: rating.
 */
create model if not exists `rank_model`
(
 `user_id` BIGINT,
 `movie_id` BIGINT,
 `genres` ARRAY<STRING>,
 `gender` STRING,
 `age` INT,
 `occupation` INT,
 `zip_code` STRING
)
with (
'model'='tzrec.wide_and_deep',
'label_columns'='rating'
);
