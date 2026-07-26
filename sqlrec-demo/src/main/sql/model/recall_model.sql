/*
 * recall_model: DSSM two-tower recall model (tzrec.dssm).
 * Trained jointly on user and item features; serves two towers via
 * recall_service_user and recall_service_item.
 */
create model if not exists `recall_model`
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
'model'='tzrec.dssm',
'label_columns'='rating',
'item_features'='movie_id,genres'
);