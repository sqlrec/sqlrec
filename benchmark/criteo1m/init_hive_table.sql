-- Hive table for Criteo 1M GBDT benchmark.
-- A single table is shared by both CatBoost and LightGBM:
--   CatBoost  — uses all columns (FLOAT numeric + STRING categorical)
--   LightGBM  — uses only the numeric columns (I1-I13) in its model definition

CREATE TABLE IF NOT EXISTS `criteo` (
 `label` INT,
 `I1`  FLOAT, `I2`  FLOAT, `I3`  FLOAT, `I4`  FLOAT, `I5`  FLOAT,
 `I6`  FLOAT, `I7`  FLOAT, `I8`  FLOAT, `I9`  FLOAT, `I10` FLOAT,
 `I11` FLOAT, `I12` FLOAT, `I13` FLOAT,
 `C1`  STRING, `C2`  STRING, `C3`  STRING, `C4`  STRING, `C5`  STRING,
 `C6`  STRING, `C7`  STRING, `C8`  STRING, `C9`  STRING, `C10` STRING,
 `C11` STRING, `C12` STRING, `C13` STRING, `C14` STRING, `C15` STRING,
 `C16` STRING, `C17` STRING, `C18` STRING, `C19` STRING, `C20` STRING,
 `C21` STRING, `C22` STRING, `C23` STRING, `C24` STRING, `C25` STRING,
 `C26` STRING
) PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET;

ALTER TABLE `criteo` ADD IF NOT EXISTS PARTITION (`dt` = '2024-01-01');
