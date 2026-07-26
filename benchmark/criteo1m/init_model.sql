-- Criteo 1M GBDT benchmark: model training, export, serving, and inference test.
-- CatBoost, LightGBM, and XGBoost share the same `criteo` table.
--   CatBoost  — model definition includes all 39 features (FLOAT + STRING)
--   LightGBM  — model definition includes only the 13 numeric features (FLOAT)
--   XGBoost   — model definition includes only the 13 numeric features (FLOAT)

-- ===========================================================================
-- CatBoost model — supports STRING categorical features natively
-- ===========================================================================
CREATE MODEL IF NOT EXISTS `cb_ctr_model` (
 `I1`  FLOAT, `I2`  FLOAT, `I3`  FLOAT, `I4`  FLOAT, `I5`  FLOAT,
 `I6`  FLOAT, `I7`  FLOAT, `I8`  FLOAT, `I9`  FLOAT, `I10` FLOAT,
 `I11` FLOAT, `I12` FLOAT, `I13` FLOAT,
 `C1`  STRING, `C2`  STRING, `C3`  STRING, `C4`  STRING, `C5`  STRING,
 `C6`  STRING, `C7`  STRING, `C8`  STRING, `C9`  STRING, `C10` STRING,
 `C11` STRING, `C12` STRING, `C13` STRING, `C14` STRING, `C15` STRING,
 `C16` STRING, `C17` STRING, `C18` STRING, `C19` STRING, `C20` STRING,
 `C21` STRING, `C22` STRING, `C23` STRING, `C24` STRING, `C25` STRING,
 `C26` STRING,
 `label` INT
) WITH (
 'model'='gbdt.catboost',
 'label_columns'='label'
);

-- Train (use small iterations for benchmark)
TRAIN MODEL cb_ctr_model CHECKPOINT='v1' ON criteo
WITH (
 'NAMESPACE'='sqlrec',
 'pod_memory'='4Gi',
 'cb_iterations'='50'
);

-- Export to serving format (.cbm)
EXPORT MODEL cb_ctr_model CHECKPOINT='v1' ON criteo
WITH (
 'NAMESPACE'='sqlrec',
 'pod_memory'='4Gi'
);

-- Create serving service
CREATE SERVICE cb_ctr_service ON MODEL cb_ctr_model CHECKPOINT='v1_export'
WITH (
 'NAMESPACE'='sqlrec'
);

call sleep('3000');

-- Inference test
CACHE TABLE cb_test_input AS
SELECT
  120.0 AS I1, 5.0 AS I2, 0.0 AS I3, 3.0 AS I4, 2.0 AS I5,
  10.0 AS I6, 1.0 AS I7, 4.0 AS I8, 50.0 AS I9, 0.0 AS I10,
  0.0 AS I11, 0.0 AS I12, 0.0 AS I13,
  '05db9164' AS C1, '68fd1e25' AS C2, '5a9ed9b0' AS C3, '5b392875' AS C4,
  '8ef127cb' AS C5, 'b285f96d' AS C6, '07c540c4' AS C7, '4cfef0f3' AS C8,
  '07d13a8f' AS C9, 'be9b4610' AS C10, '875862a4' AS C11, 'e5b8bd5a' AS C12,
  '37e7b9d4' AS C13, '776318b6' AS C14, '4f5e5d7a' AS C15, 'b043a4d7' AS C16,
  '3e4a8d1c' AS C17, '9a2e5d8f' AS C18, '5c7e8a1b' AS C19, '7f3d6c2e' AS C20,
  '1a2b3c4d' AS C21, '6e5f4a3b' AS C22, '2d3c4b5a' AS C23, '8f7e6d5c' AS C24,
  '4b3a2918' AS C25, '7c6b5a49' AS C26;

CALL call_service('cb_ctr_service', cb_test_input);


-- ===========================================================================
-- LightGBM model — only numeric features (I1-I13), no categorical features
-- ===========================================================================
CREATE MODEL IF NOT EXISTS `lgb_ctr_model` (
 `I1`  FLOAT, `I2`  FLOAT, `I3`  FLOAT, `I4`  FLOAT, `I5`  FLOAT,
 `I6`  FLOAT, `I7`  FLOAT, `I8`  FLOAT, `I9`  FLOAT, `I10` FLOAT,
 `I11` FLOAT, `I12` FLOAT, `I13` FLOAT,
 `label` INT
) WITH (
 'model'='gbdt.lightgbm',
 'label_columns'='label'
);

-- Train (use small iterations for benchmark)
TRAIN MODEL lgb_ctr_model CHECKPOINT='v1' ON criteo
WITH (
 'NAMESPACE'='sqlrec',
 'pod_memory'='4Gi',
 'num_iterations'='50'
);

-- Export to serving format (.onnx)
EXPORT MODEL lgb_ctr_model CHECKPOINT='v1' ON criteo
WITH (
 'NAMESPACE'='sqlrec',
 'pod_memory'='4Gi'
);

-- Create serving service
CREATE SERVICE lgb_ctr_service ON MODEL lgb_ctr_model CHECKPOINT='v1_export'
WITH (
 'NAMESPACE'='sqlrec'
);

call sleep('3000');

-- Inference test (only I1-I13, matching the model definition)
CACHE TABLE lgb_test_input AS
SELECT
  120.0 AS I1, 5.0 AS I2, 0.0 AS I3, 3.0 AS I4, 2.0 AS I5,
  10.0 AS I6, 1.0 AS I7, 4.0 AS I8, 50.0 AS I9, 0.0 AS I10,
  0.0 AS I11, 0.0 AS I12, 0.0 AS I13;

CALL call_service('lgb_ctr_service', lgb_test_input);


-- ===========================================================================
-- XGBoost model — only numeric features (I1-I13), same as LightGBM
-- ===========================================================================
CREATE MODEL IF NOT EXISTS `xgb_ctr_model` (
 `I1`  FLOAT, `I2`  FLOAT, `I3`  FLOAT, `I4`  FLOAT, `I5`  FLOAT,
 `I6`  FLOAT, `I7`  FLOAT, `I8`  FLOAT, `I9`  FLOAT, `I10` FLOAT,
 `I11` FLOAT, `I12` FLOAT, `I13` FLOAT,
 `label` INT
) WITH (
 'model'='gbdt.xgboost',
 'label_columns'='label'
);

-- Train (use small iterations for benchmark)
TRAIN MODEL xgb_ctr_model CHECKPOINT='v1' ON criteo
WITH (
 'NAMESPACE'='sqlrec',
 'pod_memory'='4Gi',
 'num_iterations'='50'
);

-- Export to serving format (.onnx)
EXPORT MODEL xgb_ctr_model CHECKPOINT='v1' ON criteo
WITH (
 'NAMESPACE'='sqlrec',
 'pod_memory'='4Gi'
);

-- Create serving service
CREATE SERVICE xgb_ctr_service ON MODEL xgb_ctr_model CHECKPOINT='v1_export'
WITH (
 'NAMESPACE'='sqlrec'
);

call sleep('3000');

-- Inference test (only I1-I13, matching the model definition)
CACHE TABLE xgb_test_input AS
SELECT
  120.0 AS I1, 5.0 AS I2, 0.0 AS I3, 3.0 AS I4, 2.0 AS I5,
  10.0 AS I6, 1.0 AS I7, 4.0 AS I8, 50.0 AS I9, 0.0 AS I10,
  0.0 AS I11, 0.0 AS I12, 0.0 AS I13;

CALL call_service('xgb_ctr_service', xgb_test_input);
