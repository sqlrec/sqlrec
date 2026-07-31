package com.sqlrec.model.gbdt;

import com.sqlrec.common.config.ConfigOption;
import com.sqlrec.model.common.ModelConfigBase;

/**
 * Configuration class for GBDT (CatBoost / LightGBM) model-related parameters.
 *
 * <p>Inherits the shared options (image version, label columns, pod resources, replicas and the
 * shell/pipeline-config file names) from {@link ModelConfigBase}. Only GBDT-specific training
 * parameters and the GBDT image name are declared here.
 */
public class Config extends ModelConfigBase {

    public static final ConfigOption<String> IMAGE = new ConfigOption<>(
            "image",
            "sqlrec/gbdt", "Docker image", null, String.class);

    // Common GBDT training parameters
    public static final ConfigOption<String> OBJECTIVE = new ConfigOption<>(
            "objective",
            "binary", "Learning objective (binary, multiclass, regression)", null, String.class);
    public static final ConfigOption<String> METRIC = new ConfigOption<>(
            "metric",
            "auc", "Evaluation metric (auc, logloss, rmse)", null, String.class);
    public static final ConfigOption<Integer> NUM_ITERATIONS = new ConfigOption<>(
            "num_iterations",
            300, "Number of boosting iterations", null, Integer.class);
    public static final ConfigOption<Double> LEARNING_RATE = new ConfigOption<>(
            "learning_rate",
            0.1, "Learning rate / shrinkage", null, Double.class);
    public static final ConfigOption<Integer> MAX_DEPTH = new ConfigOption<>(
            "max_depth",
            6, "Maximum tree depth", null, Integer.class);
    public static final ConfigOption<Integer> NUM_LEAVES = new ConfigOption<>(
            "num_leaves",
            63, "Maximum number of leaves per tree (LightGBM)", null, Integer.class);
    public static final ConfigOption<Double> FEATURE_FRACTION = new ConfigOption<>(
            "feature_fraction",
            0.8, "Fraction of features used per tree (LightGBM)", null, Double.class);
    public static final ConfigOption<Double> BAGGING_FRACTION = new ConfigOption<>(
            "bagging_fraction",
            0.8, "Fraction of data used per tree (LightGBM)", null, Double.class);
    public static final ConfigOption<Integer> BAGGING_FREQ = new ConfigOption<>(
            "bagging_freq",
            5, "Frequency of bagging (LightGBM)", null, Integer.class);
    public static final ConfigOption<Integer> MIN_DATA_IN_LEAF = new ConfigOption<>(
            "min_data_in_leaf",
            20, "Minimum number of samples in one leaf", null, Integer.class);

    // XGBoost specific
    public static final ConfigOption<Integer> MIN_CHILD_WEIGHT = new ConfigOption<>(
            "min_child_weight",
            1, "Minimum sum of instance weight needed in a child (XGBoost)", null, Integer.class);

    public static final ConfigOption<Double> L2_REGULARIZATION = new ConfigOption<>(
            "l2_regularization",
            1.0, "L2 regularization coefficient", null, Double.class);

    // CatBoost specific
    public static final ConfigOption<Integer> CB_ITERATIONS = new ConfigOption<>(
            "cb_iterations",
            1000, "CatBoost number of iterations (overrides num_iterations when set)", null, Integer.class);
    public static final ConfigOption<Integer> CB_DEPTH = new ConfigOption<>(
            "cb_depth",
            6, "CatBoost tree depth (overrides max_depth when set)", null, Integer.class);
    public static final ConfigOption<Double> CB_L2_LEAF_REG = new ConfigOption<>(
            "cb_l2_leaf_reg",
            3.0, "CatBoost L2 leaf regularization", null, Double.class);
}
