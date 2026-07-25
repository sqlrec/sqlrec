package com.sqlrec.model.gbdt;

import com.sqlrec.common.config.ConfigOption;
import com.sqlrec.common.config.SqlRecConfigs;

/**
 * Configuration class for GBDT (CatBoost / LightGBM) model-related parameters.
 */
public class Config {
    public static final String SHELL_DIR = "/data";
    public static final String PIPELINE_CONFIG_NAME = "pipeline.config";
    public static final String START_SHELL_NAME = "start.sh";

    public static final ConfigOption<String> IMAGE = new ConfigOption<>(
            "image",
            "sqlrec/gbdt", "Docker image", null, String.class);
    public static final ConfigOption<String> VERSION = new ConfigOption<>(
            "version",
            SqlRecConfigs.SQLREC_VERSION.getValue() + "-cpu", "Docker image version", null, String.class);

    // Label configuration
    public static final ConfigOption<String> LABEL_COLUMNS = new ConfigOption<>(
            "label_columns",
            null, "Label columns", null, String.class);

    // Common GBDT training parameters
    public static final ConfigOption<String> OBJECTIVE = new ConfigOption<>(
            "objective",
            "binary", "Learning objective (binary, multiclass, regression)", null, String.class);
    public static final ConfigOption<String> METRIC = new ConfigOption<>(
            "metric",
            "auc", "Evaluation metric (auc, logloss, rmse)", null, String.class);
    public static final ConfigOption<Integer> NUM_ITERATIONS = new ConfigOption<>(
            "num_iterations",
            100, "Number of boosting iterations", null, Integer.class);
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
            0.9, "Fraction of features used per tree (LightGBM)", null, Double.class);
    public static final ConfigOption<Double> BAGGING_FRACTION = new ConfigOption<>(
            "bagging_fraction",
            0.9, "Fraction of data used per tree (LightGBM)", null, Double.class);
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

    public static final ConfigOption<Integer> POD_CPU_CORES = new ConfigOption<>(
            "pod_cpu_cores",
            1, "Number of CPU cores for pod (used as resource request)", null, Integer.class);

    public static final ConfigOption<String> POD_MEMORY = new ConfigOption<>(
            "pod_memory",
            "2Gi", "Memory for pod (used as resource request)", null, String.class);

    public static final ConfigOption<String> POD_CPU_LIMIT = new ConfigOption<>(
            "pod_cpu_limit",
            null, "CPU limit for pod (e.g. '2' or '2000m'). If not set, no CPU limit is configured.", null, String.class);

    public static final ConfigOption<String> POD_MEMORY_LIMIT = new ConfigOption<>(
            "pod_memory_limit",
            null, "Memory limit for pod (e.g. '8Gi'). If not set, no memory limit is configured.", null, String.class);

    public static final ConfigOption<Integer> REPLICAS = new ConfigOption<>(
            "replicas",
            1, "Number of replicas for deployment", null, Integer.class);
}
