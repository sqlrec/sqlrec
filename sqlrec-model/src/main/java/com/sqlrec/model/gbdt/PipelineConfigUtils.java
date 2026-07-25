package com.sqlrec.model.gbdt;

import com.sqlrec.common.config.ConfigOption;
import com.sqlrec.common.model.ModelConfig;
import com.sqlrec.common.model.ModelExportConf;
import com.sqlrec.common.model.ModelTrainConf;
import com.sqlrec.common.schema.FieldSchema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Generates pipeline.config (JSON) for the GBDT Python train/export entry points.
 */
public class PipelineConfigUtils {

    public enum ModelType {
        LIGHTGBM("lightgbm"),
        XGBOOST("xgboost"),
        CATBOOST("catboost");

        private final String key;

        ModelType(String key) {
            this.key = key;
        }

        public String getKey() {
            return key;
        }
    }

    public static String generateTrainConfig(ModelType modelType, ModelConfig model, ModelTrainConf trainConf) {
        StringBuilder config = new StringBuilder();
        config.append("{\n");

        appendStringField(config, "model_type", modelType.getKey(), 1, true);
        appendStringField(config, "train_input_path",
                joinPaths(trainConf.getTrainDataPaths()), 1, true);
        appendStringField(config, "model_dir", trainConf.getModelDir(), 1, true);
        appendStringField(config, "base_model_dir", trainConf.getBaseModelDir(), 1, true);

        appendStringField(config, "label_columns",
                Config.LABEL_COLUMNS.getValue(model.getParams()), 1, true);

        List<String> featureNames = getFeatureNames(model, modelType);
        List<String> categoricalFeatures = resolveCategoricalFeatures(model, modelType);

        appendListField(config, "feature_columns", featureNames, 1, true);
        appendListField(config, "categorical_features", categoricalFeatures, 1, true);

        Map<String, String> params = mergeParams(model.getParams(), trainConf.getParams());
        appendParams(config, params, modelType);

        config.append("}\n");
        return config.toString();
    }

    public static String generateExportConfig(ModelExportConf exportConf) {
        String baseModelDir = exportConf.getBaseModelDir();
        if (baseModelDir == null || baseModelDir.isEmpty()) {
            throw new IllegalArgumentException("base_model_dir is required for GBDT export");
        }
        String exportDir = baseModelDir + "_export";
        StringBuilder config = new StringBuilder();
        config.append("{\n");
        appendStringField(config, "base_model_dir", exportConf.getBaseModelDir(), 1, true);
        appendStringField(config, "export_dir", exportDir, 1, false);
        config.append("}\n");
        return config.toString();
    }

    private static void appendParams(StringBuilder config, Map<String, String> params, ModelType modelType) {
        config.append("  \"params\": {\n");
        appendStringField(config, "objective", Config.OBJECTIVE.getValue(params), 2, true);
        appendStringField(config, "metric", Config.METRIC.getValue(params), 2, true);

        if (modelType == ModelType.CATBOOST) {
            // CatBoost uses its own param names; CB_* overrides generic GBDT params.
            int iterations = resolveInt(Config.CB_ITERATIONS, Config.NUM_ITERATIONS, params);
            int depth = resolveInt(Config.CB_DEPTH, Config.MAX_DEPTH, params);
            double l2LeafReg = resolveDouble(Config.CB_L2_LEAF_REG, Config.L2_REGULARIZATION, params);
            appendIntField(config, "iterations", iterations, 2, true);
            appendIntField(config, "depth", depth, 2, true);
            appendDoubleField(config, "l2_leaf_reg", l2LeafReg, 2, true);
            appendDoubleField(config, "learning_rate", Config.LEARNING_RATE.getValue(params), 2, false);
        } else if (modelType == ModelType.XGBOOST) {
            // XGBoost params: reuse generic names where they align, but the Python
            // side maps them to XGBoost-native names (e.g. bagging_fraction → subsample).
            appendIntField(config, "num_iterations", Config.NUM_ITERATIONS.getValue(params), 2, true);
            appendIntField(config, "max_depth", Config.MAX_DEPTH.getValue(params), 2, true);
            appendDoubleField(config, "learning_rate", Config.LEARNING_RATE.getValue(params), 2, true);
            appendDoubleField(config, "feature_fraction", Config.FEATURE_FRACTION.getValue(params), 2, true);
            appendDoubleField(config, "bagging_fraction", Config.BAGGING_FRACTION.getValue(params), 2, true);
            appendIntField(config, "min_child_weight", Config.MIN_CHILD_WEIGHT.getValue(params), 2, true);
            appendDoubleField(config, "l2_regularization", Config.L2_REGULARIZATION.getValue(params), 2, false);
        } else {
            // LightGBM
            appendIntField(config, "num_iterations", Config.NUM_ITERATIONS.getValue(params), 2, true);
            appendIntField(config, "num_leaves", Config.NUM_LEAVES.getValue(params), 2, true);
            appendIntField(config, "max_depth", Config.MAX_DEPTH.getValue(params), 2, true);
            appendDoubleField(config, "learning_rate", Config.LEARNING_RATE.getValue(params), 2, true);
            appendDoubleField(config, "feature_fraction", Config.FEATURE_FRACTION.getValue(params), 2, true);
            appendDoubleField(config, "bagging_fraction", Config.BAGGING_FRACTION.getValue(params), 2, true);
            appendIntField(config, "bagging_freq", Config.BAGGING_FREQ.getValue(params), 2, true);
            appendIntField(config, "min_data_in_leaf", Config.MIN_DATA_IN_LEAF.getValue(params), 2, true);
            appendDoubleField(config, "l2_regularization", Config.L2_REGULARIZATION.getValue(params), 2, false);
        }
        config.append("  }\n");
    }

    /** Merge base params with overrides; overrides take precedence. Returns unmodifiable. */
    public static Map<String, String> mergeParams(Map<String, String> base, Map<String, String> overrides) {
        Map<String, String> merged = new LinkedHashMap<>();
        if (base != null) {
            merged.putAll(base);
        }
        if (overrides != null) {
            merged.putAll(overrides);
        }
        return Collections.unmodifiableMap(merged);
    }

    /** Three-tier precedence: primary if set → fallback if set → primary default. */
    private static int resolveInt(ConfigOption<Integer> primary, ConfigOption<Integer> fallback,
                                  Map<String, String> params) {
        if (primary.isSet(params)) {
            return primary.getValue(params);
        }
        if (fallback.isSet(params)) {
            return fallback.getValue(params);
        }
        return primary.getDefaultValue();
    }

    /** Same three-tier precedence as {@link #resolveInt}. */
    private static double resolveDouble(ConfigOption<Double> primary, ConfigOption<Double> fallback,
                                        Map<String, String> params) {
        if (primary.isSet(params)) {
            return primary.getValue(params);
        }
        if (fallback.isSet(params)) {
            return fallback.getValue(params);
        }
        return primary.getDefaultValue();
    }

    /**
     * Get feature column names, excluding the label column.
     * Unsupported types are rejected by {@link GbdtModelBase#checkModel} beforehand;
     * the type filters here are defensive.
     */
    private static List<String> getFeatureNames(ModelConfig model, ModelType modelType) {
        List<String> features = new ArrayList<>();
        if (model.getInputFields() != null) {
            for (FieldSchema f : model.getInputFields()) {
                if (isLabelColumn(model, f.getName())) {
                    continue;
                }
                // Defensive: skip types that checkModel should have rejected.
                if (isArrayType(f.getType())) {
                    continue;
                }
                if ((modelType == ModelType.LIGHTGBM || modelType == ModelType.XGBOOST) && !isFloatType(f.getType())) {
                    continue;
                }
                features.add(f.getName());
            }
        }
        return features;
    }

    /**
     * Resolve categorical feature names.
     * CatBoost: int/bigint/string → categorical; float/double → numeric.
     * LightGBM: no categoricals (only float features are included).
     */
    private static List<String> resolveCategoricalFeatures(ModelConfig model, ModelType modelType) {
        if (modelType == ModelType.LIGHTGBM || modelType == ModelType.XGBOOST) {
            return Collections.emptyList();
        }

        Set<String> categorical = new LinkedHashSet<>();

        // CatBoost: auto-detect int/bigint/string as categorical.
        if (model.getInputFields() != null) {
            for (FieldSchema f : model.getInputFields()) {
                if (isLabelColumn(model, f.getName())) {
                    continue;
                }
                if (isArrayType(f.getType())) {
                    continue;
                }
                if (isAutoCategoricalCatBoost(f.getType())) {
                    categorical.add(f.getName());
                }
            }
        }

        return new ArrayList<>(categorical);
    }

    /** Returns true for CatBoost auto-categorical types: int, bigint, string. */
    private static boolean isAutoCategoricalCatBoost(String fieldType) {
        if (fieldType == null) {
            return false;
        }
        String lower = fieldType.toLowerCase();
        // float / double → numeric; everything else → categorical
        return !lower.equals("float") && !lower.equals("double");
    }

    /** Returns true for float/double types. */
    private static boolean isFloatType(String fieldType) {
        if (fieldType == null) {
            return false;
        }
        String lower = fieldType.toLowerCase();
        return lower.equals("float") || lower.equals("double");
    }

    /** Returns true for array types (array&lt;...&gt;). */
    private static boolean isArrayType(String fieldType) {
        return fieldType != null && fieldType.toLowerCase().startsWith("array<");
    }

    private static boolean isLabelColumn(ModelConfig model, String name) {
        Map<String, String> params = model.getParams();
        if (params == null) {
            return false;
        }
        String labels = params.get(Config.LABEL_COLUMNS.getKey());
        if (labels == null || labels.isEmpty()) {
            return false;
        }
        for (String l : labels.split(",")) {
            if (l.trim().equals(name)) {
                return true;
            }
        }
        return false;
    }

    private static String joinPaths(List<String> paths) {
        if (paths == null || paths.isEmpty()) {
            return "";
        }
        return String.join(",", paths);
    }

    private static void appendStringField(StringBuilder sb, String key, String value,
                                          int indent, boolean withComma) {
        String ind = indent(indent);
        if (value == null) {
            value = "";
        }
        sb.append(ind).append("\"").append(key).append("\": ")
                .append("\"").append(escapeJson(value)).append("\"");
        if (withComma) {
            sb.append(",");
        }
        sb.append("\n");
    }

    private static void appendIntField(StringBuilder sb, String key, int value,
                                       int indent, boolean withComma) {
        String ind = indent(indent);
        sb.append(ind).append("\"").append(key).append("\": ").append(value);
        if (withComma) {
            sb.append(",");
        }
        sb.append("\n");
    }

    private static void appendDoubleField(StringBuilder sb, String key, double value,
                                          int indent, boolean withComma) {
        String ind = indent(indent);
        sb.append(ind).append("\"").append(key).append("\": ").append(value);
        if (withComma) {
            sb.append(",");
        }
        sb.append("\n");
    }

    private static void appendListField(StringBuilder sb, String key, List<String> values,
                                        int indent, boolean withComma) {
        String ind = indent(indent);
        sb.append(ind).append("\"").append(key).append("\": [");
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append("\"").append(escapeJson(values.get(i))).append("\"");
        }
        sb.append("]");
        if (withComma) {
            sb.append(",");
        }
        sb.append("\n");
    }

    private static String indent(int n) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; i++) {
            sb.append("  ");
        }
        return sb.toString();
    }

    private static String escapeJson(String s) {
        StringBuilder sb = new StringBuilder();
        for (char c : s.toCharArray()) {
            switch (c) {
                case '"':
                    sb.append("\\\"");
                    break;
                case '\\':
                    sb.append("\\\\");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                case '\r':
                    sb.append("\\r");
                    break;
                case '\t':
                    sb.append("\\t");
                    break;
                default:
                    sb.append(c);
            }
        }
        return sb.toString();
    }
}
