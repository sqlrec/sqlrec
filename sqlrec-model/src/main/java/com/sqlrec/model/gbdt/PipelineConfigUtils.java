package com.sqlrec.model.gbdt;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.sqlrec.common.config.ConfigOption;
import com.sqlrec.common.model.ModelConf;
import com.sqlrec.common.model.ModelExportConf;
import com.sqlrec.common.model.ModelTrainConf;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.model.common.FieldTypeUtils;

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

    private static final Gson GSON = new GsonBuilder()
            .setPrettyPrinting()
            .disableHtmlEscaping()
            .create();

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

    public static String generateTrainConfig(ModelType modelType, ModelConf model, ModelTrainConf trainConf) {
        JsonObject config = new JsonObject();

        config.addProperty("model_type", modelType.getKey());
        config.addProperty("train_input_path", joinPaths(trainConf.getTrainDataPaths()));
        config.addProperty("model_dir", orEmpty(trainConf.getModelDir()));
        config.addProperty("base_model_dir", orEmpty(trainConf.getBaseModelDir()));
        config.addProperty("label_columns", orEmpty(Config.LABEL_COLUMNS.getValue(model.getParams())));

        config.add("feature_columns", toJsonArray(getFeatureNames(model, modelType)));
        config.add("categorical_features", toJsonArray(resolveCategoricalFeatures(model, modelType)));

        Map<String, String> params = mergeParams(model.getParams(), trainConf.getParams());
        config.add("params", buildParams(params, modelType));

        // Trailing newline keeps the ConfigMap YAML block scalar a plain "|" (clip) block.
        return GSON.toJson(config) + "\n";
    }

    public static String generateExportConfig(ModelExportConf exportConf) {
        String baseModelDir = exportConf.getBaseModelDir();
        if (baseModelDir == null || baseModelDir.isEmpty()) {
            throw new IllegalArgumentException("base_model_dir is required for GBDT export");
        }
        String exportDir = baseModelDir + "_export";
        JsonObject config = new JsonObject();
        config.addProperty("base_model_dir", baseModelDir);
        config.addProperty("export_dir", exportDir);
        return GSON.toJson(config) + "\n";
    }

    private static JsonObject buildParams(Map<String, String> params, ModelType modelType) {
        JsonObject json = new JsonObject();
        json.addProperty("objective", Config.OBJECTIVE.getValue(params));
        json.addProperty("metric", Config.METRIC.getValue(params));

        if (modelType == ModelType.CATBOOST) {
            // CatBoost uses its own param names; CB_* overrides generic GBDT params.
            int iterations = resolveInt(Config.CB_ITERATIONS, Config.NUM_ITERATIONS, params);
            int depth = resolveInt(Config.CB_DEPTH, Config.MAX_DEPTH, params);
            double l2LeafReg = resolveDouble(Config.CB_L2_LEAF_REG, Config.L2_REGULARIZATION, params);
            json.addProperty("iterations", iterations);
            json.addProperty("depth", depth);
            json.addProperty("l2_leaf_reg", l2LeafReg);
            json.addProperty("learning_rate", Config.LEARNING_RATE.getValue(params));
        } else if (modelType == ModelType.XGBOOST) {
            // XGBoost params: reuse generic names where they align, but the Python
            // side maps them to XGBoost-native names (e.g. bagging_fraction -> subsample).
            json.addProperty("num_iterations", Config.NUM_ITERATIONS.getValue(params));
            json.addProperty("max_depth", Config.MAX_DEPTH.getValue(params));
            json.addProperty("learning_rate", Config.LEARNING_RATE.getValue(params));
            json.addProperty("feature_fraction", Config.FEATURE_FRACTION.getValue(params));
            json.addProperty("bagging_fraction", Config.BAGGING_FRACTION.getValue(params));
            json.addProperty("min_child_weight", Config.MIN_CHILD_WEIGHT.getValue(params));
            json.addProperty("l2_regularization", Config.L2_REGULARIZATION.getValue(params));
        } else {
            // LightGBM
            json.addProperty("num_iterations", Config.NUM_ITERATIONS.getValue(params));
            json.addProperty("num_leaves", Config.NUM_LEAVES.getValue(params));
            json.addProperty("max_depth", Config.MAX_DEPTH.getValue(params));
            json.addProperty("learning_rate", Config.LEARNING_RATE.getValue(params));
            json.addProperty("feature_fraction", Config.FEATURE_FRACTION.getValue(params));
            json.addProperty("bagging_fraction", Config.BAGGING_FRACTION.getValue(params));
            json.addProperty("bagging_freq", Config.BAGGING_FREQ.getValue(params));
            json.addProperty("min_data_in_leaf", Config.MIN_DATA_IN_LEAF.getValue(params));
            json.addProperty("l2_regularization", Config.L2_REGULARIZATION.getValue(params));
        }
        return json;
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

    /** Three-tier precedence: primary if set -> fallback if set -> primary default. */
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
    private static List<String> getFeatureNames(ModelConf model, ModelType modelType) {
        List<String> features = new ArrayList<>();
        if (model.getInputFields() != null) {
            for (FieldSchema f : model.getInputFields()) {
                if (isLabelColumn(model, f.getName())) {
                    continue;
                }
                // Defensive: skip types that checkModel should have rejected.
                if (FieldTypeUtils.isArray(f.getType())) {
                    continue;
                }
                if ((modelType == ModelType.LIGHTGBM || modelType == ModelType.XGBOOST) && !FieldTypeUtils.isFloat(f.getType())) {
                    continue;
                }
                features.add(f.getName());
            }
        }
        return features;
    }

    /**
     * Resolve categorical feature names.
     * CatBoost: int/bigint/string -> categorical; float/double -> numeric.
     * LightGBM: no categoricals (only float features are included).
     */
    private static List<String> resolveCategoricalFeatures(ModelConf model, ModelType modelType) {
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
                if (FieldTypeUtils.isArray(f.getType())) {
                    continue;
                }
                if (isAutoCategoricalCatBoost(f.getType())) {
                    categorical.add(f.getName());
                }
            }
        }

        return new ArrayList<>(categorical);
    }

    /** Returns true for CatBoost auto-categorical types: anything that is not float/double. */
    private static boolean isAutoCategoricalCatBoost(String fieldType) {
        return fieldType != null && !FieldTypeUtils.isFloat(fieldType);
    }

    private static boolean isLabelColumn(ModelConf model, String name) {
        Map<String, String> params = model.getParams();
        if (params == null) {
            return false;
        }
        return FieldTypeUtils.parseCsvList(params.get(Config.LABEL_COLUMNS.getKey())).contains(name);
    }

    private static String joinPaths(List<String> paths) {
        if (paths == null || paths.isEmpty()) {
            return "";
        }
        return String.join(",", paths);
    }

    private static JsonArray toJsonArray(List<String> values) {
        JsonArray array = new JsonArray();
        for (String value : values) {
            array.add(value);
        }
        return array;
    }

    /** Null string values serialize as {@code ""} (matching the Python side's expectations). */
    private static String orEmpty(String value) {
        return value == null ? "" : value;
    }
}
