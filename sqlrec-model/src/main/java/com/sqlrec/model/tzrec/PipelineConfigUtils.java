package com.sqlrec.model.tzrec;

import com.sqlrec.common.model.ModelConf;
import com.sqlrec.common.model.ModelExportConf;
import com.sqlrec.common.model.ModelTrainConf;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.model.common.FieldTypeUtils;
import com.sqlrec.model.common.ModelConfigUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PipelineConfigUtils {

    public static ModelConf effectiveModel(ModelConf model, Map<String, String> overrides) {
        ModelConf effective = new ModelConf();
        effective.setModelName(model.getModelName());
        effective.setInputFields(model.getInputFields());
        effective.setParams(ModelConfigUtils.mergeParams(model.getParams(), overrides));
        return effective;
    }

    public static String quoted(String value) {
        StringBuilder out = new StringBuilder("\"");
        for (char c : value.toCharArray()) {
            if (c == '\\' || c == '"') out.append('\\').append(c);
            else if (c < 32) out.append(String.format("\\%03o", (int) c));
            else out.append(c);
        }
        return out.append('"').toString();
    }

    public static String generateWideAndDeepTrainConfig(ModelConf model, ModelTrainConf trainConf) {
        model = effectiveModel(model, trainConf.getParams());
        StringBuilder config = generateTrainConfigPrefix(model, trainConf);
        config.append(generateModelConfig(model));

        return config.toString();
    }

    public static String generateWideAndDeepExportConfig(ModelConf model, ModelExportConf exportConf) {
        model = effectiveModel(model, exportConf.getParams());
        StringBuilder config = generateExportConfigPrefix(model, exportConf);
        config.append(generateModelConfig(model));

        return config.toString();
    }

    static StringBuilder generateTrainConfigPrefix(ModelConf model, ModelTrainConf trainConf) {
        StringBuilder config = new StringBuilder();

        addInputPaths(config, trainConf.getTrainDataPaths());

        String evalPath = Config.EVAL_INPUT_PATH.getValueOrNull(model.getParams());
        if (evalPath != null && !evalPath.isBlank()) {
            config.append("eval_input_path: ").append(quoted(parquetInputPaths(evalPath))).append("\neval_config {}\n");
        }

        addModelDir(config, trainConf.getModelDir());

        config.append(generateTrainConfig(model, model.getParams(), trainConf.getBaseModelDir()));

        config.append(generateDataConfig(model, model.getParams()));

        config.append(generateFeatureConfigs(model));

        return config;
    }

    static StringBuilder generateExportConfigPrefix(ModelConf model, ModelExportConf exportConf) {
        StringBuilder config = new StringBuilder();

        addInputPaths(config, exportConf.getTrainDataPaths());

        addModelDir(config, exportConf.getBaseModelDir());

        config.append(generateDataConfig(model, model.getParams()));

        config.append(generateFeatureConfigs(model));

        return config;
    }

    private static void addInputPaths(StringBuilder config, List<String> trainDataPaths) {
        if (trainDataPaths != null && !trainDataPaths.isEmpty()) {
            String trainInputPath = parquetInputPaths(String.join(",", trainDataPaths));
            config.append("train_input_path: ").append(quoted(trainInputPath)).append("\n");
        }
    }

    private static String parquetInputPaths(String paths) {
        return Arrays.stream(paths.split(",", -1))
                .map(path -> path.endsWith("/*") ? path + ".parquet" : path)
                .collect(Collectors.joining(","));
    }

    private static void addModelDir(StringBuilder config, String modelDir) {
        if (modelDir != null) {
            config.append("model_dir: ").append(quoted(modelDir)).append("\n");
        }
    }

    public static String generateTrainConfig(ModelConf model, Map<String, String> params, String baseModelDir) {
        params = ModelConfigUtils.mergeParams(model.getParams(), params);
        StringBuilder config = new StringBuilder();
        double sparseLr = Config.SPARSE_LR.getValue(params);
        double denseLr = Config.DENSE_LR.getValue(params);
        int numEpochs = Config.NUM_EPOCHS.getValue(params);

        config.append("train_config {\n");
        config.append("    sparse_optimizer {\n");
        config.append("        adagrad_optimizer {\n");
        config.append("            lr: " + sparseLr + "\n");
        config.append("        }\n");
        config.append("        constant_learning_rate {\n");
        config.append("        }\n");
        config.append("    }\n");
        config.append("    dense_optimizer {\n");
        config.append("        adam_optimizer {\n");
        config.append("            lr: " + denseLr + "\n");
        config.append("        }\n");
        config.append("        constant_learning_rate {\n");
        config.append("        }\n");
        config.append("    }\n");
        config.append("    num_epochs: " + numEpochs + "\n");
        // getValueOrNull: unset + null default returns null (no throw), so mixed
        // precision stays off by default; a non-null default would still apply.
        String mixedPrecision = Config.MIXED_PRECISION.getValueOrNull(params);
        if (mixedPrecision != null && !mixedPrecision.isEmpty()) {
            config.append("    mixed_precision: \"" + mixedPrecision.trim().toUpperCase() + "\"\n");
        }
        if (baseModelDir != null && !baseModelDir.isEmpty()) {
            config.append("    fine_tune_checkpoint: ").append(quoted(baseModelDir)).append("\n");
        }
        config.append("}\n");
        return config.toString();
    }

    public static String generateDataConfig(ModelConf model, Map<String, String> params) {
        params = ModelConfigUtils.mergeParams(model.getParams(), params);
        StringBuilder config = new StringBuilder();
        int batchSize = Config.BATCH_SIZE.getValue(params);
        int numWorkers = Config.NUM_WORKERS.getValue(params);
        String labelFields = Config.LABEL_COLUMNS.getValue(params);

        config.append("data_config {\n");
        config.append("    batch_size: " + batchSize + "\n");
        config.append("    dataset_type: ParquetDataset\n");
        config.append("    fg_mode: FG_NORMAL\n");
        for (String label : FieldTypeUtils.parseCsvList(labelFields)) {
            config.append("    label_fields: ").append(quoted(label)).append("\n");
        }
        config.append("    num_workers: " + numWorkers + "\n");
        config.append("}\n");
        return config.toString();
    }

    public static String generateFeatureConfigs(ModelConf model) {
        StringBuilder config = new StringBuilder();

        if (model.getInputFields() == null) {
            return config.toString();
        }
        for (FieldSchema fieldSchema : FeatureOptions.features(model)) {
            String featureName = fieldSchema.getName();
            String fieldType = fieldSchema.getType();
            if (FeatureOptions.isRaw(fieldType)) {
                String prefix = "column." + featureName + ".";
                Map<String, String> params = model.getParams();
                config.append("feature_configs {\n    raw_feature {\n");
                config.append("        feature_name: ").append(quoted(featureName)).append("\n");
                config.append("        expression: ").append(quoted("item:" + featureName)).append("\n");
                int valueDim = FeatureOptions.valueDim(fieldSchema, params);
                config.append("        value_dim: ").append(valueDim).append("\n");
                if (params.containsKey(prefix + "separator")) config.append("        separator: ").append(quoted(params.get(prefix + "separator"))).append("\n");
                config.append("        default_value: ").append(quoted(FeatureOptions.defaultValue(fieldSchema, params))).append("\n");
                if (params.containsKey(prefix + "normalizer")) {
                    config.append("        normalizer: ").append(quoted(params.get(prefix + "normalizer"))).append("\n");
                }
                List<Float> boundaries = FeatureOptions.boundaries(featureName, params);
                String embedding = params.getOrDefault(prefix + "embedding", "none");
                if (!boundaries.isEmpty()) {
                    config.append("        boundaries: ").append(boundaries).append("\n");
                }
                if (!boundaries.isEmpty() || !embedding.equals("none")) {
                    config.append("        embedding_dim: ").append(FeatureOptions.embeddingDim(featureName, params)).append("\n");
                }
                if (embedding.equals("mlp")) config.append("        mlp {}\n");
                if (embedding.equals("autodis")) {
                    config.append("        autodis { num_channels: ").append(Integer.parseInt(params.getOrDefault(prefix + "autodis.num_channels", "3"))).append(" }\n");
                }
                config.append("    }\n}\n");
                continue;
            }
            int defaultNumBuckets = Config.NUM_BUCKETS.getValue(model.getParams());
            int defaultEmbeddingDim = Config.EMBEDDING_DIM.getValue(model.getParams());

            int numBuckets = defaultNumBuckets;
            String bucketSizeKey = "column." + featureName + ".bucket_size";
            if (model.getParams() != null && model.getParams().containsKey(bucketSizeKey)) {
                numBuckets = Integer.parseInt(model.getParams().get(bucketSizeKey));
            }

            int embeddingDim = defaultEmbeddingDim;
            String embeddingDimKey = "column." + featureName + ".embedding_dim";
            if (model.getParams() != null && model.getParams().containsKey(embeddingDimKey)) {
                embeddingDim = Integer.parseInt(model.getParams().get(embeddingDimKey));
            }

            config.append("feature_configs {\n");
            config.append("    id_feature {\n");
            config.append("        feature_name: ").append(quoted(featureName)).append("\n");
            config.append("        expression: ").append(quoted("item:" + featureName)).append("\n");
            String idDefault = model.getParams().get("column." + featureName + ".default_value");
            if (idDefault != null) config.append("        default_value: ").append(quoted(idDefault)).append("\n");
            String separator = model.getParams().get("column." + featureName + ".separator");
            if (separator != null) config.append("        separator: ").append(quoted(separator)).append("\n");
            if (isIntFeature(fieldType)) {
                config.append("        num_buckets: ").append(numBuckets).append("\n");
            } else {
                config.append("        hash_bucket_size: ").append(numBuckets).append("\n");
            }
            config.append("        embedding_dim: ").append(embeddingDim).append("\n");
            config.append("    }\n");
            config.append("}\n");
        }

        return config.toString();
    }

    public static String generateModelConfig(ModelConf model) {
        StringBuilder config = new StringBuilder();
        config.append("model_config {\n");

        List<String> sparse = FeatureOptions.sparseFeatures(model);
        addFeatureGroup(config, "wide", sparse, "WIDE");

        addFeatureGroup(config, "deep", getFeatures(model), "DEEP");

        String hiddenUnits = Config.HIDDEN_UNITS.getValue(model.getParams());
        boolean deepfm = model.getParams() != null && "tzrec.deepfm".equals(model.getParams().get("model"));
        if (deepfm) addFeatureGroup(config, "fm", sparse, "DEEP");
        config.append(deepfm ? "    deepfm {\n" : "    wide_and_deep {\n");
        config.append("        deep {\n");
        config.append("            hidden_units: [" + hiddenUnits + "]\n");
        config.append("        }\n");
        config.append("    }\n");

        config.append("    metrics {\n");
        config.append("        auc {}\n");
        config.append("    }\n");

        config.append("    losses {\n");
        config.append("        binary_cross_entropy {}\n");
        config.append("    }\n");

        config.append("}\n");
        return config.toString();
    }

    private static boolean isIntFeature(String fieldType) {
        return FieldTypeUtils.isInteger(fieldType);
    }

    private static List<String> getFeatures(ModelConf model) {
        List<String> categoricalFeatures = new java.util.ArrayList<>();
        for (FieldSchema fieldSchema : FeatureOptions.features(model)) {
            categoricalFeatures.add(fieldSchema.getName());
        }
        return categoricalFeatures;
    }

    private static void addFeatureNames(StringBuilder config, List<String> featureNames) {
        for (String featureName : featureNames) {
            config.append("        feature_names: ").append(quoted(featureName)).append("\n");
        }
    }

    public static String generateDSSMTrainConfig(ModelConf model, ModelTrainConf trainConf) {
        model = effectiveModel(model, trainConf.getParams());
        StringBuilder config = generateTrainConfigPrefix(model, trainConf);
        config.append(generateDSSMModelConfig(model));

        return config.toString();
    }

    public static String generateDSSMExportConfig(ModelConf model, ModelExportConf exportConf) {
        model = effectiveModel(model, exportConf.getParams());
        StringBuilder config = generateExportConfigPrefix(model, exportConf);
        config.append(generateDSSMModelConfig(model));

        return config.toString();
    }

    public static String generateDSSMModelConfig(ModelConf model) {
        StringBuilder config = new StringBuilder();
        config.append("model_config {\n");

        Map<String, String> params = model.getParams();
        String userFeatures = params != null ? params.get(Config.USER_FEATURES.getKey()) : null;
        String itemFeatures = params != null ? params.get(Config.ITEM_FEATURES.getKey()) : null;

        List<String> userFeatureList = FieldTypeUtils.parseCsvList(userFeatures);
        List<String> itemFeatureList = FieldTypeUtils.parseCsvList(itemFeatures);

        List<String> allFeatures = getFeatures(model);

        if (userFeatureList.isEmpty() && !itemFeatureList.isEmpty()) {
            userFeatureList = inferRemainingFeatures(allFeatures, itemFeatureList);
        } else if (itemFeatureList.isEmpty() && !userFeatureList.isEmpty()) {
            itemFeatureList = inferRemainingFeatures(allFeatures, userFeatureList);
        }

        if (!userFeatureList.isEmpty()) {
            addFeatureGroup(config, "user", userFeatureList, "DEEP");
        }

        if (!itemFeatureList.isEmpty()) {
            addFeatureGroup(config, "item", itemFeatureList, "DEEP");
        }

        String userHiddenUnits = Config.USER_HIDDEN_UNITS.getValue(model.getParams());
        String itemHiddenUnits = Config.ITEM_HIDDEN_UNITS.getValue(model.getParams());
        int outputDim = Config.OUTPUT_DIM.getValue(model.getParams());

        config.append("    dssm {\n");
        addDssmTower(config, "user", userHiddenUnits);
        addDssmTower(config, "item", itemHiddenUnits);
        config.append("        output_dim: " + outputDim + "\n");
        config.append("        in_batch_negative: true\n");
        config.append("    }\n");

        addRecallMetric(config, 1);

        addRecallMetric(config, 5);

        config.append("    losses {\n");
        config.append("        softmax_cross_entropy {}\n");
        config.append("    }\n");

        config.append("}\n");
        return config.toString();
    }

    static void addFeatureGroup(
            StringBuilder config, String name, List<String> features, String type) {
        config.append("    feature_groups {\n");
        config.append("        group_name: \"").append(name).append("\"\n");
        addFeatureNames(config, features);
        config.append("        group_type: ").append(type).append("\n");
        config.append("    }\n");
    }

    private static void addDssmTower(StringBuilder config, String name, String hiddenUnits) {
        config.append("        ").append(name).append("_tower {\n");
        config.append("            input: '").append(name).append("'\n");
        config.append("            mlp {\n");
        config.append("                hidden_units: [").append(hiddenUnits).append("]\n");
        config.append("            }\n");
        config.append("        }\n");
    }

    private static void addRecallMetric(StringBuilder config, int topK) {
        config.append("    metrics {\n");
        config.append("        recall_at_k {\n");
        config.append("            top_k: ").append(topK).append("\n");
        config.append("        }\n");
        config.append("    }\n");
    }

    private static List<String> inferRemainingFeatures(List<String> allFeatures, List<String> specifiedFeatures) {
        List<String> remaining = new java.util.ArrayList<>();
        for (String feature : allFeatures) {
            if (!specifiedFeatures.contains(feature)) {
                remaining.add(feature);
            }
        }
        return remaining;
    }
}
