package com.sqlrec.model.tzrec;

import com.sqlrec.common.model.ModelConfig;
import com.sqlrec.common.model.ModelExportConf;
import com.sqlrec.common.model.ModelTrainConf;
import com.sqlrec.common.schema.FieldSchema;

import java.util.List;
import java.util.Map;

public class PipelineConfigUtils {

    public static String generateWideAndDeepTrainConfig(ModelConfig model, ModelTrainConf trainConf) {
        StringBuilder config = new StringBuilder();

        addInputPaths(config, trainConf.getTrainDataPaths());

        addModelDir(config, trainConf.getModelDir());

        config.append(generateTrainConfig(model, trainConf.getParams(), trainConf.getBaseModelDir()));

        config.append(generateDataConfig(model, trainConf.getParams()));

        config.append(generateFeatureConfigs(model));

        config.append(generateModelConfig(model));

        return config.toString();
    }

    public static String generateWideAndDeepExportConfig(ModelConfig model, ModelExportConf exportConf) {
        StringBuilder config = new StringBuilder();

        addInputPaths(config, exportConf.getTrainDataPaths());

        addModelDir(config, exportConf.getBaseModelDir());

        config.append(generateDataConfig(model, exportConf.getParams()));

        config.append(generateFeatureConfigs(model));

        config.append(generateModelConfig(model));

        return config.toString();
    }

    private static void addInputPaths(StringBuilder config, List<String> trainDataPaths) {
        if (trainDataPaths != null && !trainDataPaths.isEmpty()) {
            String trainInputPath = String.join(",", trainDataPaths);
            config.append("train_input_path: \"").append(trainInputPath).append("\"\n");
        }
    }

    private static void addModelDir(StringBuilder config, String modelDir) {
        if (modelDir != null) {
            config.append("model_dir: \"").append(modelDir).append("\"\n");
        }
    }

    public static String generateTrainConfig(ModelConfig model, Map<String, String> params, String baseModelDir) {
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
        if (baseModelDir != null && !baseModelDir.isEmpty()) {
            config.append("    fine_tune_checkpoint: \"").append(baseModelDir).append("\"\n");
        }
        config.append("}\n");
        return config.toString();
    }

    public static String generateDataConfig(ModelConfig model, Map<String, String> params) {
        StringBuilder config = new StringBuilder();
        int batchSize = Config.BATCH_SIZE.getValue(params);
        int numWorkers = Config.NUM_WORKERS.getValue(params);
        String labelFields = Config.LABEL_COLUMNS.getValue(model.getParams());

        config.append("data_config {\n");
        config.append("    batch_size: " + batchSize + "\n");
        config.append("    dataset_type: ParquetDataset\n");
        config.append("    fg_mode: FG_NORMAL\n");
        config.append("    label_fields: \"" + labelFields + "\"\n");
        config.append("    num_workers: " + numWorkers + "\n");
        config.append("}\n");
        return config.toString();
    }

    public static String generateFeatureConfigs(ModelConfig model) {
        StringBuilder config = new StringBuilder();

        if (model.getInputFields() != null) {
            for (FieldSchema fieldSchema : model.getInputFields()) {
                String featureName = fieldSchema.getName();
                String fieldType = fieldSchema.getType();

                if (isNumericFeature(fieldType)) {
                    // skip numeric feature
//                    config.append("feature_configs {\n");
//                    config.append("    raw_feature {\n");
//                    config.append("        feature_name: \"").append(featureName).append("\"\n");
//                    config.append("        expression: \"item:").append(featureName).append("\"\n");
//                    config.append("    }\n");
//                    config.append("}\n");
                } else {
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
                    config.append("        feature_name: \"").append(featureName).append("\"\n");
                    config.append("        expression: \"item:").append(featureName).append("\"\n");
                    if (isIntFeature(fieldType)) {
                        config.append("        num_buckets: ").append(numBuckets).append("\n");
                    } else {
                        config.append("        hash_bucket_size: ").append(numBuckets).append("\n");
                    }
                    config.append("        embedding_dim: ").append(embeddingDim).append("\n");
                    config.append("    }\n");
                    config.append("}\n");
                }
            }
        }

        return config.toString();
    }

    public static String generateModelConfig(ModelConfig model) {
        StringBuilder config = new StringBuilder();
        config.append("model_config {\n");

        config.append("    feature_groups {\n");
        config.append("        group_name: \"wide\"\n");
        addFeatureNames(config, getFeatures(model));
        config.append("        group_type: WIDE\n");
        config.append("    }\n");

        config.append("    feature_groups {\n");
        config.append("        group_name: \"deep\"\n");
        addFeatureNames(config, getFeatures(model));
        config.append("        group_type: DEEP\n");
        config.append("    }\n");

        String hiddenUnits = Config.HIDDEN_UNITS.getValue(model.getParams());
        config.append("    deepfm {\n");
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

    private static boolean isNumericFeature(String fieldType) {
        return "float".equalsIgnoreCase(fieldType) || "double".equalsIgnoreCase(fieldType);
    }

    private static boolean isIntFeature(String fieldType) {
        return "int".equalsIgnoreCase(fieldType) || "array<int>".equalsIgnoreCase(fieldType) ||
                "bigint".equalsIgnoreCase(fieldType) || "array<bigint>".equalsIgnoreCase(fieldType);
    }

    private static List<String> getFeatures(ModelConfig model) {
        List<String> categoricalFeatures = new java.util.ArrayList<>();
        if (model.getInputFields() != null) {
            for (FieldSchema fieldSchema : model.getInputFields()) {
                if (isNumericFeature(fieldSchema.getType())) {
                    // skip numeric feature
                    continue;
                }
                categoricalFeatures.add(fieldSchema.getName());
            }
        }
        return categoricalFeatures;
    }

    private static void addFeatureNames(StringBuilder config, List<String> featureNames) {
        for (String featureName : featureNames) {
            config.append("        feature_names: \"").append(featureName).append("\"\n");
        }
    }

    public static String generateDSSMTrainConfig(ModelConfig model, ModelTrainConf trainConf) {
        StringBuilder config = new StringBuilder();

        addInputPaths(config, trainConf.getTrainDataPaths());

        addModelDir(config, trainConf.getModelDir());

        config.append(generateTrainConfig(model, trainConf.getParams(), trainConf.getBaseModelDir()));

        config.append(generateDSSMDataConfig(model, trainConf.getParams()));

        config.append(generateFeatureConfigs(model));

        config.append(generateDSSMModelConfig(model));

        return config.toString();
    }

    public static String generateDSSMExportConfig(ModelConfig model, ModelExportConf exportConf) {
        StringBuilder config = new StringBuilder();

        addInputPaths(config, exportConf.getTrainDataPaths());

        addModelDir(config, exportConf.getBaseModelDir());

        config.append(generateDSSMDataConfig(model, exportConf.getParams()));

        config.append(generateFeatureConfigs(model));

        config.append(generateDSSMModelConfig(model));

        return config.toString();
    }

    public static String generateDSSMDataConfig(ModelConfig model, Map<String, String> params) {
        StringBuilder config = new StringBuilder();
        int batchSize = Config.BATCH_SIZE.getValue(params);
        int numWorkers = Config.NUM_WORKERS.getValue(params);
        String labelFields = Config.LABEL_COLUMNS.getValue(model.getParams());

        config.append("data_config {\n");
        config.append("    batch_size: " + batchSize + "\n");
        config.append("    dataset_type: ParquetDataset\n");
        config.append("    fg_mode: FG_NORMAL\n");
        config.append("    label_fields: \"" + labelFields + "\"\n");
        config.append("    num_workers: " + numWorkers + "\n");
        config.append("}\n");
        return config.toString();
    }

    public static String generateDSSMModelConfig(ModelConfig model) {
        StringBuilder config = new StringBuilder();
        config.append("model_config {\n");

        String userFeatures = null;
        if (model.getParams().containsKey(Config.USER_FEATURES.getKey())) {
            userFeatures = model.getParams().get(Config.USER_FEATURES.getKey());
        }
        String itemFeatures = null;
        if (model.getParams().containsKey(Config.ITEM_FEATURES.getKey())) {
            itemFeatures = model.getParams().get(Config.ITEM_FEATURES.getKey());
        }

        List<String> userFeatureList = parseFeatureList(userFeatures);
        List<String> itemFeatureList = parseFeatureList(itemFeatures);

        List<String> allFeatures = getFeatures(model);

        if (userFeatureList.isEmpty() && !itemFeatureList.isEmpty()) {
            userFeatureList = inferRemainingFeatures(allFeatures, itemFeatureList);
        } else if (itemFeatureList.isEmpty() && !userFeatureList.isEmpty()) {
            itemFeatureList = inferRemainingFeatures(allFeatures, userFeatureList);
        }

        if (!userFeatureList.isEmpty()) {
            config.append("    feature_groups {\n");
            config.append("        group_name: \"user\"\n");
            for (String featureName : userFeatureList) {
                config.append("        feature_names: \"").append(featureName).append("\"\n");
            }
            config.append("        group_type: DEEP\n");
            config.append("    }\n");
        }

        if (!itemFeatureList.isEmpty()) {
            config.append("    feature_groups {\n");
            config.append("        group_name: \"item\"\n");
            for (String featureName : itemFeatureList) {
                config.append("        feature_names: \"").append(featureName).append("\"\n");
            }
            config.append("        group_type: DEEP\n");
            config.append("    }\n");
        }

        String userHiddenUnits = Config.USER_HIDDEN_UNITS.getValue(model.getParams());
        String itemHiddenUnits = Config.ITEM_HIDDEN_UNITS.getValue(model.getParams());
        int outputDim = Config.OUTPUT_DIM.getValue(model.getParams());

        config.append("    dssm {\n");
        config.append("        user_tower {\n");
        config.append("            input: 'user'\n");
        config.append("            mlp {\n");
        config.append("                hidden_units: [" + userHiddenUnits + "]\n");
        config.append("            }\n");
        config.append("        }\n");
        config.append("        item_tower {\n");
        config.append("            input: 'item'\n");
        config.append("            mlp {\n");
        config.append("                hidden_units: [" + itemHiddenUnits + "]\n");
        config.append("            }\n");
        config.append("        }\n");
        config.append("        output_dim: " + outputDim + "\n");
        config.append("        in_batch_negative: true\n");
        config.append("    }\n");

        config.append("    metrics {\n");
        config.append("        recall_at_k {\n");
        config.append("            top_k: 1\n");
        config.append("        }\n");
        config.append("    }\n");

        config.append("    metrics {\n");
        config.append("        recall_at_k {\n");
        config.append("            top_k: 5\n");
        config.append("        }\n");
        config.append("    }\n");

        config.append("    losses {\n");
        config.append("        softmax_cross_entropy {}\n");
        config.append("    }\n");

        config.append("}\n");
        return config.toString();
    }

    private static List<String> parseFeatureList(String features) {
        List<String> featureList = new java.util.ArrayList<>();
        if (features != null && !features.isEmpty()) {
            for (String feature : features.split(",")) {
                String trimmed = feature.trim();
                if (!trimmed.isEmpty()) {
                    featureList.add(trimmed);
                }
            }
        }
        return featureList;
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
