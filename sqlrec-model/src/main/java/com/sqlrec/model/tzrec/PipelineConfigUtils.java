package com.sqlrec.model.tzrec;

import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.model.common.ModelConfig;
import com.sqlrec.model.common.ModelExportConf;
import com.sqlrec.model.common.ModelTrainConf;

import java.util.List;
import java.util.Map;

/**
 * Utility class for generating Torch Easy Rec configuration files
 */
public class PipelineConfigUtils {

    public static String generateWideAndDeepTrainConfig(ModelConfig model, ModelTrainConf trainConf) {
        StringBuilder config = new StringBuilder();

        // Add input paths
        addInputPaths(config, trainConf.trainDataPaths);

        // Add model directory
        addModelDir(config, trainConf.modelDir);

        // Add train config
        config.append(generateTrainConfig(model, trainConf.params));

        // Add data config
        config.append(generateDataConfig(model, trainConf.params));

        // Add feature configs
        config.append(generateFeatureConfigs(model));

        // Add model config
        config.append(generateModelConfig(model));

        return config.toString();
    }

    public static String generateWideAndDeepExportConfig(ModelConfig model, ModelExportConf exportConf) {
        StringBuilder config = new StringBuilder();

        // Add input paths
        addInputPaths(config, exportConf.trainDataPaths);

        // Add model directory
        addModelDir(config, exportConf.baseModelDir);

        // Add data config
        config.append(generateDataConfig(model, exportConf.params));

        // Add feature configs
        config.append(generateFeatureConfigs(model));

        // Add model config
        config.append(generateModelConfig(model));

        return config.toString();
    }

    private static void addInputPaths(StringBuilder config, List<String> trainDataPaths) {
        if (trainDataPaths != null && !trainDataPaths.isEmpty()) {
            // Add train input path as comma-separated string
            String trainInputPath = String.join(",", trainDataPaths);
            config.append("train_input_path: \"").append(trainInputPath).append("\"\n");
        }
    }

    private static void addModelDir(StringBuilder config, String modelDir) {
        if (modelDir != null) {
            config.append("model_dir: \"").append(modelDir).append("\"\n");
        }
    }

    public static String generateTrainConfig(ModelConfig model, Map<String, String> params) {
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
        config.append("}\n");
        return config.toString();
    }

    public static String generateDataConfig(ModelConfig model, Map<String, String> params) {
        StringBuilder config = new StringBuilder();
        int batchSize = Config.BATCH_SIZE.getValue(params);
        int numWorkers = Config.NUM_WORKERS.getValue(params);
        String labelFields = Config.LABEL_FIELDS.getValue(model.params);

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

        if (model.fieldSchemas != null) {
            for (FieldSchema fieldSchema : model.fieldSchemas) {
                String featureName = fieldSchema.name;
                String fieldType = fieldSchema.type;

                if (isNumericFeature(fieldType)) {
                    // Generate raw_feature for numeric features
                    config.append("feature_configs {\n");
                    config.append("    raw_feature {\n");
                    config.append("        feature_name: \"").append(featureName).append("\"\n");
                    config.append("        expression: \"item:").append(featureName).append("\"\n");
                    config.append("    }\n");
                    config.append("}\n");
                } else {
                    // Generate id_feature for categorical features
                    int numBuckets = Config.NUM_BUCKETS.getValue(model.params);
                    int hashBucketSize = Config.HASH_BUCKET_SIZE.getValue(model.params);
                    int embeddingDim = Config.EMBEDDING_DIM.getValue(model.params);
                    config.append("feature_configs {\n");
                    config.append("    id_feature {\n");
                    config.append("        feature_name: \"").append(featureName).append("\"\n");
                    config.append("        expression: \"item:").append(featureName).append("\"\n");
                    if (isIntFeature(fieldType)) {
                        config.append("        num_buckets: ").append(numBuckets).append("\n");
                    } else {
                        config.append("        hash_bucket_size: ").append(hashBucketSize).append("\n");
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

        // Add wide feature group
        config.append("    feature_groups {\n");
        config.append("        group_name: \"wide\"\n");
        // Add feature names for wide group
        addFeatureNames(config, getFeatures(model));
        config.append("        group_type: WIDE\n");
        config.append("    }\n");

        // Add deep feature group
        config.append("    feature_groups {\n");
        config.append("        group_name: \"deep\"\n");
        // Add feature names for deep group
        addFeatureNames(config, getFeatures(model));
        config.append("        group_type: DEEP\n");
        config.append("    }\n");

        // Add deepfm configuration
        Map<String, String> params = model.params != null ? model.params : new java.util.HashMap<>();
        String hiddenUnits = Config.HIDDEN_UNITS.getValue(params);
        config.append("    deepfm {\n");
        config.append("        deep {\n");
        config.append("            hidden_units: [" + hiddenUnits + "]\n");
        config.append("        }\n");
        config.append("    }\n");

        // Add metrics
        config.append("    metrics {\n");
        config.append("        auc {}\n");
        config.append("    }\n");

        // Add losses
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
        if (model.fieldSchemas != null) {
            for (FieldSchema fieldSchema : model.fieldSchemas) {
                categoricalFeatures.add(fieldSchema.name);
            }
        }
        return categoricalFeatures;
    }

    private static void addFeatureNames(StringBuilder config, List<String> featureNames) {
        for (String featureName : featureNames) {
            config.append("        feature_names: \"").append(featureName).append("\"\n");
        }
    }
}
