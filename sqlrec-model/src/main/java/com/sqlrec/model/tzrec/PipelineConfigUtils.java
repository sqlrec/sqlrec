package com.sqlrec.model.tzrec;

import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.model.common.Model;
import com.sqlrec.model.common.ModelTrainConf;

import java.util.List;
import java.util.Map;

/**
 * Utility class for generating Torch Easy Rec configuration files
 */
public class PipelineConfigUtils {

    public static String generateWideAndDeepConfig(Model model, ModelTrainConf trainConf) {
        StringBuilder config = new StringBuilder();

        // Add input paths
        addInputPaths(config, trainConf);

        // Add model directory
        addModelDir(config, trainConf);

        // Add train config
        config.append(generateTrainConfig(model, trainConf));

        // Add data config
        config.append(generateDataConfig(model, trainConf));

        // Add feature configs
        config.append(generateFeatureConfigs(model));

        // Add model config
        config.append(generateModelConfig(model));

        return config.toString();
    }

    private static void addInputPaths(StringBuilder config, ModelTrainConf trainConf) {
        List<String> trainDataPaths = trainConf.trainDataPaths;
        if (trainDataPaths != null && !trainDataPaths.isEmpty()) {
            // Add train input path as comma-separated string
            String trainInputPath = String.join(",", trainDataPaths);
            config.append("train_input_path: \"").append(trainInputPath).append("\"\n");
        }
    }

    private static void addModelDir(StringBuilder config, ModelTrainConf trainConf) {
        if (trainConf.modelDir != null) {
            config.append("model_dir: \"").append(trainConf.modelDir).append("\"\n");
        }
    }

    public static String generateTrainConfig(Model model, ModelTrainConf trainConf) {
        StringBuilder config = new StringBuilder();
        double sparseLr = Config.SPARSE_LR.getValue(trainConf.params);
        double denseLr = Config.DENSE_LR.getValue(trainConf.params);
        int numEpochs = Config.NUM_EPOCHS.getValue(trainConf.params);

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

    public static String generateDataConfig(Model model, ModelTrainConf trainConf) {
        StringBuilder config = new StringBuilder();
        int batchSize = Config.BATCH_SIZE.getValue(trainConf.params);
        int numWorkers = Config.NUM_WORKERS.getValue(trainConf.params);
        String labelFields = Config.LABEL_FIELDS.getValue(trainConf.params);

        config.append("data_config {\n");
        config.append("    batch_size: " + batchSize + "\n");
        config.append("    dataset_type: ParquetDataset\n");
        config.append("    fg_mode: FG_NORMAL\n");
        config.append("    label_fields: \"" + labelFields + "\"\n");
        config.append("    num_workers: " + numWorkers + "\n");
        config.append("}\n");
        return config.toString();
    }

    public static String generateFeatureConfigs(Model model) {
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
                    int embeddingDim = Config.EMBEDDING_DIM.getValue(model.params);
                    config.append("feature_configs {\n");
                    config.append("    id_feature {\n");
                    config.append("        feature_name: \"").append(featureName).append("\"\n");
                    config.append("        expression: \"item:").append(featureName).append("\"\n");
                    config.append("        num_buckets: ").append(numBuckets).append("\n");
                    config.append("        embedding_dim: ").append(embeddingDim).append("\n");
                    config.append("    }\n");
                    config.append("}\n");
                }
            }
        }

        return config.toString();
    }

    public static String generateModelConfig(Model model) {
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

    private static List<String> getFeatures(Model model) {
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
