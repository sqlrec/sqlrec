package com.sqlrec.model.tzrec;

import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.model.common.ModelConfig;
import com.sqlrec.model.common.ModelTrainConf;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit test for TorchEasyRecConfigUtils
 */
public class PipelineConfigUtilsTest {

    @Test
    public void testGenerateWideAndDeepConfig() {
        // Create test model
        ModelConfig model = new ModelConfig();
        model.modelName = "test_model";

        // Add test field schemas
        List<FieldSchema> fieldSchemas = new ArrayList<>();
        fieldSchemas.add(new FieldSchema("feature1", "float")); // Numeric feature
        fieldSchemas.add(new FieldSchema("feature2", "int"));    // Categorical feature
        fieldSchemas.add(new FieldSchema("feature3", "string")); // Categorical feature
        model.fieldSchemas = fieldSchemas;

        // Add test params
        Map<String, String> modelParams = new HashMap<>();
        modelParams.put("hidden_units", "256,128,64");
        modelParams.put("embedding_dim", "32");
        modelParams.put("num_buckets", "10000000");
        model.params = modelParams;

        // Create test train config
        ModelTrainConf trainConf = new ModelTrainConf();
        trainConf.modelDir = "hdfs://data/test_model_dir";

        // Add test train data paths
        List<String> trainDataPaths = new ArrayList<>();
        trainDataPaths.add("hdfs://project1/tables/data1");
        trainDataPaths.add("hdfs://project2/tables/data2");
        trainConf.trainDataPaths = trainDataPaths;

        // Add test train params
        Map<String, String> trainParams = new HashMap<>();
        trainParams.put("sparse_lr", "0.005");
        trainParams.put("dense_lr", "0.001");
        trainParams.put("num_epochs", "3");
        trainParams.put("num_steps", "200");
        trainParams.put("batch_size", "4096");
        trainParams.put("num_workers", "4");
        trainParams.put("label_fields", "label");
        trainConf.params = trainParams;

        // Generate config
        String config = PipelineConfigUtils.generateWideAndDeepConfig(model, trainConf);

        System.out.println(config);
    }
}
