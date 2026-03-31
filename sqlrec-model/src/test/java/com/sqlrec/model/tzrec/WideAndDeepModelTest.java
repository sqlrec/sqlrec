package com.sqlrec.model.tzrec;

import com.sqlrec.common.model.ModelConfig;
import com.sqlrec.common.model.ModelExportConf;
import com.sqlrec.common.model.ModelTrainConf;
import com.sqlrec.common.model.ServiceConfig;
import com.sqlrec.common.schema.FieldSchema;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class WideAndDeepModelTest {

    @Test
    public void testGenModelTrainK8sYaml() {
        // Create test model
        ModelConfig model = new ModelConfig();
        model.modelName = "test_model";

        // Add test field schemas
        List<FieldSchema> fieldSchemas = new ArrayList<>();
        fieldSchemas.add(new FieldSchema("feature1", "float")); // Numeric feature
        fieldSchemas.add(new FieldSchema("feature2", "int"));    // Categorical feature
        fieldSchemas.add(new FieldSchema("feature3", "string")); // Categorical feature
        model.inputFields = fieldSchemas;

        // Add test params
        Map<String, String> modelParams = new HashMap<>();
        modelParams.put("hidden_units", "256,128,64");
        modelParams.put("embedding_dim", "32");
        modelParams.put("num_buckets", "10000000");
        modelParams.put("label_columns", "label");
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
        trainConf.params = trainParams;

        // Generate config
        WideAndDeepModel modelController = new WideAndDeepModel();
        String k8sYaml = modelController.genModelTrainK8sYaml(model, trainConf);
        System.out.println(k8sYaml);
        assertNotNull(k8sYaml);
    }

    @Test
    public void testGenModelExportK8sYaml() {
        // Create test model
        ModelConfig model = new ModelConfig();
        model.modelName = "test_model";

        // Add test field schemas
        List<FieldSchema> fieldSchemas = new ArrayList<>();
        fieldSchemas.add(new FieldSchema("feature1", "float")); // Numeric feature
        fieldSchemas.add(new FieldSchema("feature2", "int"));    // Categorical feature
        fieldSchemas.add(new FieldSchema("feature3", "string")); // Categorical feature
        model.inputFields = fieldSchemas;

        // Add test params
        Map<String, String> modelParams = new HashMap<>();
        modelParams.put("hidden_units", "256,128,64");
        modelParams.put("embedding_dim", "32");
        modelParams.put("num_buckets", "10000000");
        modelParams.put("label_columns", "label");
        model.params = modelParams;

        // Create test export config
        ModelExportConf exportConf = new ModelExportConf();
        exportConf.baseModelDir = "hdfs://data/test_model_dir";

        // Add test train params
        Map<String, String> exportParams = new HashMap<>();
        exportParams.put("num_workers", "4");
        exportConf.params = exportParams;

        // Generate config
        WideAndDeepModel modelController = new WideAndDeepModel();
        String k8sYaml = modelController.genModelExportK8sYaml(model, exportConf);
        System.out.println(k8sYaml);
        assertNotNull(k8sYaml);
    }

    @Test
    public void testGetServiceK8sYaml() {
        ModelConfig model = new ModelConfig();
        model.modelName = "test_model";

        ServiceConfig serviceConf = new ServiceConfig();
        serviceConf.serviceName = "test-service";
        serviceConf.modelName = "test_model";
        serviceConf.checkpointName = "v1";
        serviceConf.modelCheckpointDir = "/model/checkpoint/v1";

        Map<String, String> params = new HashMap<>();
        params.put("pod_cpu_cores", "4");
        params.put("pod_memory", "16Gi");
        serviceConf.params = params;

        WideAndDeepModel modelController = new WideAndDeepModel();
        String k8sYaml = modelController.getServiceK8sYaml(model, serviceConf);
        System.out.println(k8sYaml);
        assertNotNull(k8sYaml);
        assert k8sYaml.contains("Deployment");
        assert k8sYaml.contains("Service");
    }
}
