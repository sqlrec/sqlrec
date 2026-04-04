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
        ModelConfig model = new ModelConfig();
        model.setModelName("test_model");

        List<FieldSchema> fieldSchemas = new ArrayList<>();
        fieldSchemas.add(new FieldSchema("feature1", "float"));
        fieldSchemas.add(new FieldSchema("feature2", "int"));
        fieldSchemas.add(new FieldSchema("feature3", "string"));
        model.setInputFields(fieldSchemas);

        Map<String, String> modelParams = new HashMap<>();
        modelParams.put("hidden_units", "256,128,64");
        modelParams.put("embedding_dim", "32");
        modelParams.put("num_buckets", "10000000");
        modelParams.put("label_columns", "label");
        model.setParams(modelParams);

        ModelTrainConf trainConf = new ModelTrainConf();
        trainConf.setModelDir("hdfs://data/test_model_dir");

        List<String> trainDataPaths = new ArrayList<>();
        trainDataPaths.add("hdfs://project1/tables/data1");
        trainDataPaths.add("hdfs://project2/tables/data2");
        trainConf.setTrainDataPaths(trainDataPaths);

        Map<String, String> trainParams = new HashMap<>();
        trainParams.put("sparse_lr", "0.005");
        trainParams.put("dense_lr", "0.001");
        trainParams.put("num_epochs", "3");
        trainParams.put("num_steps", "200");
        trainParams.put("batch_size", "4096");
        trainParams.put("num_workers", "4");
        trainConf.setParams(trainParams);

        WideAndDeepModel modelController = new WideAndDeepModel();
        String k8sYaml = modelController.genModelTrainK8sYaml(model, trainConf);
        System.out.println(k8sYaml);
        assertNotNull(k8sYaml);
    }

    @Test
    public void testGenModelExportK8sYaml() {
        ModelConfig model = new ModelConfig();
        model.setModelName("test_model");

        List<FieldSchema> fieldSchemas = new ArrayList<>();
        fieldSchemas.add(new FieldSchema("feature1", "float"));
        fieldSchemas.add(new FieldSchema("feature2", "int"));
        fieldSchemas.add(new FieldSchema("feature3", "string"));
        model.setInputFields(fieldSchemas);

        Map<String, String> modelParams = new HashMap<>();
        modelParams.put("hidden_units", "256,128,64");
        modelParams.put("embedding_dim", "32");
        modelParams.put("num_buckets", "10000000");
        modelParams.put("label_columns", "label");
        model.setParams(modelParams);

        ModelExportConf exportConf = new ModelExportConf();
        exportConf.setBaseModelDir("hdfs://data/test_model_dir");

        Map<String, String> exportParams = new HashMap<>();
        exportParams.put("num_workers", "4");
        exportConf.setParams(exportParams);

        WideAndDeepModel modelController = new WideAndDeepModel();
        String k8sYaml = modelController.genModelExportK8sYaml(model, exportConf);
        System.out.println(k8sYaml);
        assertNotNull(k8sYaml);
    }

    @Test
    public void testGetServiceK8sYaml() {
        ModelConfig model = new ModelConfig();
        model.setModelName("test_model");

        ServiceConfig serviceConf = new ServiceConfig();
        serviceConf.setServiceName("test-service");
        serviceConf.setModelName("test_model");
        serviceConf.setCheckpointName("v1");
        serviceConf.setModelCheckpointDir("/model/checkpoint/v1");

        Map<String, String> params = new HashMap<>();
        params.put("pod_cpu_cores", "4");
        params.put("pod_memory", "16Gi");
        serviceConf.setParams(params);

        WideAndDeepModel modelController = new WideAndDeepModel();
        String k8sYaml = modelController.getServiceK8sYaml(model, serviceConf);
        System.out.println(k8sYaml);
        assertNotNull(k8sYaml);
        assert k8sYaml.contains("Deployment");
        assert k8sYaml.contains("Service");
    }
}
