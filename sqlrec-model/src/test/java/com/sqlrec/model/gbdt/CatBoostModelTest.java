package com.sqlrec.model.gbdt;

import com.sqlrec.common.model.ModelConf;
import com.sqlrec.common.model.ModelExportConf;
import com.sqlrec.common.model.ModelTrainConf;
import com.sqlrec.common.model.ServiceConf;
import com.sqlrec.common.schema.FieldSchema;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class CatBoostModelTest {

    @Test
    public void testGetModelName() {
        CatBoostModel modelController = new CatBoostModel();
        assertEquals("gbdt.catboost", modelController.getModelName());
    }

    @Test
    public void testGetOutputFields() {
        CatBoostModel modelController = new CatBoostModel();
        ModelConf model = new ModelConf();

        List<FieldSchema> outputFields = modelController.getOutputFields(model);

        assertNotNull(outputFields);
        assertEquals(1, outputFields.size());
        assertEquals("probs", outputFields.get(0).getName());
        assertEquals("FLOAT", outputFields.get(0).getType());
    }

    @Test
    public void testCheckModelWithoutLabelColumns() {
        CatBoostModel modelController = new CatBoostModel();
        ModelConf model = new ModelConf();

        String result = modelController.checkModel(model);
        assertEquals("label_columns is required for GBDT model", result);
    }

    @Test
    public void testCheckModelWithLabelColumns() {
        CatBoostModel modelController = new CatBoostModel();
        ModelConf model = new ModelConf();
        List<FieldSchema> fields = new ArrayList<>();
        fields.add(new FieldSchema("age", "int"));
        fields.add(new FieldSchema("label", "int"));
        model.setInputFields(fields);
        Map<String, String> params = new HashMap<>();
        params.put("label_columns", "label");
        model.setParams(params);

        String result = modelController.checkModel(model);
        assertNull(result);
    }

    @Test
    public void testCheckModelRejectsArrayFeature() {
        CatBoostModel modelController = new CatBoostModel();
        ModelConf model = new ModelConf();
        List<FieldSchema> fields = new ArrayList<>();
        fields.add(new FieldSchema("embed", "array<float>"));
        fields.add(new FieldSchema("age", "int"));
        fields.add(new FieldSchema("label", "int"));
        model.setInputFields(fields);
        Map<String, String> params = new HashMap<>();
        params.put("label_columns", "label");
        model.setParams(params);

        String result = modelController.checkModel(model);
        assertNotNull(result);
        assertTrue(result.contains("array-type features"));
        assertTrue(result.contains("embed"));
    }

    @Test
    public void testCheckModelAcceptsMixedTypes() {
        // CatBoost should accept int, bigint, string, float, double features.
        CatBoostModel modelController = new CatBoostModel();
        ModelConf model = new ModelConf();
        List<FieldSchema> fields = new ArrayList<>();
        fields.add(new FieldSchema("age", "int"));
        fields.add(new FieldSchema("user_id", "bigint"));
        fields.add(new FieldSchema("country", "string"));
        fields.add(new FieldSchema("score", "float"));
        fields.add(new FieldSchema("weight", "double"));
        fields.add(new FieldSchema("label", "int"));
        model.setInputFields(fields);
        Map<String, String> params = new HashMap<>();
        params.put("label_columns", "label");
        model.setParams(params);

        String result = modelController.checkModel(model);
        assertNull(result);
    }

    @Test
    public void testGetExportCheckpoints() {
        CatBoostModel modelController = new CatBoostModel();
        ModelExportConf exportConf = new ModelExportConf();
        exportConf.setCheckpointName("v1");

        List<String> checkpoints = modelController.getExportCheckpoints(exportConf);

        assertNotNull(checkpoints);
        assertEquals(1, checkpoints.size());
        assertEquals("v1_export", checkpoints.get(0));
    }

    @Test
    public void testGetServiceUrl() {
        CatBoostModel modelController = new CatBoostModel();
        ModelConf model = new ModelConf();
        ServiceConf serviceConf = new ServiceConf();
        serviceConf.setId("cb-service-id");

        Map<String, String> params = new HashMap<>();
        params.put("NAMESPACE", "default");
        serviceConf.setParams(params);

        String url = modelController.getServiceUrl(model, serviceConf);
        assertEquals("http://cb-service-id.default.svc.cluster.local:80/predict", url);
    }

    @Test
    public void testGenModelTrainK8sYaml() {
        ModelConf model = new ModelConf();
        model.setModelName("cb_test_model");

        List<FieldSchema> fieldSchemas = new ArrayList<>();
        fieldSchemas.add(new FieldSchema("feature1", "int"));
        fieldSchemas.add(new FieldSchema("feature2", "string"));
        fieldSchemas.add(new FieldSchema("label", "int"));
        model.setInputFields(fieldSchemas);

        Map<String, String> modelParams = new HashMap<>();
        modelParams.put("label_columns", "label");
        model.setParams(modelParams);

        ModelTrainConf trainConf = new ModelTrainConf();
        trainConf.setId("cb-train-job-123");
        trainConf.setModelDir("hdfs://data/cb_test_model_dir");

        List<String> trainDataPaths = new ArrayList<>();
        trainDataPaths.add("hdfs://project1/tables/data1");
        trainConf.setTrainDataPaths(trainDataPaths);

        CatBoostModel modelController = new CatBoostModel();
        String k8sYaml = modelController.genModelTrainK8sYaml(model, trainConf);

        String expectedYaml = """
---
apiVersion: "v1"
kind: "ConfigMap"
metadata:
  name: "cb-train-job-123-cm"
data:
  pipeline.config: |
    {
      "model_type": "catboost",
      "train_input_path": "hdfs://project1/tables/data1",
      "model_dir": "hdfs://data/cb_test_model_dir",
      "base_model_dir": "",
      "label_columns": "label",
      "feature_columns": [
        "feature1",
        "feature2"
      ],
      "categorical_features": [
        "feature1",
        "feature2"
      ],
      "params": {
        "objective": "binary",
        "metric": "auc",
        "iterations": 1000,
        "depth": 6,
        "l2_leaf_reg": 3.0,
        "learning_rate": 0.1
      }
    }
  start.sh: |-
    #!/bin/bash
    set -ex
    export PYTHONPATH=/app:$PYTHONPATH

    exec python -m gbdt.train_catboost \\
        --pipeline_config_path /data/pipeline.config
---
apiVersion: "batch/v1"
kind: "Job"
metadata:
  name: "cb-train-job-123-job"
spec:
  backoffLimit: 1
  template:
    spec:
      containers:
      - command:
        - "bash"
        - "/data/start.sh"
        image: "sqlrec/gbdt:0.1.0-cpu"
        name: "gbdt-job"
        resources:
          requests:
            cpu: "1"
            memory: "2Gi"
        volumeMounts:
        - mountPath: "/data"
          name: "config-volume"
      restartPolicy: "Never"
      volumes:
      - configMap:
          name: "cb-train-job-123-cm"
        name: "config-volume"
""";
        assertEquals(expectedYaml, k8sYaml);
    }

    @Test
    public void testGenModelTrainK8sYamlWithCategoricalFeatures() {
        ModelConf model = new ModelConf();
        model.setModelName("cb_cat_model");

        List<FieldSchema> fieldSchemas = new ArrayList<>();
        fieldSchemas.add(new FieldSchema("user_id", "bigint"));
        fieldSchemas.add(new FieldSchema("user_country", "string"));
        fieldSchemas.add(new FieldSchema("age", "int"));
        fieldSchemas.add(new FieldSchema("label", "int"));
        model.setInputFields(fieldSchemas);

        Map<String, String> modelParams = new HashMap<>();
        modelParams.put("label_columns", "label");
        model.setParams(modelParams);

        ModelTrainConf trainConf = new ModelTrainConf();
        trainConf.setId("cb-cat-train-job");
        trainConf.setModelDir("hdfs://data/cb_cat_model_dir");

        List<String> trainDataPaths = new ArrayList<>();
        trainDataPaths.add("hdfs://data/train");
        trainConf.setTrainDataPaths(trainDataPaths);

        CatBoostModel modelController = new CatBoostModel();
        String k8sYaml = modelController.genModelTrainK8sYaml(model, trainConf);

        // All three features (bigint, string, int) are auto-detected as categorical.
        String expectedList = "[\n        \"user_id\",\n        \"user_country\",\n        \"age\"\n      ]";
        assertTrue(k8sYaml.contains("\"categorical_features\": " + expectedList));
        assertTrue(k8sYaml.contains("\"feature_columns\": " + expectedList));
    }

    @Test
    public void testGenModelExportK8sYaml() {
        ModelConf model = new ModelConf();
        model.setModelName("cb_test_model");

        List<FieldSchema> fieldSchemas = new ArrayList<>();
        fieldSchemas.add(new FieldSchema("feature1", "int"));
        fieldSchemas.add(new FieldSchema("feature2", "string"));
        fieldSchemas.add(new FieldSchema("label", "int"));
        model.setInputFields(fieldSchemas);

        Map<String, String> modelParams = new HashMap<>();
        modelParams.put("label_columns", "label");
        model.setParams(modelParams);

        ModelExportConf exportConf = new ModelExportConf();
        exportConf.setId("cb-export-job-456");
        exportConf.setBaseModelDir("hdfs://data/cb_test_model_dir");
        exportConf.setCheckpointName("v1");

        CatBoostModel modelController = new CatBoostModel();
        String k8sYaml = modelController.genModelExportK8sYaml(model, exportConf);

        String expectedYaml = """
---
apiVersion: "v1"
kind: "ConfigMap"
metadata:
  name: "cb-export-job-456-cm"
data:
  pipeline.config: |
    {
      "base_model_dir": "hdfs://data/cb_test_model_dir",
      "export_dir": "hdfs://data/cb_test_model_dir_export"
    }
  start.sh: |-
    #!/bin/bash
    set -ex
    export PYTHONPATH=/app:$PYTHONPATH

    exec python -m gbdt.export_catboost \\
        --pipeline_config_path /data/pipeline.config
---
apiVersion: "batch/v1"
kind: "Job"
metadata:
  name: "cb-export-job-456-job"
spec:
  backoffLimit: 1
  template:
    spec:
      containers:
      - command:
        - "bash"
        - "/data/start.sh"
        image: "sqlrec/gbdt:0.1.0-cpu"
        name: "gbdt-job"
        resources:
          requests:
            cpu: "1"
            memory: "2Gi"
        volumeMounts:
        - mountPath: "/data"
          name: "config-volume"
      restartPolicy: "Never"
      volumes:
      - configMap:
          name: "cb-export-job-456-cm"
        name: "config-volume"
""";
        assertEquals(expectedYaml, k8sYaml);
    }

    @Test
    public void testGetServiceK8sYaml() {
        ModelConf model = new ModelConf();
        model.setModelName("cb_test_model");

        ServiceConf serviceConf = new ServiceConf();
        serviceConf.setId("cb-service-id");
        serviceConf.setServiceName("cb-service");
        serviceConf.setModelName("cb_test_model");
        serviceConf.setCheckpointName("v1");
        serviceConf.setModelCheckpointDir("/model/checkpoint/v1");

        Map<String, String> params = new HashMap<>();
        params.put("pod_cpu_cores", "4");
        params.put("pod_memory", "16Gi");
        params.put("pod_cpu_limit", "8");
        params.put("pod_memory_limit", "32Gi");
        params.put("replicas", "3");
        serviceConf.setParams(params);

        CatBoostModel modelController = new CatBoostModel();
        String k8sYaml = modelController.getServiceK8sYaml(model, serviceConf);

        // Serving now uses bash -c with an inline script that downloads model
        // from HDFS and runs the C++ catboost_server binary (not server.sh).
        assertTrue(k8sYaml.contains("catboost_server"), "YAML should contain catboost_server binary");
        assertFalse(k8sYaml.contains("/app/server.sh"), "YAML should not contain old server.sh path");
        assertTrue(k8sYaml.contains("cb-service-id"));
        assertTrue(k8sYaml.contains("replicas: 3"));
        assertTrue(k8sYaml.contains("/model/checkpoint/v1"));
    }

    @Test
    public void testGetServiceK8sYamlWithDefaultParams() {
        ModelConf model = new ModelConf();
        model.setModelName("cb_test_model");

        ServiceConf serviceConf = new ServiceConf();
        serviceConf.setId("cb-default-service-id");
        serviceConf.setModelCheckpointDir("/model/checkpoint/default");

        CatBoostModel modelController = new CatBoostModel();
        String k8sYaml = modelController.getServiceK8sYaml(model, serviceConf);

        // Serving now uses bash -c with an inline script that downloads model
        // from HDFS and runs the C++ catboost_server binary (not server.sh).
        assertTrue(k8sYaml.contains("catboost_server"), "YAML should contain catboost_server binary");
        assertFalse(k8sYaml.contains("/app/server.sh"), "YAML should not contain old server.sh path");
        assertTrue(k8sYaml.contains("cb-default-service-id"));
        assertTrue(k8sYaml.contains("replicas: 1"));
        assertTrue(k8sYaml.contains("/model/checkpoint/default"));
    }
}
