package com.sqlrec.model.gbdt;

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

import static org.junit.jupiter.api.Assertions.*;

public class LightGBMModelTest {

    @Test
    public void testGetModelName() {
        LightGBMModel modelController = new LightGBMModel();
        assertEquals("gbdt.lightgbm", modelController.getModelName());
    }

    @Test
    public void testGetOutputFields() {
        LightGBMModel modelController = new LightGBMModel();
        ModelConfig model = new ModelConfig();

        List<FieldSchema> outputFields = modelController.getOutputFields(model);

        assertNotNull(outputFields);
        assertEquals(1, outputFields.size());
        assertEquals("probs", outputFields.get(0).getName());
        assertEquals("FLOAT", outputFields.get(0).getType());
    }

    @Test
    public void testCheckModelWithoutLabelColumns() {
        LightGBMModel modelController = new LightGBMModel();
        ModelConfig model = new ModelConfig();

        String result = modelController.checkModel(model);
        assertEquals("label_columns is required for GBDT model", result);
    }

    @Test
    public void testCheckModelWithEmptyLabelColumns() {
        LightGBMModel modelController = new LightGBMModel();
        ModelConfig model = new ModelConfig();
        Map<String, String> params = new HashMap<>();
        params.put("label_columns", "");
        model.setParams(params);

        String result = modelController.checkModel(model);
        assertEquals("label_columns is required for GBDT model", result);
    }

    @Test
    public void testCheckModelWithLabelColumns() {
        LightGBMModel modelController = new LightGBMModel();
        ModelConfig model = new ModelConfig();
        List<FieldSchema> fields = new ArrayList<>();
        fields.add(new FieldSchema("score", "double"));
        fields.add(new FieldSchema("label", "int"));
        model.setInputFields(fields);
        Map<String, String> params = new HashMap<>();
        params.put("label_columns", "label");
        model.setParams(params);

        String result = modelController.checkModel(model);
        assertNull(result);
    }

    @Test
    public void testCheckModelRejectsIntFeature() {
        LightGBMModel modelController = new LightGBMModel();
        ModelConfig model = new ModelConfig();
        List<FieldSchema> fields = new ArrayList<>();
        fields.add(new FieldSchema("age", "int"));
        fields.add(new FieldSchema("label", "int"));
        model.setInputFields(fields);
        Map<String, String> params = new HashMap<>();
        params.put("label_columns", "label");
        model.setParams(params);

        String result = modelController.checkModel(model);
        assertNotNull(result);
        assertTrue(result.contains("only supports float/double features"));
        assertTrue(result.contains("age"));
    }

    @Test
    public void testCheckModelRejectsArrayFeature() {
        LightGBMModel modelController = new LightGBMModel();
        ModelConfig model = new ModelConfig();
        List<FieldSchema> fields = new ArrayList<>();
        fields.add(new FieldSchema("embed", "array<float>"));
        fields.add(new FieldSchema("score", "double"));
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
    public void testGetExportCheckpoints() {
        LightGBMModel modelController = new LightGBMModel();
        ModelExportConf exportConf = new ModelExportConf();
        exportConf.setCheckpointName("v1");

        List<String> checkpoints = modelController.getExportCheckpoints(exportConf);

        assertNotNull(checkpoints);
        assertEquals(1, checkpoints.size());
        assertEquals("v1_export", checkpoints.get(0));
    }

    @Test
    public void testGetExportCleanPath() {
        LightGBMModel modelController = new LightGBMModel();
        ModelExportConf exportConf = new ModelExportConf();
        assertNull(modelController.getExportCleanPath(exportConf));
    }

    @Test
    public void testGetServiceUrl() {
        LightGBMModel modelController = new LightGBMModel();
        ModelConfig model = new ModelConfig();
        ServiceConfig serviceConf = new ServiceConfig();
        serviceConf.setId("test-service-id");

        Map<String, String> params = new HashMap<>();
        params.put("NAMESPACE", "default");
        serviceConf.setParams(params);

        String url = modelController.getServiceUrl(model, serviceConf);
        assertEquals("http://test-service-id.default.svc.cluster.local:80/predict", url);
    }

    @Test
    public void testGetServiceUrlWithCustomNamespace() {
        LightGBMModel modelController = new LightGBMModel();
        ModelConfig model = new ModelConfig();
        ServiceConfig serviceConf = new ServiceConfig();
        serviceConf.setId("my-service");

        Map<String, String> params = new HashMap<>();
        params.put("NAMESPACE", "custom-namespace");
        serviceConf.setParams(params);

        String url = modelController.getServiceUrl(model, serviceConf);
        assertEquals("http://my-service.custom-namespace.svc.cluster.local:80/predict", url);
    }

    @Test
    public void testGenModelTrainK8sYaml() {
        ModelConfig model = new ModelConfig();
        model.setModelName("test_model");

        List<FieldSchema> fieldSchemas = new ArrayList<>();
        fieldSchemas.add(new FieldSchema("feature1", "float"));
        fieldSchemas.add(new FieldSchema("feature2", "double"));
        fieldSchemas.add(new FieldSchema("label", "int"));
        model.setInputFields(fieldSchemas);

        Map<String, String> modelParams = new HashMap<>();
        modelParams.put("label_columns", "label");
        model.setParams(modelParams);

        ModelTrainConf trainConf = new ModelTrainConf();
        trainConf.setId("train-job-123");
        trainConf.setModelDir("hdfs://data/test_model_dir");

        List<String> trainDataPaths = new ArrayList<>();
        trainDataPaths.add("hdfs://project1/tables/data1");
        trainConf.setTrainDataPaths(trainDataPaths);

        LightGBMModel modelController = new LightGBMModel();
        String k8sYaml = modelController.genModelTrainK8sYaml(model, trainConf);

        String expectedYaml = """
---
apiVersion: "v1"
kind: "ConfigMap"
metadata:
  name: "train-job-123-cm"
data:
  pipeline.config: |
    {
      "model_type": "lightgbm",
      "train_input_path": "hdfs://project1/tables/data1",
      "model_dir": "hdfs://data/test_model_dir",
      "base_model_dir": "",
      "label_columns": "label",
      "feature_columns": ["feature1", "feature2"],
      "categorical_features": [],
      "params": {
        "objective": "binary",
        "metric": "auc",
        "num_iterations": 300,
        "num_leaves": 63,
        "max_depth": 6,
        "learning_rate": 0.1,
        "feature_fraction": 0.8,
        "bagging_fraction": 0.8,
        "bagging_freq": 5,
        "min_data_in_leaf": 20,
        "l2_regularization": 1.0
      }
    }
  start.sh: |-
    #!/bin/bash
    set -ex
    export PYTHONPATH=/app:$PYTHONPATH

    exec python -m gbdt.train_lightgbm \\
        --pipeline_config_path /data/pipeline.config
---
apiVersion: "batch/v1"
kind: "Job"
metadata:
  name: "train-job-123-job"
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
          name: "train-job-123-cm"
        name: "config-volume"
""";
        assertEquals(expectedYaml, k8sYaml);
    }

    @Test
    public void testGenModelTrainK8sYamlWithCategoricalFeatures() {
        ModelConfig model = new ModelConfig();
        model.setModelName("cat_model");

        // LightGBM only accepts float/double features; int/string/array are excluded.
        List<FieldSchema> fieldSchemas = new ArrayList<>();
        fieldSchemas.add(new FieldSchema("f1", "float"));
        fieldSchemas.add(new FieldSchema("f2", "double"));
        fieldSchemas.add(new FieldSchema("excluded_int", "int"));   // excluded
        fieldSchemas.add(new FieldSchema("excluded_str", "string")); // excluded
        fieldSchemas.add(new FieldSchema("label", "int"));
        model.setInputFields(fieldSchemas);

        Map<String, String> modelParams = new HashMap<>();
        modelParams.put("label_columns", "label");
        model.setParams(modelParams);

        ModelTrainConf trainConf = new ModelTrainConf();
        trainConf.setId("cat-train-job");
        trainConf.setModelDir("hdfs://data/cat_model_dir");

        List<String> trainDataPaths = new ArrayList<>();
        trainDataPaths.add("hdfs://data/train");
        trainConf.setTrainDataPaths(trainDataPaths);

        LightGBMModel modelController = new LightGBMModel();
        String k8sYaml = modelController.genModelTrainK8sYaml(model, trainConf);

        // Only float/double features are included; int/string are filtered out.
        assertTrue(k8sYaml.contains("\"feature_columns\": [\"f1\", \"f2\"]"));
        assertTrue(k8sYaml.contains("\"categorical_features\": []"));
    }

    @Test
    public void testGenModelExportK8sYaml() {
        ModelConfig model = new ModelConfig();
        model.setModelName("test_model");

        List<FieldSchema> fieldSchemas = new ArrayList<>();
        fieldSchemas.add(new FieldSchema("feature1", "float"));
        fieldSchemas.add(new FieldSchema("feature2", "double"));
        fieldSchemas.add(new FieldSchema("label", "int"));
        model.setInputFields(fieldSchemas);

        Map<String, String> modelParams = new HashMap<>();
        modelParams.put("label_columns", "label");
        model.setParams(modelParams);

        ModelExportConf exportConf = new ModelExportConf();
        exportConf.setId("export-job-456");
        exportConf.setBaseModelDir("hdfs://data/test_model_dir");
        exportConf.setCheckpointName("v1");

        LightGBMModel modelController = new LightGBMModel();
        String k8sYaml = modelController.genModelExportK8sYaml(model, exportConf);

        String expectedYaml = """
---
apiVersion: "v1"
kind: "ConfigMap"
metadata:
  name: "export-job-456-cm"
data:
  pipeline.config: |
    {
      "base_model_dir": "hdfs://data/test_model_dir",
      "export_dir": "hdfs://data/test_model_dir_export"
    }
  start.sh: |-
    #!/bin/bash
    set -ex
    export PYTHONPATH=/app:$PYTHONPATH

    exec python -m gbdt.export_lightgbm \\
        --pipeline_config_path /data/pipeline.config
---
apiVersion: "batch/v1"
kind: "Job"
metadata:
  name: "export-job-456-job"
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
          name: "export-job-456-cm"
        name: "config-volume"
""";
        assertEquals(expectedYaml, k8sYaml);
    }

    @Test
    public void testGetServiceK8sYaml() {
        ModelConfig model = new ModelConfig();
        model.setModelName("test_model");

        ServiceConfig serviceConf = new ServiceConfig();
        serviceConf.setId("test-service-id");
        serviceConf.setServiceName("test-service");
        serviceConf.setModelName("test_model");
        serviceConf.setCheckpointName("v1");
        serviceConf.setModelCheckpointDir("/model/checkpoint/v1");

        Map<String, String> params = new HashMap<>();
        params.put("pod_cpu_cores", "4");
        params.put("pod_memory", "16Gi");
        params.put("pod_cpu_limit", "8");
        params.put("pod_memory_limit", "32Gi");
        params.put("replicas", "3");
        serviceConf.setParams(params);

        LightGBMModel modelController = new LightGBMModel();
        String k8sYaml = modelController.getServiceK8sYaml(model, serviceConf);

        String expectedYaml = """
---
apiVersion: "apps/v1"
kind: "Deployment"
metadata:
  name: "test-service-id"
spec:
  replicas: 3
  selector:
    matchLabels:
      app: "test-service-id"
  template:
    metadata:
      labels:
        app: "test-service-id"
    spec:
      containers:
      - command:
        - "bash"
        - "-c"
        - |
          #!/bin/bash
          set -ex
          export PYTHONPATH=/app:$PYTHONPATH

          LOCAL_CACHE_DIR=${LOCAL_CACHE_DIR:-/tmp/gbdt_model_cache}
          python -c "import sys; from common.filesystem import download_dir; download_dir(sys.argv[1], sys.argv[2])" '/model/checkpoint/v1' "$LOCAL_CACHE_DIR"

          exec /app/onnx_server $LOCAL_CACHE_DIR 80
        image: "sqlrec/gbdt:0.1.0-cpu"
        name: "gbdt-service"
        ports:
        - containerPort: 80
          name: "http"
        resources:
          limits:
            cpu: "8"
            memory: "32Gi"
          requests:
            cpu: "4"
            memory: "16Gi"
---
apiVersion: "v1"
kind: "Service"
metadata:
  name: "test-service-id"
spec:
  ports:
  - name: "server"
    port: 80
    targetPort: 80
  selector:
    app: "test-service-id"
""";
        assertEquals(expectedYaml, k8sYaml);
    }

    @Test
    public void testGetServiceK8sYamlWithDefaultParams() {
        ModelConfig model = new ModelConfig();
        model.setModelName("test_model");

        ServiceConfig serviceConf = new ServiceConfig();
        serviceConf.setId("default-service-id");
        serviceConf.setModelCheckpointDir("/model/checkpoint/default");

        LightGBMModel modelController = new LightGBMModel();
        String k8sYaml = modelController.getServiceK8sYaml(model, serviceConf);

        String expectedYaml = """
---
apiVersion: "apps/v1"
kind: "Deployment"
metadata:
  name: "default-service-id"
spec:
  replicas: 1
  selector:
    matchLabels:
      app: "default-service-id"
  template:
    metadata:
      labels:
        app: "default-service-id"
    spec:
      containers:
      - command:
        - "bash"
        - "-c"
        - |
          #!/bin/bash
          set -ex
          export PYTHONPATH=/app:$PYTHONPATH

          LOCAL_CACHE_DIR=${LOCAL_CACHE_DIR:-/tmp/gbdt_model_cache}
          python -c "import sys; from common.filesystem import download_dir; download_dir(sys.argv[1], sys.argv[2])" '/model/checkpoint/default' "$LOCAL_CACHE_DIR"

          exec /app/onnx_server $LOCAL_CACHE_DIR 80
        image: "sqlrec/gbdt:0.1.0-cpu"
        name: "gbdt-service"
        ports:
        - containerPort: 80
          name: "http"
        resources:
          requests:
            cpu: "1"
            memory: "2Gi"
---
apiVersion: "v1"
kind: "Service"
metadata:
  name: "default-service-id"
spec:
  ports:
  - name: "server"
    port: 80
    targetPort: 80
  selector:
    app: "default-service-id"
""";
        assertEquals(expectedYaml, k8sYaml);
    }
}
