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

import static org.junit.jupiter.api.Assertions.*;

public class WideAndDeepModelTest {

    @Test
    public void testGetModelName() {
        WideAndDeepModel modelController = new WideAndDeepModel();
        assertEquals("tzrec.wide_and_deep", modelController.getModelName());
    }

    @Test
    public void testGetOutputFields() {
        WideAndDeepModel modelController = new WideAndDeepModel();
        ModelConfig model = new ModelConfig();
        
        List<FieldSchema> outputFields = modelController.getOutputFields(model);
        
        assertNotNull(outputFields);
        assertEquals(1, outputFields.size());
        assertEquals("probs", outputFields.get(0).getName());
        assertEquals("FLOAT", outputFields.get(0).getType());
    }

    @Test
    public void testCheckModel() {
        WideAndDeepModel modelController = new WideAndDeepModel();
        ModelConfig model = new ModelConfig();
        
        String result = modelController.checkModel(model);
        assertNull(result);
    }

    @Test
    public void testGetExportCheckpoints() {
        WideAndDeepModel modelController = new WideAndDeepModel();
        ModelExportConf exportConf = new ModelExportConf();
        exportConf.setCheckpointName("v1");
        
        List<String> checkpoints = modelController.getExportCheckpoints(exportConf);
        
        assertNotNull(checkpoints);
        assertEquals(1, checkpoints.size());
        assertEquals("v1_export", checkpoints.get(0));
    }

    @Test
    public void testGetServiceUrl() {
        WideAndDeepModel modelController = new WideAndDeepModel();
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
        WideAndDeepModel modelController = new WideAndDeepModel();
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
        trainConf.setId("train-job-123");
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
        trainParams.put("nnodes", "2");
        trainParams.put("nproc_per_node", "4");
        trainParams.put("master_port", "30000");
        trainConf.setParams(trainParams);

        WideAndDeepModel modelController = new WideAndDeepModel();
        String k8sYaml = modelController.genModelTrainK8sYaml(model, trainConf);
        
        String expectedYaml = """
---
apiVersion: "v1"
kind: "ConfigMap"
metadata:
  name: "train-job-123-cm"
data:
  pipeline.config: |
    train_input_path: "hdfs://project1/tables/data1,hdfs://project2/tables/data2"
    model_dir: "hdfs://data/test_model_dir"
    train_config {
        sparse_optimizer {
            adagrad_optimizer {
                lr: 0.005
            }
            constant_learning_rate {
            }
        }
        dense_optimizer {
            adam_optimizer {
                lr: 0.001
            }
            constant_learning_rate {
            }
        }
        num_epochs: 3
    }
    data_config {
        batch_size: 4096
        dataset_type: ParquetDataset
        fg_mode: FG_NORMAL
        label_fields: "label"
        num_workers: 4
    }
    feature_configs {
        raw_feature {
            feature_name: "feature1"
            expression: "item:feature1"
        }
    }
    feature_configs {
        id_feature {
            feature_name: "feature2"
            expression: "item:feature2"
            num_buckets: 10000000
            embedding_dim: 32
        }
    }
    feature_configs {
        id_feature {
            feature_name: "feature3"
            expression: "item:feature3"
            hash_bucket_size: 10000000
            embedding_dim: 32
        }
    }
    model_config {
        feature_groups {
            group_name: "wide"
            feature_names: "feature1"
            feature_names: "feature2"
            feature_names: "feature3"
            group_type: WIDE
        }
        feature_groups {
            group_name: "deep"
            feature_names: "feature1"
            feature_names: "feature2"
            feature_names: "feature3"
            group_type: DEEP
        }
        deepfm {
            deep {
                hidden_units: [256,128,64]
            }
        }
        metrics {
            auc {}
        }
        losses {
            binary_cross_entropy {}
        }
    }
  start.sh: |-
    #!/bin/bash
    set -ex

    NODE_RANK=${JOB_COMPLETION_INDEX:-0}
    MASTER_ADDR=${JOB_NAME}-0.${SERVICE_NAME}

    torchrun --master_addr=$MASTER_ADDR --master_port=$MASTER_PORT \\
        --nnodes=$NNODES --nproc-per-node=$NPROC_PER_NODE --node_rank=$NODE_RANK \\
        -m tzrec.train_eval \\
        --pipeline_config_path /data/pipeline.config
---
apiVersion: "v1"
kind: "Service"
metadata:
  name: "train-job-123-job-headless"
spec:
  clusterIP: "None"
  ports:
  - name: "torch-distributed"
    port: 30000
    targetPort: 30000
  selector:
    job-name: "train-job-123-job"
---
apiVersion: "batch/v1"
kind: "Job"
metadata:
  name: "train-job-123-job"
spec:
  backoffLimit: 1
  completionMode: "Indexed"
  completions: 2
  parallelism: 2
  template:
    spec:
      containers:
      - command:
        - "bash"
        - "/data/start.sh"
        env:
        - name: "JOB_NAME"
          value: "train-job-123-job"
        - name: "SERVICE_NAME"
          value: "train-job-123-job-headless"
        - name: "MASTER_PORT"
          value: "30000"
        - name: "NNODES"
          value: "2"
        - name: "NPROC_PER_NODE"
          value: "4"
        - name: "USE_FSSPEC"
          value: "1"
        - name: "USE_SPAWN_MULTI_PROCESS"
          value: "1"
        - name: "USE_FARM_HASH_TO_BUCKETIZE"
          value: "true"
        image: "sqlrec/tzrec:0.1.0-cpu"
        name: "tzrec-job"
        resources:
          limits:
            cpu: "2"
            memory: "8Gi"
          requests:
            cpu: "2"
            memory: "8Gi"
        volumeMounts:
        - mountPath: "/data"
          name: "config-volume"
      restartPolicy: "Never"
      subdomain: "train-job-123-job-headless"
      volumes:
      - configMap:
          name: "train-job-123-cm"
        name: "config-volume"
""";
        assertEquals(expectedYaml, k8sYaml);
    }

    @Test
    public void testGenModelTrainK8sYamlWithDefaultParams() {
        ModelConfig model = new ModelConfig();
        model.setModelName("default_model");

        List<FieldSchema> fieldSchemas = new ArrayList<>();
        fieldSchemas.add(new FieldSchema("feature1", "double"));
        model.setInputFields(fieldSchemas);

        Map<String, String> modelParams = new HashMap<>();
        modelParams.put("label_columns", "click");
        model.setParams(modelParams);

        ModelTrainConf trainConf = new ModelTrainConf();
        trainConf.setId("default-train-job");
        trainConf.setModelDir("hdfs://data/default_model_dir");

        WideAndDeepModel modelController = new WideAndDeepModel();
        String k8sYaml = modelController.genModelTrainK8sYaml(model, trainConf);
        
        String expectedYaml = """
---
apiVersion: "v1"
kind: "ConfigMap"
metadata:
  name: "default-train-job-cm"
data:
  pipeline.config: |
    model_dir: "hdfs://data/default_model_dir"
    train_config {
        sparse_optimizer {
            adagrad_optimizer {
                lr: 0.001
            }
            constant_learning_rate {
            }
        }
        dense_optimizer {
            adam_optimizer {
                lr: 0.001
            }
            constant_learning_rate {
            }
        }
        num_epochs: 1
    }
    data_config {
        batch_size: 8192
        dataset_type: ParquetDataset
        fg_mode: FG_NORMAL
        label_fields: "click"
        num_workers: 8
    }
    feature_configs {
        raw_feature {
            feature_name: "feature1"
            expression: "item:feature1"
        }
    }
    model_config {
        feature_groups {
            group_name: "wide"
            feature_names: "feature1"
            group_type: WIDE
        }
        feature_groups {
            group_name: "deep"
            feature_names: "feature1"
            group_type: DEEP
        }
        deepfm {
            deep {
                hidden_units: [512,256,128]
            }
        }
        metrics {
            auc {}
        }
        losses {
            binary_cross_entropy {}
        }
    }
  start.sh: |-
    #!/bin/bash
    set -ex

    NODE_RANK=${JOB_COMPLETION_INDEX:-0}
    MASTER_ADDR=${JOB_NAME}-0.${SERVICE_NAME}

    torchrun --master_addr=$MASTER_ADDR --master_port=$MASTER_PORT \\
        --nnodes=$NNODES --nproc-per-node=$NPROC_PER_NODE --node_rank=$NODE_RANK \\
        -m tzrec.train_eval \\
        --pipeline_config_path /data/pipeline.config
---
apiVersion: "v1"
kind: "Service"
metadata:
  name: "default-train-job-job-headless"
spec:
  clusterIP: "None"
  ports:
  - name: "torch-distributed"
    port: 29500
    targetPort: 29500
  selector:
    job-name: "default-train-job-job"
---
apiVersion: "batch/v1"
kind: "Job"
metadata:
  name: "default-train-job-job"
spec:
  backoffLimit: 1
  completionMode: "Indexed"
  completions: 1
  parallelism: 1
  template:
    spec:
      containers:
      - command:
        - "bash"
        - "/data/start.sh"
        env:
        - name: "JOB_NAME"
          value: "default-train-job-job"
        - name: "SERVICE_NAME"
          value: "default-train-job-job-headless"
        - name: "MASTER_PORT"
          value: "29500"
        - name: "NNODES"
          value: "1"
        - name: "NPROC_PER_NODE"
          value: "1"
        - name: "USE_FSSPEC"
          value: "1"
        - name: "USE_SPAWN_MULTI_PROCESS"
          value: "1"
        - name: "USE_FARM_HASH_TO_BUCKETIZE"
          value: "true"
        image: "sqlrec/tzrec:0.1.0-cpu"
        name: "tzrec-job"
        resources:
          limits:
            cpu: "2"
            memory: "8Gi"
          requests:
            cpu: "2"
            memory: "8Gi"
        volumeMounts:
        - mountPath: "/data"
          name: "config-volume"
      restartPolicy: "Never"
      subdomain: "default-train-job-job-headless"
      volumes:
      - configMap:
          name: "default-train-job-cm"
        name: "config-volume"
""";
        assertEquals(expectedYaml, k8sYaml);
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
        exportConf.setId("export-job-456");
        exportConf.setBaseModelDir("hdfs://data/test_model_dir");
        exportConf.setCheckpointName("v1");

        Map<String, String> exportParams = new HashMap<>();
        exportParams.put("num_workers", "4");
        exportParams.put("nnodes", "1");
        exportParams.put("nproc_per_node", "2");
        exportConf.setParams(exportParams);

        WideAndDeepModel modelController = new WideAndDeepModel();
        String k8sYaml = modelController.genModelExportK8sYaml(model, exportConf);
        
        String expectedYaml = """
---
apiVersion: "v1"
kind: "ConfigMap"
metadata:
  name: "export-job-456-cm"
data:
  pipeline.config: |
    model_dir: "hdfs://data/test_model_dir"
    data_config {
        batch_size: 8192
        dataset_type: ParquetDataset
        fg_mode: FG_NORMAL
        label_fields: "label"
        num_workers: 4
    }
    feature_configs {
        raw_feature {
            feature_name: "feature1"
            expression: "item:feature1"
        }
    }
    feature_configs {
        id_feature {
            feature_name: "feature2"
            expression: "item:feature2"
            num_buckets: 10000000
            embedding_dim: 32
        }
    }
    feature_configs {
        id_feature {
            feature_name: "feature3"
            expression: "item:feature3"
            hash_bucket_size: 10000000
            embedding_dim: 32
        }
    }
    model_config {
        feature_groups {
            group_name: "wide"
            feature_names: "feature1"
            feature_names: "feature2"
            feature_names: "feature3"
            group_type: WIDE
        }
        feature_groups {
            group_name: "deep"
            feature_names: "feature1"
            feature_names: "feature2"
            feature_names: "feature3"
            group_type: DEEP
        }
        deepfm {
            deep {
                hidden_units: [256,128,64]
            }
        }
        metrics {
            auc {}
        }
        losses {
            binary_cross_entropy {}
        }
    }
  start.sh: |-
    #!/bin/bash
    set -ex

    NODE_RANK=${JOB_COMPLETION_INDEX:-0}
    MASTER_ADDR=${JOB_NAME}-0.${SERVICE_NAME}

    torchrun --master_addr=$MASTER_ADDR --master_port=$MASTER_PORT \\
        --nnodes=$NNODES --nproc-per-node=$NPROC_PER_NODE --node_rank=$NODE_RANK \\
        -m tzrec.export \\
        --pipeline_config_path /data/pipeline.config \\
        --export_dir hdfs://data/test_model_dir_export
---
apiVersion: "v1"
kind: "Service"
metadata:
  name: "export-job-456-job-headless"
spec:
  clusterIP: "None"
  ports:
  - name: "torch-distributed"
    port: 29500
    targetPort: 29500
  selector:
    job-name: "export-job-456-job"
---
apiVersion: "batch/v1"
kind: "Job"
metadata:
  name: "export-job-456-job"
spec:
  backoffLimit: 1
  completionMode: "Indexed"
  completions: 1
  parallelism: 1
  template:
    spec:
      containers:
      - command:
        - "bash"
        - "/data/start.sh"
        env:
        - name: "JOB_NAME"
          value: "export-job-456-job"
        - name: "SERVICE_NAME"
          value: "export-job-456-job-headless"
        - name: "MASTER_PORT"
          value: "29500"
        - name: "NNODES"
          value: "1"
        - name: "NPROC_PER_NODE"
          value: "2"
        - name: "USE_FSSPEC"
          value: "1"
        - name: "USE_SPAWN_MULTI_PROCESS"
          value: "1"
        - name: "USE_FARM_HASH_TO_BUCKETIZE"
          value: "true"
        image: "sqlrec/tzrec:0.1.0-cpu"
        name: "tzrec-job"
        resources:
          limits:
            cpu: "2"
            memory: "8Gi"
          requests:
            cpu: "2"
            memory: "8Gi"
        volumeMounts:
        - mountPath: "/data"
          name: "config-volume"
      restartPolicy: "Never"
      subdomain: "export-job-456-job-headless"
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
        params.put("replicas", "3");
        serviceConf.setParams(params);

        WideAndDeepModel modelController = new WideAndDeepModel();
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
        - "/app/server.sh"
        - "--scripted_model_dir"
        - "/model/checkpoint/v1"
        env:
        - name: "USE_FSSPEC"
          value: "1"
        - name: "USE_SPAWN_MULTI_PROCESS"
          value: "1"
        - name: "USE_FARM_HASH_TO_BUCKETIZE"
          value: "true"
        image: "sqlrec/tzrec:0.1.0-cpu"
        name: "tzrec-service"
        ports:
        - containerPort: 80
          name: "http"
        resources:
          limits:
            cpu: "4"
            memory: "16Gi"
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

        WideAndDeepModel modelController = new WideAndDeepModel();
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
        - "/app/server.sh"
        - "--scripted_model_dir"
        - "/model/checkpoint/default"
        env:
        - name: "USE_FSSPEC"
          value: "1"
        - name: "USE_SPAWN_MULTI_PROCESS"
          value: "1"
        - name: "USE_FARM_HASH_TO_BUCKETIZE"
          value: "true"
        image: "sqlrec/tzrec:0.1.0-cpu"
        name: "tzrec-service"
        ports:
        - containerPort: 80
          name: "http"
        resources:
          limits:
            cpu: "2"
            memory: "8Gi"
          requests:
            cpu: "2"
            memory: "8Gi"
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

    @Test
    public void testGenModelTrainK8sYamlWithIntFeatures() {
        ModelConfig model = new ModelConfig();
        model.setModelName("int_feature_model");

        List<FieldSchema> fieldSchemas = new ArrayList<>();
        fieldSchemas.add(new FieldSchema("int_feature", "int"));
        fieldSchemas.add(new FieldSchema("bigint_feature", "bigint"));
        fieldSchemas.add(new FieldSchema("int_array", "array<int>"));
        model.setInputFields(fieldSchemas);

        Map<String, String> modelParams = new HashMap<>();
        modelParams.put("label_columns", "label");
        modelParams.put("num_buckets", "500000");
        model.setParams(modelParams);

        ModelTrainConf trainConf = new ModelTrainConf();
        trainConf.setId("int-train-job");
        trainConf.setModelDir("hdfs://data/int_model_dir");

        WideAndDeepModel modelController = new WideAndDeepModel();
        String k8sYaml = modelController.genModelTrainK8sYaml(model, trainConf);
        
        String expectedYaml = """
---
apiVersion: "v1"
kind: "ConfigMap"
metadata:
  name: "int-train-job-cm"
data:
  pipeline.config: |
    model_dir: "hdfs://data/int_model_dir"
    train_config {
        sparse_optimizer {
            adagrad_optimizer {
                lr: 0.001
            }
            constant_learning_rate {
            }
        }
        dense_optimizer {
            adam_optimizer {
                lr: 0.001
            }
            constant_learning_rate {
            }
        }
        num_epochs: 1
    }
    data_config {
        batch_size: 8192
        dataset_type: ParquetDataset
        fg_mode: FG_NORMAL
        label_fields: "label"
        num_workers: 8
    }
    feature_configs {
        id_feature {
            feature_name: "int_feature"
            expression: "item:int_feature"
            num_buckets: 500000
            embedding_dim: 16
        }
    }
    feature_configs {
        id_feature {
            feature_name: "bigint_feature"
            expression: "item:bigint_feature"
            num_buckets: 500000
            embedding_dim: 16
        }
    }
    feature_configs {
        id_feature {
            feature_name: "int_array"
            expression: "item:int_array"
            num_buckets: 500000
            embedding_dim: 16
        }
    }
    model_config {
        feature_groups {
            group_name: "wide"
            feature_names: "int_feature"
            feature_names: "bigint_feature"
            feature_names: "int_array"
            group_type: WIDE
        }
        feature_groups {
            group_name: "deep"
            feature_names: "int_feature"
            feature_names: "bigint_feature"
            feature_names: "int_array"
            group_type: DEEP
        }
        deepfm {
            deep {
                hidden_units: [512,256,128]
            }
        }
        metrics {
            auc {}
        }
        losses {
            binary_cross_entropy {}
        }
    }
  start.sh: |-
    #!/bin/bash
    set -ex

    NODE_RANK=${JOB_COMPLETION_INDEX:-0}
    MASTER_ADDR=${JOB_NAME}-0.${SERVICE_NAME}

    torchrun --master_addr=$MASTER_ADDR --master_port=$MASTER_PORT \\
        --nnodes=$NNODES --nproc-per-node=$NPROC_PER_NODE --node_rank=$NODE_RANK \\
        -m tzrec.train_eval \\
        --pipeline_config_path /data/pipeline.config
---
apiVersion: "v1"
kind: "Service"
metadata:
  name: "int-train-job-job-headless"
spec:
  clusterIP: "None"
  ports:
  - name: "torch-distributed"
    port: 29500
    targetPort: 29500
  selector:
    job-name: "int-train-job-job"
---
apiVersion: "batch/v1"
kind: "Job"
metadata:
  name: "int-train-job-job"
spec:
  backoffLimit: 1
  completionMode: "Indexed"
  completions: 1
  parallelism: 1
  template:
    spec:
      containers:
      - command:
        - "bash"
        - "/data/start.sh"
        env:
        - name: "JOB_NAME"
          value: "int-train-job-job"
        - name: "SERVICE_NAME"
          value: "int-train-job-job-headless"
        - name: "MASTER_PORT"
          value: "29500"
        - name: "NNODES"
          value: "1"
        - name: "NPROC_PER_NODE"
          value: "1"
        - name: "USE_FSSPEC"
          value: "1"
        - name: "USE_SPAWN_MULTI_PROCESS"
          value: "1"
        - name: "USE_FARM_HASH_TO_BUCKETIZE"
          value: "true"
        image: "sqlrec/tzrec:0.1.0-cpu"
        name: "tzrec-job"
        resources:
          limits:
            cpu: "2"
            memory: "8Gi"
          requests:
            cpu: "2"
            memory: "8Gi"
        volumeMounts:
        - mountPath: "/data"
          name: "config-volume"
      restartPolicy: "Never"
      subdomain: "int-train-job-job-headless"
      volumes:
      - configMap:
          name: "int-train-job-cm"
        name: "config-volume"
""";
        assertEquals(expectedYaml, k8sYaml);
    }

    @Test
    public void testGenModelTrainK8sYamlWithCustomFeatureConfig() {
        ModelConfig model = new ModelConfig();
        model.setModelName("custom_feature_model");

        List<FieldSchema> fieldSchemas = new ArrayList<>();
        fieldSchemas.add(new FieldSchema("custom_feature", "string"));
        model.setInputFields(fieldSchemas);

        Map<String, String> modelParams = new HashMap<>();
        modelParams.put("label_columns", "label");
        modelParams.put("column.custom_feature.bucket_size", "200000");
        modelParams.put("column.custom_feature.embedding_dim", "64");
        model.setParams(modelParams);

        ModelTrainConf trainConf = new ModelTrainConf();
        trainConf.setId("custom-train-job");
        trainConf.setModelDir("hdfs://data/custom_model_dir");

        WideAndDeepModel modelController = new WideAndDeepModel();
        String k8sYaml = modelController.genModelTrainK8sYaml(model, trainConf);
        
        String expectedYaml = """
---
apiVersion: "v1"
kind: "ConfigMap"
metadata:
  name: "custom-train-job-cm"
data:
  pipeline.config: |
    model_dir: "hdfs://data/custom_model_dir"
    train_config {
        sparse_optimizer {
            adagrad_optimizer {
                lr: 0.001
            }
            constant_learning_rate {
            }
        }
        dense_optimizer {
            adam_optimizer {
                lr: 0.001
            }
            constant_learning_rate {
            }
        }
        num_epochs: 1
    }
    data_config {
        batch_size: 8192
        dataset_type: ParquetDataset
        fg_mode: FG_NORMAL
        label_fields: "label"
        num_workers: 8
    }
    feature_configs {
        id_feature {
            feature_name: "custom_feature"
            expression: "item:custom_feature"
            hash_bucket_size: 200000
            embedding_dim: 64
        }
    }
    model_config {
        feature_groups {
            group_name: "wide"
            feature_names: "custom_feature"
            group_type: WIDE
        }
        feature_groups {
            group_name: "deep"
            feature_names: "custom_feature"
            group_type: DEEP
        }
        deepfm {
            deep {
                hidden_units: [512,256,128]
            }
        }
        metrics {
            auc {}
        }
        losses {
            binary_cross_entropy {}
        }
    }
  start.sh: |-
    #!/bin/bash
    set -ex

    NODE_RANK=${JOB_COMPLETION_INDEX:-0}
    MASTER_ADDR=${JOB_NAME}-0.${SERVICE_NAME}

    torchrun --master_addr=$MASTER_ADDR --master_port=$MASTER_PORT \\
        --nnodes=$NNODES --nproc-per-node=$NPROC_PER_NODE --node_rank=$NODE_RANK \\
        -m tzrec.train_eval \\
        --pipeline_config_path /data/pipeline.config
---
apiVersion: "v1"
kind: "Service"
metadata:
  name: "custom-train-job-job-headless"
spec:
  clusterIP: "None"
  ports:
  - name: "torch-distributed"
    port: 29500
    targetPort: 29500
  selector:
    job-name: "custom-train-job-job"
---
apiVersion: "batch/v1"
kind: "Job"
metadata:
  name: "custom-train-job-job"
spec:
  backoffLimit: 1
  completionMode: "Indexed"
  completions: 1
  parallelism: 1
  template:
    spec:
      containers:
      - command:
        - "bash"
        - "/data/start.sh"
        env:
        - name: "JOB_NAME"
          value: "custom-train-job-job"
        - name: "SERVICE_NAME"
          value: "custom-train-job-job-headless"
        - name: "MASTER_PORT"
          value: "29500"
        - name: "NNODES"
          value: "1"
        - name: "NPROC_PER_NODE"
          value: "1"
        - name: "USE_FSSPEC"
          value: "1"
        - name: "USE_SPAWN_MULTI_PROCESS"
          value: "1"
        - name: "USE_FARM_HASH_TO_BUCKETIZE"
          value: "true"
        image: "sqlrec/tzrec:0.1.0-cpu"
        name: "tzrec-job"
        resources:
          limits:
            cpu: "2"
            memory: "8Gi"
          requests:
            cpu: "2"
            memory: "8Gi"
        volumeMounts:
        - mountPath: "/data"
          name: "config-volume"
      restartPolicy: "Never"
      subdomain: "custom-train-job-job-headless"
      volumes:
      - configMap:
          name: "custom-train-job-cm"
        name: "config-volume"
""";
        assertEquals(expectedYaml, k8sYaml);
    }
}
