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

public class DSSMModelTest {

    @Test
    public void testGetModelName() {
        DSSMModel modelController = new DSSMModel();
        assertEquals("tzrec.dssm", modelController.getModelName());
    }

    @Test
    public void testGetOutputFields() {
        DSSMModel modelController = new DSSMModel();
        ModelConfig model = new ModelConfig();
        
        List<FieldSchema> outputFields = modelController.getOutputFields(model);
        
        assertNotNull(outputFields);
        assertEquals(2, outputFields.size());
        assertEquals("user_tower_emb", outputFields.get(0).getName());
        assertEquals("ARRAY<FLOAT>", outputFields.get(0).getType());
        assertEquals("item_tower_emb", outputFields.get(1).getName());
        assertEquals("ARRAY<FLOAT>", outputFields.get(1).getType());
    }

    @Test
    public void testCheckModelWithBothFeatures() {
        DSSMModel modelController = new DSSMModel();
        ModelConfig model = new ModelConfig();
        
        Map<String, String> params = new HashMap<>();
        params.put("user_features", "user_id,cms_segid");
        params.put("item_features", "item_id,cate_id");
        model.setParams(params);
        
        String result = modelController.checkModel(model);
        assertNull(result);
    }

    @Test
    public void testCheckModelWithOnlyUserFeatures() {
        DSSMModel modelController = new DSSMModel();
        ModelConfig model = new ModelConfig();
        
        Map<String, String> params = new HashMap<>();
        params.put("user_features", "user_id,cms_segid");
        model.setParams(params);
        
        String result = modelController.checkModel(model);
        assertNull(result);
    }

    @Test
    public void testCheckModelWithOnlyItemFeatures() {
        DSSMModel modelController = new DSSMModel();
        ModelConfig model = new ModelConfig();
        
        Map<String, String> params = new HashMap<>();
        params.put("item_features", "item_id,cate_id");
        model.setParams(params);
        
        String result = modelController.checkModel(model);
        assertNull(result);
    }

    @Test
    public void testCheckModelWithNoFeatures() {
        DSSMModel modelController = new DSSMModel();
        ModelConfig model = new ModelConfig();
        model.setParams(new HashMap<>());
        
        String result = modelController.checkModel(model);
        assertEquals("At least one of user_features or item_features is required for DSSM model", result);
    }

    @Test
    public void testGetExportCheckpoints() {
        DSSMModel modelController = new DSSMModel();
        ModelExportConf exportConf = new ModelExportConf();
        exportConf.setCheckpointName("v1");
        
        List<String> checkpoints = modelController.getExportCheckpoints(exportConf);
        
        assertNotNull(checkpoints);
        assertEquals(2, checkpoints.size());
        assertEquals("v1_export/user", checkpoints.get(1));
        assertEquals("v1_export/item", checkpoints.get(0));
    }

    @Test
    public void testGetServiceUrl() {
        DSSMModel modelController = new DSSMModel();
        ModelConfig model = new ModelConfig();
        ServiceConfig serviceConf = new ServiceConfig();
        serviceConf.setId("dssm-service-id");
        
        Map<String, String> params = new HashMap<>();
        params.put("NAMESPACE", "default");
        serviceConf.setParams(params);
        
        String url = modelController.getServiceUrl(model, serviceConf);
        
        assertEquals("http://dssm-service-id.default.svc.cluster.local:80/predict", url);
    }

    @Test
    public void testGetServiceUrlWithCustomNamespace() {
        DSSMModel modelController = new DSSMModel();
        ModelConfig model = new ModelConfig();
        ServiceConfig serviceConf = new ServiceConfig();
        serviceConf.setId("my-dssm-service");
        
        Map<String, String> params = new HashMap<>();
        params.put("NAMESPACE", "custom-namespace");
        serviceConf.setParams(params);
        
        String url = modelController.getServiceUrl(model, serviceConf);
        
        assertEquals("http://my-dssm-service.custom-namespace.svc.cluster.local:80/predict", url);
    }

    @Test
    public void testGenModelTrainK8sYamlWithBothFeatures() {
        ModelConfig model = new ModelConfig();
        model.setModelName("dssm_model");

        List<FieldSchema> fieldSchemas = new ArrayList<>();
        fieldSchemas.add(new FieldSchema("user_id", "string"));
        fieldSchemas.add(new FieldSchema("cms_segid", "int"));
        fieldSchemas.add(new FieldSchema("item_id", "string"));
        fieldSchemas.add(new FieldSchema("cate_id", "string"));
        model.setInputFields(fieldSchemas);

        Map<String, String> modelParams = new HashMap<>();
        modelParams.put("user_features", "user_id,cms_segid");
        modelParams.put("item_features", "item_id,cate_id");
        modelParams.put("user_hidden_units", "256,128,64");
        modelParams.put("item_hidden_units", "256,128,64");
        modelParams.put("output_dim", "32");
        modelParams.put("embedding_dim", "16");
        modelParams.put("num_buckets", "100000");
        modelParams.put("label_columns", "click");
        model.setParams(modelParams);

        ModelTrainConf trainConf = new ModelTrainConf();
        trainConf.setId("dssm-train-job");
        trainConf.setModelDir("hdfs://data/dssm_model_dir");

        List<String> trainDataPaths = new ArrayList<>();
        trainDataPaths.add("hdfs://project/tables/train_data");
        trainConf.setTrainDataPaths(trainDataPaths);

        Map<String, String> trainParams = new HashMap<>();
        trainParams.put("batch_size", "2048");
        trainParams.put("num_workers", "4");
        trainConf.setParams(trainParams);

        DSSMModel modelController = new DSSMModel();
        String k8sYaml = modelController.genModelTrainK8sYaml(model, trainConf);
        
        String expectedYaml = """
---
apiVersion: "v1"
kind: "ConfigMap"
metadata:
  name: "dssm-train-job-cm"
data:
  pipeline.config: |
    train_input_path: "hdfs://project/tables/train_data"
    model_dir: "hdfs://data/dssm_model_dir"
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
        batch_size: 2048
        dataset_type: ParquetDataset
        fg_mode: FG_NORMAL
        label_fields: "click"
        num_workers: 4
    }
    feature_configs {
        id_feature {
            feature_name: "user_id"
            expression: "item:user_id"
            hash_bucket_size: 100000
            embedding_dim: 16
        }
    }
    feature_configs {
        id_feature {
            feature_name: "cms_segid"
            expression: "item:cms_segid"
            num_buckets: 100000
            embedding_dim: 16
        }
    }
    feature_configs {
        id_feature {
            feature_name: "item_id"
            expression: "item:item_id"
            hash_bucket_size: 100000
            embedding_dim: 16
        }
    }
    feature_configs {
        id_feature {
            feature_name: "cate_id"
            expression: "item:cate_id"
            hash_bucket_size: 100000
            embedding_dim: 16
        }
    }
    model_config {
        feature_groups {
            group_name: "user"
            feature_names: "user_id"
            feature_names: "cms_segid"
            group_type: DEEP
        }
        feature_groups {
            group_name: "item"
            feature_names: "item_id"
            feature_names: "cate_id"
            group_type: DEEP
        }
        dssm {
            user_tower {
                input: 'user'
                mlp {
                    hidden_units: [256,128,64]
                }
            }
            item_tower {
                input: 'item'
                mlp {
                    hidden_units: [256,128,64]
                }
            }
            output_dim: 32
            in_batch_negative: true
        }
        metrics {
            recall_at_k {
                top_k: 1
            }
        }
        metrics {
            recall_at_k {
                top_k: 5
            }
        }
        losses {
            softmax_cross_entropy {}
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
  name: "dssm-train-job-job-headless"
spec:
  clusterIP: "None"
  ports:
  - name: "torch-distributed"
    port: 29500
    targetPort: 29500
  selector:
    job-name: "dssm-train-job-job"
---
apiVersion: "batch/v1"
kind: "Job"
metadata:
  name: "dssm-train-job-job"
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
          value: "dssm-train-job-job"
        - name: "SERVICE_NAME"
          value: "dssm-train-job-job-headless"
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
          requests:
            cpu: "1"
            memory: "2Gi"
        volumeMounts:
        - mountPath: "/data"
          name: "config-volume"
      restartPolicy: "Never"
      subdomain: "dssm-train-job-job-headless"
      volumes:
      - configMap:
          name: "dssm-train-job-cm"
        name: "config-volume"
""";
        assertEquals(expectedYaml, k8sYaml);
    }

    @Test
    public void testGenModelTrainK8sYamlWithOnlyUserFeatures() {
        ModelConfig model = new ModelConfig();
        model.setModelName("dssm_model");

        List<FieldSchema> fieldSchemas = new ArrayList<>();
        fieldSchemas.add(new FieldSchema("user_id", "string"));
        fieldSchemas.add(new FieldSchema("item_id", "string"));
        fieldSchemas.add(new FieldSchema("cate_id", "string"));
        model.setInputFields(fieldSchemas);

        Map<String, String> modelParams = new HashMap<>();
        modelParams.put("user_features", "user_id");
        modelParams.put("label_columns", "click");
        model.setParams(modelParams);

        ModelTrainConf trainConf = new ModelTrainConf();
        trainConf.setId("dssm-user-only-job");
        trainConf.setModelDir("hdfs://data/dssm_model_dir");

        DSSMModel modelController = new DSSMModel();
        String k8sYaml = modelController.genModelTrainK8sYaml(model, trainConf);
        
        String expectedYaml = """
---
apiVersion: "v1"
kind: "ConfigMap"
metadata:
  name: "dssm-user-only-job-cm"
data:
  pipeline.config: |
    model_dir: "hdfs://data/dssm_model_dir"
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
        id_feature {
            feature_name: "user_id"
            expression: "item:user_id"
            hash_bucket_size: 1000000
            embedding_dim: 16
        }
    }
    feature_configs {
        id_feature {
            feature_name: "item_id"
            expression: "item:item_id"
            hash_bucket_size: 1000000
            embedding_dim: 16
        }
    }
    feature_configs {
        id_feature {
            feature_name: "cate_id"
            expression: "item:cate_id"
            hash_bucket_size: 1000000
            embedding_dim: 16
        }
    }
    model_config {
        feature_groups {
            group_name: "user"
            feature_names: "user_id"
            group_type: DEEP
        }
        feature_groups {
            group_name: "item"
            feature_names: "item_id"
            feature_names: "cate_id"
            group_type: DEEP
        }
        dssm {
            user_tower {
                input: 'user'
                mlp {
                    hidden_units: [512,256,128]
                }
            }
            item_tower {
                input: 'item'
                mlp {
                    hidden_units: [512,256,128]
                }
            }
            output_dim: 64
            in_batch_negative: true
        }
        metrics {
            recall_at_k {
                top_k: 1
            }
        }
        metrics {
            recall_at_k {
                top_k: 5
            }
        }
        losses {
            softmax_cross_entropy {}
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
  name: "dssm-user-only-job-job-headless"
spec:
  clusterIP: "None"
  ports:
  - name: "torch-distributed"
    port: 29500
    targetPort: 29500
  selector:
    job-name: "dssm-user-only-job-job"
---
apiVersion: "batch/v1"
kind: "Job"
metadata:
  name: "dssm-user-only-job-job"
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
          value: "dssm-user-only-job-job"
        - name: "SERVICE_NAME"
          value: "dssm-user-only-job-job-headless"
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
          requests:
            cpu: "1"
            memory: "2Gi"
        volumeMounts:
        - mountPath: "/data"
          name: "config-volume"
      restartPolicy: "Never"
      subdomain: "dssm-user-only-job-job-headless"
      volumes:
      - configMap:
          name: "dssm-user-only-job-cm"
        name: "config-volume"
""";
        assertEquals(expectedYaml, k8sYaml);
    }

    @Test
    public void testGenModelTrainK8sYamlWithOnlyItemFeatures() {
        ModelConfig model = new ModelConfig();
        model.setModelName("dssm_model");

        List<FieldSchema> fieldSchemas = new ArrayList<>();
        fieldSchemas.add(new FieldSchema("user_id", "string"));
        fieldSchemas.add(new FieldSchema("cms_segid", "int"));
        fieldSchemas.add(new FieldSchema("item_id", "string"));
        model.setInputFields(fieldSchemas);

        Map<String, String> modelParams = new HashMap<>();
        modelParams.put("item_features", "item_id");
        modelParams.put("label_columns", "click");
        model.setParams(modelParams);

        ModelTrainConf trainConf = new ModelTrainConf();
        trainConf.setId("dssm-item-only-job");
        trainConf.setModelDir("hdfs://data/dssm_model_dir");

        DSSMModel modelController = new DSSMModel();
        String k8sYaml = modelController.genModelTrainK8sYaml(model, trainConf);
        
        String expectedYaml = """
---
apiVersion: "v1"
kind: "ConfigMap"
metadata:
  name: "dssm-item-only-job-cm"
data:
  pipeline.config: |
    model_dir: "hdfs://data/dssm_model_dir"
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
        id_feature {
            feature_name: "user_id"
            expression: "item:user_id"
            hash_bucket_size: 1000000
            embedding_dim: 16
        }
    }
    feature_configs {
        id_feature {
            feature_name: "cms_segid"
            expression: "item:cms_segid"
            num_buckets: 1000000
            embedding_dim: 16
        }
    }
    feature_configs {
        id_feature {
            feature_name: "item_id"
            expression: "item:item_id"
            hash_bucket_size: 1000000
            embedding_dim: 16
        }
    }
    model_config {
        feature_groups {
            group_name: "user"
            feature_names: "user_id"
            feature_names: "cms_segid"
            group_type: DEEP
        }
        feature_groups {
            group_name: "item"
            feature_names: "item_id"
            group_type: DEEP
        }
        dssm {
            user_tower {
                input: 'user'
                mlp {
                    hidden_units: [512,256,128]
                }
            }
            item_tower {
                input: 'item'
                mlp {
                    hidden_units: [512,256,128]
                }
            }
            output_dim: 64
            in_batch_negative: true
        }
        metrics {
            recall_at_k {
                top_k: 1
            }
        }
        metrics {
            recall_at_k {
                top_k: 5
            }
        }
        losses {
            softmax_cross_entropy {}
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
  name: "dssm-item-only-job-job-headless"
spec:
  clusterIP: "None"
  ports:
  - name: "torch-distributed"
    port: 29500
    targetPort: 29500
  selector:
    job-name: "dssm-item-only-job-job"
---
apiVersion: "batch/v1"
kind: "Job"
metadata:
  name: "dssm-item-only-job-job"
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
          value: "dssm-item-only-job-job"
        - name: "SERVICE_NAME"
          value: "dssm-item-only-job-job-headless"
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
          requests:
            cpu: "1"
            memory: "2Gi"
        volumeMounts:
        - mountPath: "/data"
          name: "config-volume"
      restartPolicy: "Never"
      subdomain: "dssm-item-only-job-job-headless"
      volumes:
      - configMap:
          name: "dssm-item-only-job-cm"
        name: "config-volume"
""";
        assertEquals(expectedYaml, k8sYaml);
    }

    @Test
    public void testGenModelExportK8sYaml() {
        ModelConfig model = new ModelConfig();
        model.setModelName("dssm_model");

        List<FieldSchema> fieldSchemas = new ArrayList<>();
        fieldSchemas.add(new FieldSchema("user_id", "string"));
        fieldSchemas.add(new FieldSchema("item_id", "string"));
        model.setInputFields(fieldSchemas);

        Map<String, String> modelParams = new HashMap<>();
        modelParams.put("user_features", "user_id");
        modelParams.put("item_features", "item_id");
        modelParams.put("label_columns", "click");
        model.setParams(modelParams);

        ModelExportConf exportConf = new ModelExportConf();
        exportConf.setId("dssm-export-job");
        exportConf.setBaseModelDir("hdfs://data/dssm_model_dir");
        exportConf.setCheckpointName("v1");

        DSSMModel modelController = new DSSMModel();
        String k8sYaml = modelController.genModelExportK8sYaml(model, exportConf);
        
        String expectedYaml = """
---
apiVersion: "v1"
kind: "ConfigMap"
metadata:
  name: "dssm-export-job-cm"
data:
  pipeline.config: |
    model_dir: "hdfs://data/dssm_model_dir"
    data_config {
        batch_size: 8192
        dataset_type: ParquetDataset
        fg_mode: FG_NORMAL
        label_fields: "click"
        num_workers: 8
    }
    feature_configs {
        id_feature {
            feature_name: "user_id"
            expression: "item:user_id"
            hash_bucket_size: 1000000
            embedding_dim: 16
        }
    }
    feature_configs {
        id_feature {
            feature_name: "item_id"
            expression: "item:item_id"
            hash_bucket_size: 1000000
            embedding_dim: 16
        }
    }
    model_config {
        feature_groups {
            group_name: "user"
            feature_names: "user_id"
            group_type: DEEP
        }
        feature_groups {
            group_name: "item"
            feature_names: "item_id"
            group_type: DEEP
        }
        dssm {
            user_tower {
                input: 'user'
                mlp {
                    hidden_units: [512,256,128]
                }
            }
            item_tower {
                input: 'item'
                mlp {
                    hidden_units: [512,256,128]
                }
            }
            output_dim: 64
            in_batch_negative: true
        }
        metrics {
            recall_at_k {
                top_k: 1
            }
        }
        metrics {
            recall_at_k {
                top_k: 5
            }
        }
        losses {
            softmax_cross_entropy {}
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
        --export_dir hdfs://data/dssm_model_dir_export
---
apiVersion: "v1"
kind: "Service"
metadata:
  name: "dssm-export-job-job-headless"
spec:
  clusterIP: "None"
  ports:
  - name: "torch-distributed"
    port: 29500
    targetPort: 29500
  selector:
    job-name: "dssm-export-job-job"
---
apiVersion: "batch/v1"
kind: "Job"
metadata:
  name: "dssm-export-job-job"
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
          value: "dssm-export-job-job"
        - name: "SERVICE_NAME"
          value: "dssm-export-job-job-headless"
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
          requests:
            cpu: "1"
            memory: "2Gi"
        volumeMounts:
        - mountPath: "/data"
          name: "config-volume"
      restartPolicy: "Never"
      subdomain: "dssm-export-job-job-headless"
      volumes:
      - configMap:
          name: "dssm-export-job-cm"
        name: "config-volume"
""";
        assertEquals(expectedYaml, k8sYaml);
    }

    @Test
    public void testGetServiceK8sYaml() {
        ModelConfig model = new ModelConfig();
        model.setModelName("dssm_model");

        ServiceConfig serviceConf = new ServiceConfig();
        serviceConf.setId("dssm-service-id");
        serviceConf.setModelCheckpointDir("/model/checkpoint/v1");

        Map<String, String> params = new HashMap<>();
        params.put("pod_cpu_cores", "4");
        params.put("pod_memory", "16Gi");
        params.put("pod_cpu_limit", "8");
        params.put("pod_memory_limit", "32Gi");
        params.put("replicas", "3");
        serviceConf.setParams(params);

        DSSMModel modelController = new DSSMModel();
        String k8sYaml = modelController.getServiceK8sYaml(model, serviceConf);
        
        String expectedYaml = """
---
apiVersion: "apps/v1"
kind: "Deployment"
metadata:
  name: "dssm-service-id"
spec:
  replicas: 3
  selector:
    matchLabels:
      app: "dssm-service-id"
  template:
    metadata:
      labels:
        app: "dssm-service-id"
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
            cpu: "8"
            memory: "32Gi"
          requests:
            cpu: "4"
            memory: "16Gi"
---
apiVersion: "v1"
kind: "Service"
metadata:
  name: "dssm-service-id"
spec:
  ports:
  - name: "server"
    port: 80
    targetPort: 80
  selector:
    app: "dssm-service-id"
""";
        assertEquals(expectedYaml, k8sYaml);
    }

    @Test
    public void testGetServiceK8sYamlWithDefaultParams() {
        ModelConfig model = new ModelConfig();
        model.setModelName("dssm_model");

        ServiceConfig serviceConf = new ServiceConfig();
        serviceConf.setId("dssm-default-service");
        serviceConf.setModelCheckpointDir("/model/checkpoint/default");

        DSSMModel modelController = new DSSMModel();
        String k8sYaml = modelController.getServiceK8sYaml(model, serviceConf);
        
        String expectedYaml = """
---
apiVersion: "apps/v1"
kind: "Deployment"
metadata:
  name: "dssm-default-service"
spec:
  replicas: 1
  selector:
    matchLabels:
      app: "dssm-default-service"
  template:
    metadata:
      labels:
        app: "dssm-default-service"
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
          requests:
            cpu: "1"
            memory: "2Gi"
---
apiVersion: "v1"
kind: "Service"
metadata:
  name: "dssm-default-service"
spec:
  ports:
  - name: "server"
    port: 80
    targetPort: 80
  selector:
    app: "dssm-default-service"
""";
        assertEquals(expectedYaml, k8sYaml);
    }
}
