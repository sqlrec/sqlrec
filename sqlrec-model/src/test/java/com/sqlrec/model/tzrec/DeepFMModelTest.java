package com.sqlrec.model.tzrec;

import com.sqlrec.common.model.ModelConf;
import com.sqlrec.common.model.ModelExportConf;
import com.sqlrec.common.model.ModelTrainConf;
import com.sqlrec.common.model.ServiceConf;
import com.sqlrec.common.schema.FieldSchema;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static com.sqlrec.common.config.SqlRecConfigs.SQLREC_VERSION;
import static org.junit.jupiter.api.Assertions.*;

class DeepFMModelTest {
    private final DeepFMModel controller = new DeepFMModel();

    @Test
    void trainYamlMatchesCompleteExpectedYaml() {
        String actual = fullTrainingYaml();
        String expectedYaml = """
---
apiVersion: "v1"
kind: "ConfigMap"
metadata:
  name: "deepfm-train-cm"
data:
  pipeline.config: |
    train_input_path: "/fixture/train.parquet"
    eval_input_path: "/fixture/eval.parquet"
    eval_config {}
    model_dir: "/fixture/deepfm/v1"
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
        num_epochs: 2
        use_tensorboard: true
        tensorboard_summaries: "loss"
        tensorboard_summaries: "learning_rate"
        tensorboard_summaries: "global_gradient_norm"
        log_step_count_steps: 100
        save_checkpoints_steps: 1000
        fine_tune_checkpoint: "/fixture/deepfm/base"
    }
    data_config {
        batch_size: 16
        dataset_type: ParquetDataset
        fg_mode: FG_NORMAL
        label_fields: "label"
        num_workers: 0
    }
    feature_configs {
        id_feature {
            feature_name: "user_id"
            expression: "item:user_id"
            hash_bucket_size: 32
            embedding_dim: 8
        }
    }
    feature_configs {
        id_feature {
            feature_name: "category"
            expression: "item:category"
            num_buckets: 32
            embedding_dim: 8
        }
    }
    feature_configs {
        raw_feature {
            feature_name: "price"
            expression: "item:price"
            value_dim: 1
            default_value: "0"
        }
    }
    model_config {
        feature_groups {
            group_name: "wide"
            feature_names: "user_id"
            feature_names: "category"
            group_type: WIDE
        }
        feature_groups {
            group_name: "deep"
            feature_names: "user_id"
            feature_names: "category"
            feature_names: "price"
            group_type: DEEP
        }
        feature_groups {
            group_name: "fm"
            feature_names: "user_id"
            feature_names: "category"
            group_type: DEEP
        }
        deepfm {
            deep {
                hidden_units: [16,8]
            }
        }
        metrics { auc {} }
        metrics { normalized_entropy {} }
        metrics { accuracy {} }
        train_metrics { auc {} }
        train_metrics { accuracy {} }
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
        /app/run.py --mode train \\
        --pipeline_config_path /data/pipeline.config --continue_train --restore_optimizer true
---
apiVersion: "v1"
kind: "Service"
metadata:
  name: "deepfm-train-job-headless"
spec:
  clusterIP: "None"
  ports:
  - name: "torch-distributed"
    port: 29500
    targetPort: 29500
  selector:
    job-name: "deepfm-train-job"
---
apiVersion: "batch/v1"
kind: "Job"
metadata:
  name: "deepfm-train-job"
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
          value: "deepfm-train-job"
        - name: "SERVICE_NAME"
          value: "deepfm-train-job-headless"
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
        image: "sqlrec/tzrec:%s-cpu"
        name: "tzrec-job"
        resources:
          requests:
            cpu: "2"
            memory: "4Gi"
        volumeMounts:
        - mountPath: "/data"
          name: "config-volume"
      restartPolicy: "Never"
      subdomain: "deepfm-train-job-headless"
      volumes:
      - configMap:
          name: "deepfm-train-cm"
        name: "config-volume"
""".formatted(SQLREC_VERSION.getValue());
        assertEquals(expectedYaml, actual);
    }

    @Test
    void exportYamlMatchesCompleteExpectedYaml() {
        String actual = fullExportYaml();
        String expectedYaml = """
---
apiVersion: "v1"
kind: "ConfigMap"
metadata:
  name: "deepfm-export-cm"
data:
  pipeline.config: |
    train_input_path: "/fixture/train.parquet"
    model_dir: "/fixture/deepfm/v1"
    data_config {
        batch_size: 17
        dataset_type: ParquetDataset
        fg_mode: FG_NORMAL
        label_fields: "label"
        num_workers: 1
    }
    feature_configs {
        id_feature {
            feature_name: "user_id"
            expression: "item:user_id"
            hash_bucket_size: 32
            embedding_dim: 8
        }
    }
    feature_configs {
        id_feature {
            feature_name: "category"
            expression: "item:category"
            num_buckets: 32
            embedding_dim: 8
        }
    }
    feature_configs {
        raw_feature {
            feature_name: "price"
            expression: "item:price"
            value_dim: 1
            default_value: "0"
        }
    }
    model_config {
        feature_groups {
            group_name: "wide"
            feature_names: "user_id"
            feature_names: "category"
            group_type: WIDE
        }
        feature_groups {
            group_name: "deep"
            feature_names: "user_id"
            feature_names: "category"
            feature_names: "price"
            group_type: DEEP
        }
        feature_groups {
            group_name: "fm"
            feature_names: "user_id"
            feature_names: "category"
            group_type: DEEP
        }
        deepfm {
            deep {
                hidden_units: [16,8]
            }
        }
        metrics { auc {} }
        metrics { normalized_entropy {} }
        metrics { accuracy {} }
        train_metrics { auc {} }
        train_metrics { accuracy {} }
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
        /app/run.py --mode export \\
        --pipeline_config_path /data/pipeline.config \\
        --export_dir '/fixture/deepfm/v1_export'
---
apiVersion: "v1"
kind: "Service"
metadata:
  name: "deepfm-export-job-headless"
spec:
  clusterIP: "None"
  ports:
  - name: "torch-distributed"
    port: 29500
    targetPort: 29500
  selector:
    job-name: "deepfm-export-job"
---
apiVersion: "batch/v1"
kind: "Job"
metadata:
  name: "deepfm-export-job"
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
          value: "deepfm-export-job"
        - name: "SERVICE_NAME"
          value: "deepfm-export-job-headless"
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
        image: "sqlrec/tzrec:%s-cpu"
        name: "tzrec-job"
        resources:
          requests:
            cpu: "2"
            memory: "4Gi"
        volumeMounts:
        - mountPath: "/data"
          name: "config-volume"
      restartPolicy: "Never"
      subdomain: "deepfm-export-job-headless"
      volumes:
      - configMap:
          name: "deepfm-export-cm"
        name: "config-volume"
""".formatted(SQLREC_VERSION.getValue());
        assertEquals(expectedYaml, actual);
    }

    @Test
    void defaultServiceYamlMatchesCompleteExpectedYaml() {
        String actual = fullServiceYaml(false);
        String expectedYaml = """
---
apiVersion: "apps/v1"
kind: "Deployment"
metadata:
  name: "deepfm-service"
spec:
  replicas: 1
  selector:
    matchLabels:
      app: "deepfm-service"
  template:
    metadata:
      labels:
        app: "deepfm-service"
    spec:
      containers:
      - command:
        - "bash"
        - "/app/server.sh"
        - "--scripted_model_dir"
        - "/fixture/deepfm/v1_export"
        env:
        - name: "USE_FSSPEC"
          value: "1"
        - name: "USE_SPAWN_MULTI_PROCESS"
          value: "1"
        - name: "USE_FARM_HASH_TO_BUCKETIZE"
          value: "true"
        image: "sqlrec/tzrec:%s-cpu"
        name: "tzrec-service"
        ports:
        - containerPort: 80
          name: "http"
        readinessProbe:
          httpGet:
            path: "/health"
            port: 80
          periodSeconds: 5
          timeoutSeconds: 5
        resources:
          requests:
            cpu: "1"
            memory: "2Gi"
        startupProbe:
          failureThreshold: 120
          httpGet:
            path: "/health"
            port: 80
          periodSeconds: 5
          timeoutSeconds: 5
---
apiVersion: "v1"
kind: "Service"
metadata:
  name: "deepfm-service"
spec:
  ports:
  - name: "server"
    port: 80
    targetPort: 80
  selector:
    app: "deepfm-service"
""".formatted(SQLREC_VERSION.getValue());
        assertEquals(expectedYaml, actual);
    }

    @Test
    void customServiceYamlMatchesCompleteExpectedYaml() {
        String actual = fullServiceYaml(true);
        String expectedYaml = """
---
apiVersion: "apps/v1"
kind: "Deployment"
metadata:
  name: "deepfm-service"
spec:
  replicas: 3
  selector:
    matchLabels:
      app: "deepfm-service"
  template:
    metadata:
      labels:
        app: "deepfm-service"
    spec:
      containers:
      - command:
        - "bash"
        - "/app/server.sh"
        - "--scripted_model_dir"
        - "/fixture/deepfm/v1_export"
        env:
        - name: "USE_FSSPEC"
          value: "1"
        - name: "USE_SPAWN_MULTI_PROCESS"
          value: "1"
        - name: "USE_FARM_HASH_TO_BUCKETIZE"
          value: "true"
        image: "custom/tzrec:v2"
        name: "tzrec-service"
        ports:
        - containerPort: 80
          name: "http"
        readinessProbe:
          httpGet:
            path: "/health"
            port: 80
          periodSeconds: 5
          timeoutSeconds: 5
        resources:
          limits:
            cpu: "8"
            memory: "32Gi"
          requests:
            cpu: "4"
            memory: "16Gi"
        startupProbe:
          failureThreshold: 120
          httpGet:
            path: "/health"
            port: 80
          periodSeconds: 5
          timeoutSeconds: 5
---
apiVersion: "v1"
kind: "Service"
metadata:
  name: "deepfm-service"
spec:
  ports:
  - name: "server"
    port: 80
    targetPort: 80
  selector:
    app: "deepfm-service"
""";
        assertEquals(expectedYaml, actual);
    }

    private ModelConf model() {
        ModelConf model = new ModelConf();
        model.setModelName("deepfm_model");
        model.setInputFields(List.of(new FieldSchema("user_id", "STRING"), new FieldSchema("category", "INT"),
                new FieldSchema("price", "DOUBLE"), new FieldSchema("label", "INT")));
        model.setParams(Map.of("model", "tzrec.deepfm", "label_columns", "label", "num_buckets", "32",
                "embedding_dim", "8", "hidden_units", "16,8"));
        return model;
    }

    private String fullTrainingYaml() {
        ModelTrainConf train = new ModelTrainConf();
        train.setId("deepfm-train");
        train.setModelDir("/fixture/deepfm/v1");
        train.setBaseModelDir("/fixture/deepfm/base");
        train.setTrainDataPaths(List.of("/fixture/train.parquet"));
        train.setParams(Map.of("batch_size", "16", "num_workers", "0", "num_epochs", "2",
                "eval_input_path", "/fixture/eval.parquet", "pod_cpu_cores", "2", "pod_memory", "4Gi"));
        return controller.genModelTrainK8sYaml(model(), train);
    }

    private String fullExportYaml() {
        ModelExportConf export = new ModelExportConf();
        export.setId("deepfm-export");
        export.setCheckpointName("v1");
        export.setBaseModelDir("/fixture/deepfm/v1");
        export.setTrainDataPaths(List.of("/fixture/train.parquet"));
        export.setParams(Map.of("batch_size", "17", "nnodes", "3", "nproc_per_node", "4",
                "pod_cpu_cores", "2", "pod_memory", "4Gi"));
        return controller.genModelExportK8sYaml(model(), export);
    }

    private String fullServiceYaml(boolean custom) {
        ServiceConf service = new ServiceConf();
        service.setId("deepfm-service");
        service.setModelCheckpointDir("/fixture/deepfm/v1_export");
        service.setParams(custom ? Map.of("image", "custom/tzrec", "version", "v2", "replicas", "3",
                "pod_cpu_cores", "4", "pod_memory", "16Gi", "pod_cpu_limit", "8", "pod_memory_limit", "32Gi") : Map.of());
        return controller.getServiceK8sYaml(model(), service);
    }
}
