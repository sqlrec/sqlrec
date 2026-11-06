package com.sqlrec.model.tzrec;

import com.sqlrec.common.model.*;
import com.sqlrec.common.schema.FieldSchema;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static com.sqlrec.common.config.SqlRecConfigs.SQLREC_VERSION;
import static org.junit.jupiter.api.Assertions.*;

class RocketLaunchingModelTest {
    private final RocketLaunchingModel controller = new RocketLaunchingModel();

    private ModelConf model(boolean dense, boolean shared) {
        ModelConf model = new ModelConf();
        List<FieldSchema> fields = new ArrayList<>();
        if (!dense) fields.add(new FieldSchema("category", "INT"));
        fields.add(new FieldSchema("price", "DOUBLE"));
        fields.add(new FieldSchema("label", "INT"));
        model.setInputFields(fields);
        Map<String, String> params = new HashMap<>(Map.of("model", "tzrec.rocket_launching", "label_columns", "label",
                "booster_hidden_units", "16,8", "light_hidden_units", "8,4", "num_buckets", "32",
                "embedding_dim", "8"));
        if (shared) params.put("share_hidden_units", "16");
        model.setParams(params);
        return model;
    }

    private ModelTrainConf train(Map<String, String> overrides) {
        ModelTrainConf train = new ModelTrainConf();
        train.setId("rocket-train"); train.setModelDir("/fixture/model");
        train.setTrainDataPaths(List.of("/fixture/train.parquet"));
        Map<String, String> params = new HashMap<>(Map.of("batch_size", "8", "num_workers", "0", "sparse_lr", "0.01", "dense_lr", "0.01"));
        params.put("task.watch_time.weight", "0.1");
        params.putAll(overrides);
        train.setParams(params);
        return train;
    }

    @Test
    void nativeConfigsMatchTheFixturesUsedForTrainingAndExport() throws Exception {
        for (String name : List.of("rocket_launching", "rocket_launching_shared", "rocket_launching_dense")) {
            ModelConf model = model(name.endsWith("dense"), name.endsWith("shared"));
            assertNull(controller.checkModel(model));
            String config = controller.generateTrainConfig(model, train(Map.of()));
            assertEquals(Files.readString(Path.of("src/test/resources/tzrec", name + ".config")), config);
            assertFalse(config.contains("group_type: WIDE"));
            assertFalse(config.contains("feature_name: \"label\""));
            assertFalse(config.contains("negative_sampler"));
        }
    }

    @Test
    void validatesNativeBranchesAndInapplicableOptions() {
        for (Map<String, String> invalid : List.of(Map.of("light_hidden_units", "4,2"),
                Map.of("booster_hidden_units", "16,0"), Map.of("light_hidden_units", "8,"),
                Map.of("share_hidden_units", ""), Map.of("feature_based_distillation", "false"),
                Map.of("feature_distillation_function", "EUCLID"),
                Map.of("label_columns", "label,price"), Map.of("label_columns", "missing"))) {
            ModelConf model = model(false, false);
            model.getParams().putAll(invalid);
            assertNotNull(controller.checkModel(model), invalid.toString());
        }
        ModelConf model = model(false, false);
        model.getInputFields().get(2).setType("STRING");
        assertNotNull(controller.checkModel(model));
        model = model(false, false);
        model.getInputFields().get(0).setName("PROBS_LIGHT");
        assertNotNull(controller.checkModel(model));
    }

    @Test
    void lifecycleKeepsOneLightOutputAndFreezesModelStructure() {
        ModelConf model = model(false, true);
        ModelExportConf export = new ModelExportConf();
        export.setId("rocket-export"); export.setBaseModelDir("/fixture/model"); export.setCheckpointName("v1");
        ServiceConf service = new ServiceConf();
        service.setId("rocket-service"); service.setModelCheckpointDir("/fixture/model_export");
        for (Map<String, String> overrides : List.of(Map.of("booster_hidden_units", "32,8"),
                Map.of("share_hidden_units", "8"), Map.of("light_hidden_units", "8,2"),
                Map.of("column.category.bucket_size", "64"), Map.of("label_columns", "price"),
                Map.of("model", "tzrec.dssm"), Map.of("feature_based_distillation", "false"))) {
            export.setParams(overrides); service.setParams(overrides);
            assertDoesNotThrow(() -> controller.genModelTrainK8sYaml(model, train(overrides)));
            assertDoesNotThrow(() -> controller.genModelExportK8sYaml(model, export));
            assertDoesNotThrow(() -> controller.getServiceK8sYaml(model, service));
        }
        Map<String, String> runtime = Map.of("batch_size", "16", "dense_lr", "0.02", "booster_hidden_units", "16, 8",
                "enable_train_metrics", "false", "train_metric_decay_rate", "0.8",
                "tensorboard_summaries", "parameter,gradient", "is_profiling", "false");
        String yaml = controller.genModelTrainK8sYaml(model, train(runtime));
        assertTrue(yaml.contains("batch_size: 16"));
        assertFalse(yaml.contains("train_metrics"));
        assertTrue(yaml.contains("tensorboard_summaries: \"parameter\""));
        assertTrue(yaml.contains("tensorboard_summaries: \"gradient\""));
        export.setParams(Map.of("batch_size", "16"));
        assertTrue(controller.genModelExportK8sYaml(model, export).contains("rocket_launching {"));
        service.setParams(Map.of("replicas", "2"));
        assertTrue(controller.getServiceK8sYaml(model, service).contains("replicas: 2"));
        assertEquals(List.of("v1_export"), controller.getExportCheckpoints(export));
        assertEquals(List.of("probs_light"), controller.getOutputFields(model).stream().map(FieldSchema::getName).toList());
        assertTrue(ServiceLoader.load(ModelController.class).stream().anyMatch(provider -> provider.type().equals(RocketLaunchingModel.class)));
    }

    @Test
    void trainYamlMatchesCompleteExpectedYaml() {
        String actual = fullTrainingYaml();
        String expectedYaml = """
---
apiVersion: "v1"
kind: "ConfigMap"
metadata:
  name: "rocket-train-cm"
data:
  pipeline.config: |
    train_input_path: "/fixture/train.parquet"
    model_dir: "/fixture/model"
    train_config {
        sparse_optimizer {
            adagrad_optimizer {
                lr: 0.01
            }
            constant_learning_rate {
            }
        }
        dense_optimizer {
            adam_optimizer {
                lr: 0.01
            }
            constant_learning_rate {
            }
        }
        num_epochs: 1
        use_tensorboard: true
        tensorboard_summaries: "loss"
        tensorboard_summaries: "learning_rate"
        tensorboard_summaries: "global_gradient_norm"
        log_step_count_steps: 100
        save_checkpoints_steps: 1000
        fine_tune_checkpoint: "/fixture/base"
    }
    data_config {
        batch_size: 8
        dataset_type: ParquetDataset
        fg_mode: FG_NORMAL
        label_fields: "label"
        num_workers: 0
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
            group_name: "deep"
            feature_names: "category"
            feature_names: "price"
            group_type: DEEP
        }
        rocket_launching {
            share_mlp { hidden_units: [16] }
            booster_mlp { hidden_units: [16,8] }
            light_mlp { hidden_units: [8,4] }
            feature_based_distillation: true
            feature_distillation_function: COSINE
        }
        num_class: 1
        losses { binary_cross_entropy {} }
        metrics { auc {} }
        metrics { normalized_entropy {} }
        metrics { accuracy {} }
        train_metrics { auc {} }
        train_metrics { accuracy {} }
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
  name: "rocket-train-job-headless"
spec:
  clusterIP: "None"
  ports:
  - name: "torch-distributed"
    port: 29600
    targetPort: 29600
  selector:
    job-name: "rocket-train-job"
---
apiVersion: "batch/v1"
kind: "Job"
metadata:
  name: "rocket-train-job"
spec:
  backoffLimit: 0
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
          value: "rocket-train-job"
        - name: "SERVICE_NAME"
          value: "rocket-train-job-headless"
        - name: "MASTER_PORT"
          value: "29600"
        - name: "NNODES"
          value: "2"
        - name: "NPROC_PER_NODE"
          value: "2"
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
      subdomain: "rocket-train-job-headless"
      volumes:
      - configMap:
          name: "rocket-train-cm"
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
  name: "rocket-export-cm"
data:
  pipeline.config: |
    train_input_path: "/fixture/train.parquet"
    model_dir: "/fixture/model"
    data_config {
        batch_size: 17
        dataset_type: ParquetDataset
        fg_mode: FG_NORMAL
        label_fields: "label"
        num_workers: 1
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
            group_name: "deep"
            feature_names: "category"
            feature_names: "price"
            group_type: DEEP
        }
        rocket_launching {
            share_mlp { hidden_units: [16] }
            booster_mlp { hidden_units: [16,8] }
            light_mlp { hidden_units: [8,4] }
            feature_based_distillation: true
            feature_distillation_function: COSINE
        }
        num_class: 1
        losses { binary_cross_entropy {} }
        metrics { auc {} }
        metrics { normalized_entropy {} }
        metrics { accuracy {} }
        train_metrics { auc {} }
        train_metrics { accuracy {} }
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
        --export_dir '/fixture/model_export'
---
apiVersion: "v1"
kind: "Service"
metadata:
  name: "rocket-export-job-headless"
spec:
  clusterIP: "None"
  ports:
  - name: "torch-distributed"
    port: 29500
    targetPort: 29500
  selector:
    job-name: "rocket-export-job"
---
apiVersion: "batch/v1"
kind: "Job"
metadata:
  name: "rocket-export-job"
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
          value: "rocket-export-job"
        - name: "SERVICE_NAME"
          value: "rocket-export-job-headless"
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
      subdomain: "rocket-export-job-headless"
      volumes:
      - configMap:
          name: "rocket-export-cm"
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
  name: "rocket-service"
spec:
  replicas: 1
  selector:
    matchLabels:
      app: "rocket-service"
  template:
    metadata:
      labels:
        app: "rocket-service"
    spec:
      containers:
      - command:
        - "bash"
        - "/app/server.sh"
        - "--scripted_model_dir"
        - "/fixture/rocket/v1_export"
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
  name: "rocket-service"
spec:
  ports:
  - name: "server"
    port: 80
    targetPort: 80
  selector:
    app: "rocket-service"
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
  name: "rocket-service"
spec:
  replicas: 3
  selector:
    matchLabels:
      app: "rocket-service"
  template:
    metadata:
      labels:
        app: "rocket-service"
    spec:
      containers:
      - command:
        - "bash"
        - "/app/server.sh"
        - "--scripted_model_dir"
        - "/fixture/rocket/v1_export"
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
  name: "rocket-service"
spec:
  ports:
  - name: "server"
    port: 80
    targetPort: 80
  selector:
    app: "rocket-service"
""";
        assertEquals(expectedYaml, actual);
    }

    private String fullTrainingYaml() {
        ModelTrainConf training = train(Map.of("nnodes", "2", "nproc_per_node", "2", "master_port", "29600",
                "pod_cpu_cores", "2", "pod_memory", "4Gi"));
        training.setBaseModelDir("/fixture/base");
        return controller.genModelTrainK8sYaml(model(false, true), training);
    }

    private String fullExportYaml() {
        ModelExportConf export = new ModelExportConf();
        export.setId("rocket-export");
        export.setCheckpointName("v1");
        export.setBaseModelDir("/fixture/model");
        export.setTrainDataPaths(List.of("/fixture/train.parquet"));
        export.setParams(Map.of("batch_size", "17", "nnodes", "3", "nproc_per_node", "4",
                "pod_cpu_cores", "2", "pod_memory", "4Gi"));
        return controller.genModelExportK8sYaml(model(false, true), export);
    }

    private String fullServiceYaml(boolean custom) {
        ServiceConf service = new ServiceConf();
        service.setId("rocket-service");
        service.setModelCheckpointDir("/fixture/rocket/v1_export");
        service.setParams(custom ? Map.of("image", "custom/tzrec", "version", "v2", "replicas", "3",
                "pod_cpu_cores", "4", "pod_memory", "16Gi", "pod_cpu_limit", "8", "pod_memory_limit", "32Gi") : Map.of());
        return controller.getServiceK8sYaml(model(false, true), service);
    }
}
