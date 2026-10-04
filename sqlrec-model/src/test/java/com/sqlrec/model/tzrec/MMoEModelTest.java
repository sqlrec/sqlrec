package com.sqlrec.model.tzrec;

import com.sqlrec.common.model.*;
import com.sqlrec.common.schema.FieldSchema;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static com.sqlrec.common.config.SqlRecConfigs.SQLREC_VERSION;
import static org.junit.jupiter.api.Assertions.*;

class MMoEModelTest {
    private final MMoEModel controller = new MMoEModel();

    private ModelConf model(boolean mixed, boolean dense) {
        ModelConf model = new ModelConf();
        List<FieldSchema> fields = new ArrayList<>();
        if (!dense) fields.add(new FieldSchema("category", "INT"));
        fields.add(new FieldSchema("price", "DOUBLE"));
        fields.add(new FieldSchema("label", "INT"));
        fields.add(new FieldSchema(mixed ? "watch_time" : "label_like", mixed ? "DOUBLE" : "INT"));
        model.setInputFields(fields);
        Map<String, String> params = new HashMap<>(Map.of("model", "tzrec.mmoe", "label_columns", mixed ? "label,watch_time" : "label,label_like"));
        params.putAll(Map.of("num_expert", "2", "expert_hidden_units", "16,8", "task_hidden_units", "8,8",
                "num_buckets", "32", "embedding_dim", "8", "batch_size", "8", "num_workers", "0", "sparse_lr", "0.01", "dense_lr", "0.01"));
        if (mixed) params.putAll(Map.of("task.watch_time.type", "regression", "task.watch_time.weight", "0.1"));
        model.setParams(params);
        return model;
    }

    private ModelTrainConf train(Map<String, String> overrides) {
        ModelTrainConf train = new ModelTrainConf();
        train.setId("mmoe-train"); train.setModelDir("/fixture/model");
        train.setTrainDataPaths(List.of("/fixture/train.parquet")); train.setParams(overrides);
        return train;
    }

    @Test
    void trainYamlMatchesTheCompleteExpectedYaml() throws Exception {
        ModelTrainConf train = train(Map.of("batch_size", "16", "dense_lr", "0.02", "num_epochs", "2",
                "eval_input_path", "/fixture/eval.parquet", "nnodes", "2", "nproc_per_node", "2",
                "master_port", "29600", "pod_cpu_cores", "2", "pod_memory", "4Gi"));
        train.setBaseModelDir("/fixture/base");
        String expectedYaml = """
---
apiVersion: "v1"
kind: "ConfigMap"
metadata:
  name: "mmoe-train-cm"
data:
  pipeline.config: |
    train_input_path: "/fixture/train.parquet"
    eval_input_path: "/fixture/eval.parquet"
    eval_config {}
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
                lr: 0.02
            }
            constant_learning_rate {
            }
        }
        num_epochs: 2
        fine_tune_checkpoint: "/fixture/base"
    }
    data_config {
        batch_size: 16
        dataset_type: ParquetDataset
        fg_mode: FG_NORMAL
        label_fields: "label"
        label_fields: "watch_time"
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
        mmoe {
            num_expert: 2
            expert_mlp { hidden_units: [16,8] }
            task_towers {
                tower_name: "label"
                label_name: "label"
                num_class: 1
                mlp { hidden_units: [8,8] }
                weight: 1.0
                losses { binary_cross_entropy {} }
                metrics { auc {} }
            }
            task_towers {
                tower_name: "watch_time"
                label_name: "watch_time"
                num_class: 1
                mlp { hidden_units: [8,8] }
                weight: 0.1
                losses { l2_loss {} }
                metrics { mean_squared_error {} }
            }
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
        --pipeline_config_path /data/pipeline.config
---
apiVersion: "v1"
kind: "Service"
metadata:
  name: "mmoe-train-job-headless"
spec:
  clusterIP: "None"
  ports:
  - name: "torch-distributed"
    port: 29600
    targetPort: 29600
  selector:
    job-name: "mmoe-train-job"
---
apiVersion: "batch/v1"
kind: "Job"
metadata:
  name: "mmoe-train-job"
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
          value: "mmoe-train-job"
        - name: "SERVICE_NAME"
          value: "mmoe-train-job-headless"
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
      subdomain: "mmoe-train-job-headless"
      volumes:
      - configMap:
          name: "mmoe-train-cm"
        name: "config-volume"
""".formatted(SQLREC_VERSION.getValue());
        assertEquals(expectedYaml, controller.genModelTrainK8sYaml(model(true, false), train));
    }

    @Test
    void exportYamlMatchesTheCompleteExpectedYamlAndUsesOneProcess() throws Exception {
        ModelConf model = model(true, false);
        model.getParams().putAll(Map.of("nnodes", "2", "nproc_per_node", "2"));
        ModelExportConf export = new ModelExportConf();
        export.setId("mmoe-export");
        export.setBaseModelDir("/fixture/model");
        export.setCheckpointName("v1");
        export.setParams(Map.of("batch_size", "16"));
        String expectedYaml = """
---
apiVersion: "v1"
kind: "ConfigMap"
metadata:
  name: "mmoe-export-cm"
data:
  pipeline.config: |
    model_dir: "/fixture/model"
    data_config {
        batch_size: 16
        dataset_type: ParquetDataset
        fg_mode: FG_NORMAL
        label_fields: "label"
        label_fields: "watch_time"
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
        mmoe {
            num_expert: 2
            expert_mlp { hidden_units: [16,8] }
            task_towers {
                tower_name: "label"
                label_name: "label"
                num_class: 1
                mlp { hidden_units: [8,8] }
                weight: 1.0
                losses { binary_cross_entropy {} }
                metrics { auc {} }
            }
            task_towers {
                tower_name: "watch_time"
                label_name: "watch_time"
                num_class: 1
                mlp { hidden_units: [8,8] }
                weight: 0.1
                losses { l2_loss {} }
                metrics { mean_squared_error {} }
            }
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
        --export_dir '/fixture/model_export'
---
apiVersion: "v1"
kind: "Service"
metadata:
  name: "mmoe-export-job-headless"
spec:
  clusterIP: "None"
  ports:
  - name: "torch-distributed"
    port: 29500
    targetPort: 29500
  selector:
    job-name: "mmoe-export-job"
---
apiVersion: "batch/v1"
kind: "Job"
metadata:
  name: "mmoe-export-job"
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
          value: "mmoe-export-job"
        - name: "SERVICE_NAME"
          value: "mmoe-export-job-headless"
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
            cpu: "1"
            memory: "2Gi"
        volumeMounts:
        - mountPath: "/data"
          name: "config-volume"
      restartPolicy: "Never"
      subdomain: "mmoe-export-job-headless"
      volumes:
      - configMap:
          name: "mmoe-export-cm"
        name: "config-volume"
""".formatted(SQLREC_VERSION.getValue());
        assertEquals(expectedYaml, controller.genModelExportK8sYaml(model, export));
    }

    @Test
    void serviceYamlMatchesTheCompleteExpectedYaml() throws Exception {
        ServiceConf service = new ServiceConf();
        service.setId("mmoe-service");
        service.setModelCheckpointDir("/fixture/model_export");
        service.setParams(Map.of("replicas", "3", "pod_cpu_cores", "4", "pod_memory", "16Gi",
                "pod_cpu_limit", "8", "pod_memory_limit", "32Gi"));
        String expectedYaml = """
---
apiVersion: "apps/v1"
kind: "Deployment"
metadata:
  name: "mmoe-service"
spec:
  replicas: 3
  selector:
    matchLabels:
      app: "mmoe-service"
  template:
    metadata:
      labels:
        app: "mmoe-service"
    spec:
      containers:
      - command:
        - "bash"
        - "/app/server.sh"
        - "--scripted_model_dir"
        - "/fixture/model_export"
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
  name: "mmoe-service"
spec:
  ports:
  - name: "server"
    port: 80
    targetPort: 80
  selector:
    app: "mmoe-service"
""".formatted(SQLREC_VERSION.getValue());
        assertEquals(expectedYaml, controller.getServiceK8sYaml(model(true, false), service));
    }

    @Test
    void generatedConfigsMatchFixturesParsedAndTrainedByTheImageGate() throws Exception {
        for (String name : List.of("mmoe", "mmoe_mixed", "mmoe_dense")) {
            ModelConf model = model(name.equals("mmoe_mixed"), name.equals("mmoe_dense"));
            assertNull(controller.checkModel(model));
            String config = controller.generateTrainConfig(model, train(Map.of()));
            assertEquals(Files.readString(Path.of("src/test/resources/tzrec", name + ".config")), config);
            assertFalse(config.contains("group_type: WIDE"));
            assertFalse(config.contains("feature_name: \"label\""));
            assertEquals(List.of("probs_label", name.equals("mmoe_mixed") ? "y_watch_time" : "probs_label_like"),
                    controller.getOutputFields(model).stream().map(FieldSchema::getName).toList());
        }
    }

    @Test
    void validatesLabelsTypesMetricsWeightsAndUnknownOptions() {
        for (Map<String, String> invalid : List.of(
                Map.of("label_columns", "label"), Map.of("label_columns", "label,label"),
                Map.of("label_columns", "label,LABEL"), Map.of("label_columns", "label,missing"),
                Map.of("label_columns", "label,label_like,"), Map.of("task.missing.type", "binary"),
                Map.of("task.label.foo", "1"), Map.of("task.label.type", "multi_class"),
                Map.of("task.label.metrics", "mean_squared_error"), Map.of("task.label.metrics", "auc,auc"),
                Map.of("task.label.weight", "NaN"), Map.of("task.label.weight", "0"),
                Map.of("task.label.weight", "1e40"), Map.of("num_expert", "0"),
                Map.of("expert_hidden_units", "16,0"), Map.of("task_hidden_units", "8,"),
                Map.of("column.label.bucket_size", "3"), Map.of("hidden_units", "8"))) {
            ModelConf model = model(false, false);
            model.getParams().putAll(invalid);
            assertNotNull(controller.checkModel(model), invalid.toString());
        }
        ModelConf model = model(true, false);
        model.getParams().put("task.watch_time.metrics", "auc");
        assertNotNull(controller.checkModel(model));
        model = model(false, false);
        model.getInputFields().get(2).setType("ARRAY<INT>");
        assertNotNull(controller.checkModel(model));
        model = model(false, false);
        model.getInputFields().get(0).setName("PROBS_LABEL");
        assertNotNull(controller.checkModel(model));
    }

    @Test
    void regressionRequiresFloatingLabelsWhileBinaryAllowsNumericLabels() {
        for (String type : List.of("INT", "BIGINT", "FLOAT", "DOUBLE")) {
            ModelConf model = model(true, false);
            model.getInputFields().get(3).setType(type);
            if (Set.of("FLOAT", "DOUBLE").contains(type)) assertNull(controller.checkModel(model));
            else assertTrue(controller.checkModel(model).contains("Regression label must be FLOAT or DOUBLE"));
            model.getParams().remove("task.watch_time.type");
            assertNull(controller.checkModel(model));
        }
    }

    @Test
    void allRegressionTasksDeclareRegressionLossesAndOutputs() {
        ModelConf model = model(true, false);
        model.getInputFields().get(2).setType("FLOAT");
        model.getParams().put("task.label.type", "regression");
        assertNull(controller.checkModel(model));
        assertEquals(List.of("y_label", "y_watch_time"),
                controller.getOutputFields(model).stream().map(FieldSchema::getName).toList());
        String config = controller.generateTrainConfig(model, train(Map.of()));
        assertFalse(config.contains("binary_cross_entropy"));
        assertTrue(config.contains("l2_loss {}"));
    }

    @Test
    void runtimeOverridesPreserveTheDeclaredContractForTrainExportAndService() {
        ModelConf model = model(false, false);
        ModelExportConf export = new ModelExportConf();
        export.setId("mmoe-export"); export.setBaseModelDir("/fixture/model"); export.setCheckpointName("v1");
        ServiceConf service = new ServiceConf();
        service.setId("mmoe-service"); service.setModelCheckpointDir("/fixture/model_export");
        for (Map<String, String> overrides : List.of(Map.of("label_columns", "label_like,label"),
                Map.of("task.label.type", "regression"), Map.of("task.label.weight", "2"),
                Map.of("expert_hidden_units", "32,8"), Map.of("num_expert", "4"),
                Map.of("column.category.bucket_size", "64"))) {
            export.setParams(overrides); service.setParams(overrides);
            assertThrows(IllegalArgumentException.class, () -> controller.genModelTrainK8sYaml(model, train(overrides)));
            assertThrows(IllegalArgumentException.class, () -> controller.genModelExportK8sYaml(model, export));
            assertThrows(IllegalArgumentException.class, () -> controller.getServiceK8sYaml(model, service));
        }
        Map<String, String> runtime = Map.of("batch_size", "16", "dense_lr", "0.02", "eval_input_path", "/fixture/eval.parquet",
                "task.label.weight", "1", "task_hidden_units", "8, 8");
        String yaml = controller.genModelTrainK8sYaml(model, train(runtime));
        assertTrue(yaml.contains("eval_input_path:"));
        assertTrue(yaml.contains("batch_size: 16"));
        export.setParams(Map.of("batch_size", "16"));
        assertTrue(controller.genModelExportK8sYaml(model, export).contains("mmoe {"));
        assertEquals(List.of("v1_export"), controller.getExportCheckpoints(export));
    }

    @Test
    void defaultTwoBinaryTasksAndPerTaskOverridesShareTheOutputContract() {
        ModelConf model = model(false, false);
        model.setParams(new HashMap<>(Map.of("label_columns", "label,label_like")));
        assertNull(controller.checkModel(model));
        assertEquals(2, controller.getOutputFields(model).size());
        model.getParams().putAll(Map.of("task.label_like.hidden_units", "32,16", "task.label_like.metrics", "auc,accuracy"));
        String config = controller.generateTrainConfig(model, train(Map.of()));
        assertTrue(config.contains("hidden_units: [32,16]"));
        assertTrue(config.contains("metrics { accuracy {} }"));
        assertTrue(ServiceLoader.load(ModelController.class).stream()
                .anyMatch(provider -> provider.type().equals(MMoEModel.class)));
    }
}
