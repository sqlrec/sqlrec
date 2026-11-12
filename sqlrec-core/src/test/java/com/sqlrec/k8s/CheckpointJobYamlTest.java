package com.sqlrec.k8s;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.config.ConfigOption;
import com.sqlrec.common.config.ModelConfigs;
import com.sqlrec.common.model.ModelConf;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.db.HdfsAccess;
import com.sqlrec.db.MetadataAccess;
import com.sqlrec.db.MetadataAccessFactory;
import com.sqlrec.db.SchemaAccess;
import com.sqlrec.db.local.InMemoryStoreAccess;
import com.sqlrec.entity.Checkpoint;
import com.sqlrec.model.ModelManager;
import com.sqlrec.sql.parser.SqlCreateModel;
import com.sqlrec.sql.parser.SqlExportModel;
import com.sqlrec.sql.parser.SqlTrainModel;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.mockito.MockedStatic;

import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/** Exercises SQL -> task ID -> persisted/submitted YAML with real model controllers. */
class CheckpointJobYamlTest {
    private static final String TASK_OPTIONS = "'NAMESPACE'='test', 'version'='test-version', "
            + "'kubernetes.env.JAVA_HOME'='/fixtures/java', 'kubernetes.env.HADOOP_HOME'='/fixtures/hadoop', "
            + "'kubernetes.env.CLASSPATH'='/fixtures/classpath', "
            + "'kubernetes.env.HADOOP_CONF_DIR'='/fixtures/hadoop/conf', "
            + "'kubernetes.env.CLIENT_DIR'='/fixtures/client'";
    private static final List<String> BACKENDS = List.of(
            "gbdt.lightgbm", "gbdt.xgboost", "gbdt.catboost",
            "tzrec.wide_and_deep", "tzrec.deepfm", "tzrec.dssm", "tzrec.mmoe", "tzrec.rocket_launching",
            "huggingface.transformers");

    @Test
    void namespaceInSqlIsPassedToDryRunAndRejectionDoesNotSaveCheckpoint() throws Exception {
        try (Fixture f = new Fixture("tzrec.wide_and_deep", "rank_model", false)) {
            var statement = (SqlTrainModel) CompileManager.parseSql(
                    "TRAIN MODEL rank_model CHECKPOINT='v4' ON training WITH ("
                            + TASK_OPTIONS.replace("'NAMESPACE'='test'", "'NAMESPACE'='${NAMESPACE}'") + ")");

            RuntimeException rejection = new RuntimeException("Failed to validate YAML: invalid namespace");
            f.k8s.when(() -> K8sManager.validateYaml(anyString())).thenAnswer(call -> {
                assertTrue(K8sYamlUtils.parseK8sYaml(call.getArgument(0)).stream()
                        .allMatch(resource -> "${NAMESPACE}".equals(resource.getMetadata().getNamespace())));
                throw rejection;
            });

            var failure = assertThrows(RuntimeException.class,
                    () -> ModelManager.trainModel(statement, "default"));

            assertSame(rejection, failure);
            assertNull(f.db.getCheckpoint("rank_model", "v4"));
            f.k8s.verify(() -> K8sManager.validateYaml(anyString()));
            f.k8s.verifyNoMoreInteractions();
        }
    }

    @Test
    void failedSubmissionIsMarkedFailedAndRetryUsesNewSqlAndJob() throws Exception {
        try (Fixture f = new Fixture("gbdt.lightgbm", "rank_model", false)) {
            RuntimeException submissionFailure = new RuntimeException("apply failed");
            f.k8s.when(() -> K8sManager.applyYaml(anyString())).thenThrow(submissionFailure);
            var statement = (SqlTrainModel) CompileManager.parseSql(
                    "TRAIN MODEL rank_model CHECKPOINT='v4' ON training WITH (" + TASK_OPTIONS + ")");

            assertSame(submissionFailure, assertThrows(RuntimeException.class,
                    () -> ModelManager.trainModel(statement, "default")));
            Checkpoint failed = f.db.getCheckpoint("rank_model", "v4");
            assertEquals(Consts.CHECKPOINT_STATUS_FAILED, failed.getStatus());
            assertNotNull(failed.getYaml());

            f.k8s.when(() -> K8sManager.applyYaml(anyString())).thenAnswer(call -> null);
            var corrected = (SqlTrainModel) CompileManager.parseSql(
                    "TRAIN MODEL rank_model CHECKPOINT='v4' ON training WITH (" + TASK_OPTIONS
                            + ", 'learning_rate'='0.03')");
            ModelManager.trainModel(corrected, "default");

            Checkpoint retried = f.db.getCheckpoint("rank_model", "v4");
            assertEquals(Consts.CHECKPOINT_STATUS_CREATED, retried.getStatus());
            assertEquals(CompileManager.getSqlStr(corrected), retried.getDdl());
            assertTrue(retried.getYaml().contains("\"learning_rate\": 0.03"));
            assertNotEquals(K8sYamlUtils.parseK8sYamlAndGetJobs(failed.getYaml()).getFirst().getMetadata().getName(),
                    K8sYamlUtils.parseK8sYamlAndGetJobs(retried.getYaml()).getFirst().getMetadata().getName());
            f.k8s.verify(() -> K8sManager.deleteYamlAndWait(failed.getYaml()));
            f.k8s.verify(() -> K8sManager.applyYaml(retried.getYaml()));
            verify(f.files, never()).deletePath(anyString());
        }
    }

    @Test
    void submissionFailureDoesNotFailAConcurrentReplacement() throws Exception {
        try (Fixture f = new Fixture("gbdt.lightgbm", "rank_model", false)) {
            Checkpoint replacement = new Checkpoint();
            replacement.setModelName("rank_model");
            replacement.setCheckpointName("v4");
            replacement.setStatus(Consts.CHECKPOINT_STATUS_CREATED);
            replacement.setYaml("replacement-yaml");
            replacement.setUpdatedAt(999L);
            RuntimeException failure = new RuntimeException("old submission failed late");
            f.k8s.when(() -> K8sManager.applyYaml(anyString())).thenAnswer(call -> {
                f.db.upsertCheckpoint(replacement);
                throw failure;
            });

            assertSame(failure, assertThrows(RuntimeException.class, () -> f.train("v4")));
            assertSame(replacement, f.db.getCheckpoint("rank_model", "v4"));
            assertEquals(Consts.CHECKPOINT_STATUS_CREATED, replacement.getStatus());
        }
    }

    @Test
    void retriesAndRecreationUseDisjointResourcesWithRealModelControllers() throws Exception {
        try (Fixture f = new Fixture("gbdt.lightgbm", "rank_model", false)) {
            String previous = f.train("v1.0");
            assertTrue(f.db.completeCheckpoint(f.db.getCheckpoint("rank_model", "v1.0"), Consts.CHECKPOINT_STATUS_FAILED));
            String retried = f.train("v1.0");
            f.db.deleteCheckpoint("rank_model", "v1.0");
            String recreated = f.train("v1.0");
            Set<String> names = new java.util.HashSet<>();
            for (String yaml : List.of(previous, retried, recreated)) {
                var job = K8sYamlUtils.parseK8sYamlAndGetJobs(yaml).getFirst();
                String jobName = job.getMetadata().getName();
                assertTaskResources(yaml, jobName.substring(0, jobName.length() - "-job".length()), false);
                for (HasMetadata resource : K8sYamlUtils.parseK8sYaml(yaml)) {
                    assertTrue(names.add(resource.getMetadata().getName()), "Attempts must not share resource names");
                }
            }
        }
    }

    @Test
    void lightgbmTrainingSqlMatchesCompleteYaml() throws Exception {
        String expectedYaml = """
            ---
            apiVersion: "v1"
            kind: "ConfigMap"
            metadata:
              name: "train-rank-model-v1-0-45da89d352a064bb-cm"
              namespace: "test"
            data:
              pipeline.config: |
                {
                  "model_type": "lightgbm",
                  "train_input_path": "file:///fixtures/training/*",
                  "model_dir": "file:///fixtures/models/rank_model/v1.0",
                  "base_model_dir": "",
                  "label_columns": "label",
                  "feature_columns": [
                    "feature"
                  ],
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
              name: "train-rank-model-v1-0-45da89d352a064bb-job"
              namespace: "test"
            spec:
              backoffLimit: 1
              template:
                spec:
                  containers:
                  - command:
                    - "bash"
                    - "/data/start.sh"
                    env:
                    - name: "HADOOP_CONF_DIR"
                      value: "/fixtures/hadoop/conf"
                    - name: "HADOOP_HOME"
                      value: "/fixtures/hadoop"
                    - name: "JAVA_HOME"
                      value: "/fixtures/java"
                    - name: "CLIENT_DIR"
                      value: "/fixtures/client"
                    - name: "CLASSPATH"
                      value: "/fixtures/classpath"
                    image: "sqlrec/gbdt:test-version"
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
                      name: "train-rank-model-v1-0-45da89d352a064bb-cm"
                    name: "config-volume"
            """;
        try (Fixture f = new Fixture("gbdt.lightgbm", "rank_model")) {
            assertEquals(expectedYaml, f.train("v1.0"));
        }
    }

    @Test
    void lightgbmExportSqlMatchesCompleteYaml() throws Exception {
        String expectedYaml = """
            ---
            apiVersion: "v1"
            kind: "ConfigMap"
            metadata:
              name: "export-rank-model-v1-0-7ecfdfa7c6e2a66f-cm"
              namespace: "test"
            data:
              pipeline.config: |
                {
                  "base_model_dir": "file:///fixtures/models/rank_model/v1.0",
                  "export_dir": "file:///fixtures/models/rank_model/v1.0_export"
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
              name: "export-rank-model-v1-0-7ecfdfa7c6e2a66f-job"
              namespace: "test"
            spec:
              backoffLimit: 1
              template:
                spec:
                  containers:
                  - command:
                    - "bash"
                    - "/data/start.sh"
                    env:
                    - name: "HADOOP_CONF_DIR"
                      value: "/fixtures/hadoop/conf"
                    - name: "HADOOP_HOME"
                      value: "/fixtures/hadoop"
                    - name: "JAVA_HOME"
                      value: "/fixtures/java"
                    - name: "CLIENT_DIR"
                      value: "/fixtures/client"
                    - name: "CLASSPATH"
                      value: "/fixtures/classpath"
                    image: "sqlrec/gbdt:test-version"
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
                      name: "export-rank-model-v1-0-7ecfdfa7c6e2a66f-cm"
                    name: "config-volume"
            """;
        try (Fixture f = new Fixture("gbdt.lightgbm", "rank_model")) {
            assertEquals(expectedYaml, f.export("v1.0"));
        }
    }

    @Test
    void dssmTrainingSqlMatchesCompleteYaml() throws Exception {
        String expectedYaml = """
            ---
            apiVersion: "v1"
            kind: "ConfigMap"
            metadata:
              name: "train-rank-model-v1-0-45da89d352a064bb-cm"
              namespace: "test"
            data:
              pipeline.config: |
                train_input_path: "file:///fixtures/training/*.parquet"
                model_dir: "file:///fixtures/models/rank_model/v1.0"
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
                    use_tensorboard: true
                    tensorboard_summaries: "loss"
                    tensorboard_summaries: "learning_rate"
                    tensorboard_summaries: "global_gradient_norm"
                    log_step_count_steps: 100
                    save_checkpoints_steps: 1000
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
                        feature_name: "uid"
                        expression: "item:uid"
                        hash_bucket_size: 1000000
                        embedding_dim: 16
                    }
                }
                feature_configs {
                    id_feature {
                        feature_name: "iid"
                        expression: "item:iid"
                        hash_bucket_size: 1000000
                        embedding_dim: 16
                    }
                }
                model_config {
                    feature_groups {
                        group_name: "user"
                        feature_names: "uid"
                        group_type: DEEP
                    }
                    feature_groups {
                        group_name: "item"
                        feature_names: "iid"
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
                    metrics {
                        recall_at_k {
                            top_k: 10
                        }
                    }
                    metrics {
                        recall_at_k {
                            top_k: 20
                        }
                    }
                    train_metrics { recall_at_k { top_k: 1 } }
                    train_metrics { recall_at_k { top_k: 5 } }
                    train_metrics { recall_at_k { top_k: 10 } }
                    train_metrics { recall_at_k { top_k: 20 } }
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
                    /app/run.py --mode train \\
                    --pipeline_config_path /data/pipeline.config --continue_train --restore_optimizer true
            ---
            apiVersion: "v1"
            kind: "Service"
            metadata:
              name: "train-rank-model-v1-0-45da89d352a064bb-job-headless"
              namespace: "test"
            spec:
              clusterIP: "None"
              ports:
              - name: "torch-distributed"
                port: 29500
                targetPort: 29500
              selector:
                job-name: "train-rank-model-v1-0-45da89d352a064bb-job"
            ---
            apiVersion: "batch/v1"
            kind: "Job"
            metadata:
              name: "train-rank-model-v1-0-45da89d352a064bb-job"
              namespace: "test"
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
                      value: "train-rank-model-v1-0-45da89d352a064bb-job"
                    - name: "SERVICE_NAME"
                      value: "train-rank-model-v1-0-45da89d352a064bb-job-headless"
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
                    - name: "HADOOP_CONF_DIR"
                      value: "/fixtures/hadoop/conf"
                    - name: "HADOOP_HOME"
                      value: "/fixtures/hadoop"
                    - name: "JAVA_HOME"
                      value: "/fixtures/java"
                    - name: "CLIENT_DIR"
                      value: "/fixtures/client"
                    - name: "CLASSPATH"
                      value: "/fixtures/classpath"
                    image: "sqlrec/tzrec:test-version"
                    name: "tzrec-job"
                    resources:
                      requests:
                        cpu: "1"
                        memory: "2Gi"
                    volumeMounts:
                    - mountPath: "/data"
                      name: "config-volume"
                  restartPolicy: "Never"
                  subdomain: "train-rank-model-v1-0-45da89d352a064bb-job-headless"
                  volumes:
                  - configMap:
                      name: "train-rank-model-v1-0-45da89d352a064bb-cm"
                    name: "config-volume"
            """;
        try (Fixture f = new Fixture("tzrec.dssm", "rank_model")) {
            assertEquals(expectedYaml, f.train("v1.0"));
        }
    }

    @Test
    void dssmExportSqlMatchesCompleteYaml() throws Exception {
        String expectedYaml = """
            ---
            apiVersion: "v1"
            kind: "ConfigMap"
            metadata:
              name: "export-rank-model-v1-0-7ecfdfa7c6e2a66f-cm"
              namespace: "test"
            data:
              pipeline.config: |
                model_dir: "file:///fixtures/models/rank_model/v1.0"
                data_config {
                    batch_size: 8192
                    dataset_type: ParquetDataset
                    fg_mode: FG_NORMAL
                    label_fields: "label"
                    num_workers: 1
                }
                feature_configs {
                    id_feature {
                        feature_name: "uid"
                        expression: "item:uid"
                        hash_bucket_size: 1000000
                        embedding_dim: 16
                    }
                }
                feature_configs {
                    id_feature {
                        feature_name: "iid"
                        expression: "item:iid"
                        hash_bucket_size: 1000000
                        embedding_dim: 16
                    }
                }
                model_config {
                    feature_groups {
                        group_name: "user"
                        feature_names: "uid"
                        group_type: DEEP
                    }
                    feature_groups {
                        group_name: "item"
                        feature_names: "iid"
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
                    metrics {
                        recall_at_k {
                            top_k: 10
                        }
                    }
                    metrics {
                        recall_at_k {
                            top_k: 20
                        }
                    }
                    train_metrics { recall_at_k { top_k: 1 } }
                    train_metrics { recall_at_k { top_k: 5 } }
                    train_metrics { recall_at_k { top_k: 10 } }
                    train_metrics { recall_at_k { top_k: 20 } }
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
                    /app/run.py --mode export \\
                    --pipeline_config_path /data/pipeline.config \\
                    --export_dir 'file:///fixtures/models/rank_model/v1.0_export'
            ---
            apiVersion: "v1"
            kind: "Service"
            metadata:
              name: "export-rank-model-v1-0-7ecfdfa7c6e2a66f-job-headless"
              namespace: "test"
            spec:
              clusterIP: "None"
              ports:
              - name: "torch-distributed"
                port: 29500
                targetPort: 29500
              selector:
                job-name: "export-rank-model-v1-0-7ecfdfa7c6e2a66f-job"
            ---
            apiVersion: "batch/v1"
            kind: "Job"
            metadata:
              name: "export-rank-model-v1-0-7ecfdfa7c6e2a66f-job"
              namespace: "test"
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
                      value: "export-rank-model-v1-0-7ecfdfa7c6e2a66f-job"
                    - name: "SERVICE_NAME"
                      value: "export-rank-model-v1-0-7ecfdfa7c6e2a66f-job-headless"
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
                    - name: "HADOOP_CONF_DIR"
                      value: "/fixtures/hadoop/conf"
                    - name: "HADOOP_HOME"
                      value: "/fixtures/hadoop"
                    - name: "JAVA_HOME"
                      value: "/fixtures/java"
                    - name: "CLIENT_DIR"
                      value: "/fixtures/client"
                    - name: "CLASSPATH"
                      value: "/fixtures/classpath"
                    image: "sqlrec/tzrec:test-version"
                    name: "tzrec-job"
                    resources:
                      requests:
                        cpu: "1"
                        memory: "2Gi"
                    volumeMounts:
                    - mountPath: "/data"
                      name: "config-volume"
                  restartPolicy: "Never"
                  subdomain: "export-rank-model-v1-0-7ecfdfa7c6e2a66f-job-headless"
                  volumes:
                  - configMap:
                      name: "export-rank-model-v1-0-7ecfdfa7c6e2a66f-cm"
                    name: "config-volume"
            """;
        try (Fixture f = new Fixture("tzrec.dssm", "rank_model")) {
            assertEquals(expectedYaml, f.export("v1.0"));
        }
    }

    @Test
    void huggingFaceTrainingSqlMatchesCompleteYaml() throws Exception {
        String expectedYaml = """
            ---
            apiVersion: "v1"
            kind: "ConfigMap"
            metadata:
              name: "train-rank-model-v1-0-45da89d352a064bb-cm"
              namespace: "test"
            data:
              pipeline.config: |
                {
                  "framework": "huggingface.transformers",
                  "model_name": "rank_model",
                  "checkpoint_name": "v1.0",
                  "repo_id": "org/test-model",
                  "revision": "main",
                  "task": "text-classification",
                  "trust_remote_code": false,
                  "allow_patterns": [],
                  "ignore_patterns": [],
                  "force_download": false,
                  "input_fields": [
                    {
                      "name": "text",
                      "type": "STRING"
                    }
                  ],
                  "model_params": {
                    "task": "text-classification",
                    "repo_id": "org/test-model",
                    "MODEL_PATH": "file:///fixtures/models/rank_model",
                    "model": "huggingface.transformers",
                    "text_column": "text",
                    "NAMESPACE": "test",
                    "kubernetes.env.HADOOP_CONF_DIR": "/fixtures/hadoop/conf",
                    "kubernetes.env.CLASSPATH": "/fixtures/classpath",
                    "kubernetes.env.CLIENT_DIR": "/fixtures/client",
                    "kubernetes.env.HADOOP_HOME": "/fixtures/hadoop",
                    "kubernetes.env.JAVA_HOME": "/fixtures/java",
                    "version": "test-version"
                  }
                }
              start.sh: |
                #!/bin/bash
                set -euo pipefail
                export PYTHONPATH=/app:${PYTHONPATH:-}
                LOCAL_MODEL_DIR=${LOCAL_MODEL_DIR:-/tmp/huggingface_model}
                rm -rf "$LOCAL_MODEL_DIR"
                python -m huggingface.download --pipeline_config_path /data/pipeline.config --output_dir "$LOCAL_MODEL_DIR"
                HADOOP="${HADOOP_HOME}/bin/hadoop"
                MODEL_BASE_DIR='file:///fixtures/models/rank_model'
                MODEL_DIR='file:///fixtures/models/rank_model/v1.0'
                STAGING_DIR='file:///fixtures/models/rank_model/v1.0.__uploading__.train-rank-model-v1-0-45da89d352a064bb'
                "$HADOOP" fs -mkdir -p "$MODEL_BASE_DIR"
                "$HADOOP" fs -rm -r -f "$STAGING_DIR"
                "$HADOOP" fs -put -f "$LOCAL_MODEL_DIR" "$STAGING_DIR"
                "$HADOOP" fs -touchz "$STAGING_DIR/_SUCCESS"
                if "$HADOOP" fs -test -e "$MODEL_DIR"; then
                    echo "Checkpoint directory already exists: $MODEL_DIR" >&2
                    exit 1
                fi
                "$HADOOP" fs -mv "$STAGING_DIR" "$MODEL_DIR"
            ---
            apiVersion: "batch/v1"
            kind: "Job"
            metadata:
              name: "train-rank-model-v1-0-45da89d352a064bb-job"
              namespace: "test"
            spec:
              backoffLimit: 1
              template:
                spec:
                  containers:
                  - command:
                    - "bash"
                    - "/data/start.sh"
                    env:
                    - name: "HADOOP_CONF_DIR"
                      value: "/fixtures/hadoop/conf"
                    - name: "HADOOP_HOME"
                      value: "/fixtures/hadoop"
                    - name: "JAVA_HOME"
                      value: "/fixtures/java"
                    - name: "CLIENT_DIR"
                      value: "/fixtures/client"
                    - name: "CLASSPATH"
                      value: "/fixtures/classpath"
                    image: "sqlrec/transformers:test-version"
                    name: "transformers-download"
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
                      name: "train-rank-model-v1-0-45da89d352a064bb-cm"
                    name: "config-volume"
            """;
        try (Fixture f = new Fixture("huggingface.transformers", "rank_model")) {
            assertEquals(expectedYaml, f.train("v1.0"));
        }
    }

    @TestFactory
    Stream<DynamicTest> trainingSqlProducesExpectedResourceNames() {
        return BACKENDS.stream().map(backend -> DynamicTest.dynamicTest(backend, () -> {
            try (Fixture f = new Fixture(backend, "rank_model")) {
                assertTaskResources(f.train("v1.0"),
                        "train-rank-model-v1-0-45da89d352a064bb", backend.startsWith("tzrec."));
            }
        }));
    }

    @TestFactory
    Stream<DynamicTest> exportSqlProducesExpectedResourceNames() {
        return BACKENDS.stream().filter(backend -> !backend.equals("huggingface.transformers"))
                .map(backend -> DynamicTest.dynamicTest(backend, () -> {
                    try (Fixture f = new Fixture(backend, "rank_model")) {
                        assertTaskResources(f.export("v1.0"),
                                "export-rank-model-v1-0-7ecfdfa7c6e2a66f", backend.startsWith("tzrec."));
                    }
                }));
    }

    @TestFactory
    Stream<DynamicTest> trainingSqlRejectsUnregisteredPathsForEveryBackend() {
        return BACKENDS.stream().map(backend -> DynamicTest.dynamicTest(backend, () -> {
            try (Fixture f = new Fixture(backend, "rank_model")) {
                String path = "file:///fixtures/models/rank_model/v1.0";
                when(f.files.pathExists(path)).thenReturn(true);

                var error = assertThrows(IllegalArgumentException.class, () -> f.train("v1.0"));

                assertTrue(error.getMessage().contains(path));
                assertNull(f.db.getCheckpoint("rank_model", "v1.0"));
                verify(f.files, never()).deletePath(any());
                f.k8s.verifyNoInteractions();
            }
        }));
    }

    @TestFactory
    Stream<DynamicTest> exportSqlRejectsUnregisteredPathsForEveryBackend() {
        return BACKENDS.stream().filter(backend -> !backend.equals("huggingface.transformers"))
                .map(backend -> DynamicTest.dynamicTest(backend, () -> {
                    try (Fixture f = new Fixture(backend, "rank_model")) {
                        String checkpoint = backend.equals("tzrec.dssm") ? "v1.0_export/item" : "v1.0_export";
                        String path = "file:///fixtures/models/rank_model/" + checkpoint;
                        when(f.files.pathExists(path)).thenReturn(true);

                        var error = assertThrows(IllegalArgumentException.class, () -> f.export("v1.0"));

                        assertTrue(error.getMessage().contains(path));
                        assertNull(f.db.getCheckpoint("rank_model", checkpoint));
                        verify(f.files, never()).deletePath(any());
                        f.k8s.verifyNoInteractions();
                    }
                }));
    }

    @Test
    void differentCheckpointsWithTheSameReadableNameUseDifferentResources() throws Exception {
        try (Fixture f = new Fixture("tzrec.dssm", "rank_model")) {
            assertTaskResources(f.train("v1.0"), "train-rank-model-v1-0-45da89d352a064bb", true);
            assertTaskResources(f.train("v1_0"), "train-rank-model-v1-0-6f268a38b3c13594", true);
        }
    }

    @Test
    void trainingAndExportUseSeparateResourceNames() throws Exception {
        try (Fixture f = new Fixture("tzrec.dssm", "rank_model")) {
            assertTaskResources(f.train("v1-export"), "train-rank-model-v1-export-fa041867f9759e61", true);
            assertTaskResources(f.export("v1"), "export-rank-model-v1-5f726a8ae040979f", true);
        }
    }

    @Test
    void truncatedNamesStillUseTheFullModelAndCheckpointIdentity() throws Exception {
        String model = "model".repeat(20);
        String checkpoint = "version".repeat(20);
        try (Fixture f = new Fixture("tzrec.dssm", model)) {
            assertTaskResources(f.train(checkpoint), "train-modelmodelmod-versionversio-1ed3f59b55614b52", true);
            assertTaskResources(f.train(checkpoint + "x"), "train-modelmodelmod-versionversio-262c58f688a9fbcd", true);
            assertTaskResources(f.export(checkpoint), "export-modelmodelmo-versionversio-b406fe683721252f", true);
        }
        try (Fixture f = new Fixture("tzrec.dssm", model + "x")) {
            assertTaskResources(f.train(checkpoint), "train-modelmodelmod-versionversio-05bcd1e6251af3bf", true);
        }
    }

    private static void assertTaskResources(String yaml, String expectedId, boolean tzrec) {
        Map<String, HasMetadata> resources = K8sYamlUtils.parseK8sYaml(yaml).stream()
                .collect(Collectors.toMap(HasMetadata::getKind, Function.identity()));
        assertEquals(tzrec ? Set.of("Job", "ConfigMap", "Service") : Set.of("Job", "ConfigMap"),
                resources.keySet());
        for (HasMetadata resource : resources.values()) {
            assertEquals("test", resource.getMetadata().getNamespace());
            String name = resource.getMetadata().getName();
            assertTrue(name.length() <= 63, name);
            assertTrue(name.matches("[a-z]([-a-z0-9]*[a-z0-9])?"), name);
        }
        ConfigMap configMap = (ConfigMap) resources.get("ConfigMap");
        Job job = (Job) resources.get("Job");
        assertEquals(expectedId + "-cm", configMap.getMetadata().getName());
        assertEquals(expectedId + "-job", job.getMetadata().getName());
        var pod = job.getSpec().getTemplate().getSpec();
        assertEquals(List.of(expectedId + "-cm"), pod.getVolumes().stream()
                .filter(volume -> volume.getConfigMap() != null)
                .map(volume -> volume.getConfigMap().getName()).toList());
        if (tzrec) {
            Service service = (Service) resources.get("Service");
            assertEquals(expectedId + "-job-headless", service.getMetadata().getName());
            assertEquals(Map.of("job-name", expectedId + "-job"), service.getSpec().getSelector());
            assertEquals(expectedId + "-job-headless", pod.getSubdomain());
            Map<String, String> env = pod.getContainers().getFirst().getEnv().stream()
                    .filter(value -> value.getValue() != null)
                    .collect(Collectors.toMap(value -> value.getName(), value -> value.getValue()));
            assertEquals(expectedId + "-job", env.get("JOB_NAME"));
            assertEquals(expectedId + "-job-headless", env.get("SERVICE_NAME"));
        }
    }

    private static class Fixture implements AutoCloseable {
        final InMemoryStoreAccess store = new InMemoryStoreAccess(List.of(), List.of(), List.of());
        final SchemaAccess schema = mock(SchemaAccess.class);
        final HdfsAccess files = mock(HdfsAccess.class);
        final MetadataAccess db = new MetadataAccess(schema, store, files);
        final MockedStatic<MetadataAccessFactory> metadata = mockStatic(MetadataAccessFactory.class);
        final MockedStatic<K8sManager> k8s = mockStatic(K8sManager.class);
        final MockedStatic<K8sYamlUtils> yamls = mockStatic(K8sYamlUtils.class, CALLS_REAL_METHODS);
        final Map<ConfigOption<String>, String> defaults = new HashMap<>();
        final String backend;
        final ModelConf model;

        Fixture(String backend, String modelName) throws Exception {
            this(backend, modelName, true);
        }

        Fixture(String backend, String modelName, boolean stableNames) throws Exception {
            this.backend = backend;
            // Snapshot the full YAML deterministically; real attempt uniqueness has its own test.
            if (stableNames) {
                yamls.when(() -> K8sYamlUtils.checkpointAttemptJobId(anyString(), anyString(), anyString()))
                        .thenAnswer(call -> K8sYamlUtils.checkpointJobId(
                                call.getArgument(0), call.getArgument(1), call.getArgument(2)));
            }
            for (ConfigOption<String> option : List.of(ModelConfigs.JAVA_HOME, ModelConfigs.HADOOP_HOME,
                    ModelConfigs.CLASSPATH, ModelConfigs.HADOOP_CONF_DIR, ModelConfigs.CLIENT_DIR,
                    ModelConfigs.CLIENT_PV_NAME, ModelConfigs.CLIENT_PVC_NAME)) {
                defaults.put(option, option.getDefaultValue());
                option.setDefaultValue("");
            }
            metadata.when(MetadataAccessFactory::getInstance).thenReturn(db);
            when(schema.getPartitionPaths(anyString(), anyString(), anyString()))
                    .thenReturn(List.of("file:///fixtures/training"));
            String fields = "feature FLOAT, label INT";
            String options = "'label_columns'='label'";
            if (backend.startsWith("tzrec.")) {
                fields = "uid STRING, iid STRING, label INT";
                if (backend.equals("tzrec.dssm")) {
                    options += ", 'user_features'='uid', 'item_features'='iid'";
                } else if (backend.equals("tzrec.mmoe")) {
                    fields += ", label2 INT";
                    options = "'label_columns'='label,label2'";
                }
            } else if (backend.equals("huggingface.transformers")) {
                fields = "text STRING";
                options = "'repo_id'='org/test-model', 'task'='text-classification', 'text_column'='text'";
            }
            model = ModelManager.createModel((SqlCreateModel) CompileManager.parseSql(
                    "CREATE MODEL `" + modelName + "` (" + fields + ") WITH ('model'='" + backend + "', "
                            + options + ", 'MODEL_PATH'='file:///fixtures/models/" + modelName + "')"));
        }

        String train(String checkpoint) throws Exception {
            String input = backend.equals("huggingface.transformers") ? "" : " ON training";
            var result = ModelManager.trainModel((SqlTrainModel) CompileManager.parseSql(
                    "TRAIN MODEL `" + model.getModelName() + "` CHECKPOINT='" + checkpoint + "'" + input
                            + " WITH (" + TASK_OPTIONS + ")"), "default");
            assertEquals(1, result.size());
            String yaml = db.getCheckpoint(model.getModelName(), checkpoint).getYaml();
            k8s.verify(() -> K8sManager.applyYaml(yaml));
            return yaml;
        }

        String export(String checkpoint) throws Exception {
            Checkpoint source = new Checkpoint();
            source.setModelName(model.getModelName());
            source.setCheckpointName(checkpoint);
            source.setModelDdl(model.getDdl());
            source.setStatus(Consts.CHECKPOINT_STATUS_SUCCEEDED);
            source.setCheckpointType(Consts.CHECKPOINT_TYPE_ORIGIN);
            db.upsertCheckpoint(source);
            var result = ModelManager.exportModel((SqlExportModel) CompileManager.parseSql(
                    "EXPORT MODEL `" + model.getModelName() + "` CHECKPOINT='" + checkpoint
                            + "' WITH (" + TASK_OPTIONS + ")"), "default");
            assertEquals(backend.equals("tzrec.dssm") ? 2 : 1, result.size());
            String yaml = db.getCheckpoint(model.getModelName(), result.getFirst().getCheckpointName()).getYaml();
            for (var output : result) {
                assertEquals(yaml, db.getCheckpoint(model.getModelName(), output.getCheckpointName()).getYaml());
            }
            k8s.verify(() -> K8sManager.applyYaml(yaml));
            return yaml;
        }

        @Override
        public void close() {
            defaults.forEach(ConfigOption::setDefaultValue);
            yamls.close();
            k8s.close();
            metadata.close();
        }
    }
}
