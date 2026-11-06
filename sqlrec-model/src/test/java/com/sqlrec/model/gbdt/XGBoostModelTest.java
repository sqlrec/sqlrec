package com.sqlrec.model.gbdt;

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

class XGBoostModelTest {
    private final XGBoostModel controller = new XGBoostModel();

    @Test
    void trainYamlMatchesCompleteExpectedYaml() {
        String actual = fullTrainingYaml();
        String expectedYaml = """
---
apiVersion: "v1"
kind: "ConfigMap"
metadata:
  name: "xgboost-train-cm"
data:
  pipeline.config: |
    {
      "model_type": "xgboost",
      "train_input_path": "/fixture/train.parquet,/fixture/train-extra.parquet",
      "model_dir": "/fixture/xgboost/v1",
      "base_model_dir": "/fixture/xgboost/base",
      "label_columns": "label",
      "feature_columns": [
        "price",
        "rating"
      ],
      "categorical_features": [],
      "params": {
        "objective": "binary",
        "metric": "auc",
        "random_seed": 7,
        "num_iterations": 20,
        "max_depth": 4,
        "learning_rate": 0.05,
        "feature_fraction": 0.8,
        "bagging_fraction": 0.8,
        "min_child_weight": 1,
        "l2_regularization": 1.0
      }
    }
  start.sh: |-
    #!/bin/bash
    set -ex
    export PYTHONPATH=/app:$PYTHONPATH

    exec python -m gbdt.train_xgboost \\
        --pipeline_config_path /data/pipeline.config
---
apiVersion: "batch/v1"
kind: "Job"
metadata:
  name: "xgboost-train-job"
spec:
  backoffLimit: 1
  template:
    spec:
      containers:
      - command:
        - "bash"
        - "/data/start.sh"
        image: "sqlrec/gbdt:%s-cpu"
        name: "gbdt-job"
        resources:
          requests:
            cpu: "2"
            memory: "4Gi"
        volumeMounts:
        - mountPath: "/data"
          name: "config-volume"
      restartPolicy: "Never"
      volumes:
      - configMap:
          name: "xgboost-train-cm"
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
  name: "xgboost-export-cm"
data:
  pipeline.config: |
    {
      "base_model_dir": "/fixture/xgboost/v1",
      "export_dir": "/fixture/xgboost/v1_export"
    }
  start.sh: |-
    #!/bin/bash
    set -ex
    export PYTHONPATH=/app:$PYTHONPATH

    exec python -m gbdt.export_xgboost \\
        --pipeline_config_path /data/pipeline.config
---
apiVersion: "batch/v1"
kind: "Job"
metadata:
  name: "xgboost-export-job"
spec:
  backoffLimit: 1
  template:
    spec:
      containers:
      - command:
        - "bash"
        - "/data/start.sh"
        image: "sqlrec/gbdt:%s-cpu"
        name: "gbdt-job"
        resources:
          requests:
            cpu: "2"
            memory: "4Gi"
        volumeMounts:
        - mountPath: "/data"
          name: "config-volume"
      restartPolicy: "Never"
      volumes:
      - configMap:
          name: "xgboost-export-cm"
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
  name: "xgboost-service"
spec:
  replicas: 1
  selector:
    matchLabels:
      app: "xgboost-service"
  template:
    metadata:
      labels:
        app: "xgboost-service"
    spec:
      containers:
      - command:
        - "bash"
        - "-c"
        - |
          #!/bin/bash
          set -ex

          LOCAL_CACHE_DIR=${LOCAL_CACHE_DIR:-/tmp/gbdt_model_cache}
          rm -rf "$LOCAL_CACHE_DIR"
          ${HADOOP_HOME}/bin/hadoop fs -get '/fixture/xgboost/v1_export' "$LOCAL_CACHE_DIR"

          exec /app/onnx_server "$LOCAL_CACHE_DIR" 80
        image: "sqlrec/gbdt:%s-cpu"
        name: "gbdt-service"
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
  name: "xgboost-service"
spec:
  ports:
  - name: "server"
    port: 80
    targetPort: 80
  selector:
    app: "xgboost-service"
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
  name: "xgboost-service"
spec:
  replicas: 3
  selector:
    matchLabels:
      app: "xgboost-service"
  template:
    metadata:
      labels:
        app: "xgboost-service"
    spec:
      containers:
      - command:
        - "bash"
        - "-c"
        - |
          #!/bin/bash
          set -ex

          LOCAL_CACHE_DIR=${LOCAL_CACHE_DIR:-/tmp/gbdt_model_cache}
          rm -rf "$LOCAL_CACHE_DIR"
          ${HADOOP_HOME}/bin/hadoop fs -get '/fixture/xgboost/v1_export' "$LOCAL_CACHE_DIR"

          exec /app/onnx_server "$LOCAL_CACHE_DIR" 80
        image: "custom/gbdt:v2"
        name: "gbdt-service"
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
  name: "xgboost-service"
spec:
  ports:
  - name: "server"
    port: 80
    targetPort: 80
  selector:
    app: "xgboost-service"
""";
        assertEquals(expectedYaml, actual);
    }

    private ModelConf model() {
        ModelConf model = new ModelConf();
        model.setModelName("xgboost_model");
        model.setInputFields(List.of(new FieldSchema("price", "FLOAT"), new FieldSchema("rating", "DOUBLE"),
                new FieldSchema("label", "INT")));
        model.setParams(Map.of("model", "gbdt.xgboost", "label_columns", "label"));
        return model;
    }

    private String fullTrainingYaml() {
        ModelTrainConf train = new ModelTrainConf();
        train.setId("xgboost-train");
        train.setModelDir("/fixture/xgboost/v1");
        train.setBaseModelDir("/fixture/xgboost/base");
        train.setTrainDataPaths(List.of("/fixture/train.parquet", "/fixture/train-extra.parquet"));
        train.setParams(Map.of("num_iterations", "20", "max_depth", "4", "learning_rate", "0.05",
                "random_seed", "7", "pod_cpu_cores", "2", "pod_memory", "4Gi"));
        return controller.genModelTrainK8sYaml(model(), train);
    }

    private String fullExportYaml() {
        ModelExportConf export = new ModelExportConf();
        export.setId("xgboost-export");
        export.setCheckpointName("v1");
        export.setBaseModelDir("/fixture/xgboost/v1");
        export.setParams(Map.of("pod_cpu_cores", "2", "pod_memory", "4Gi"));
        return controller.genModelExportK8sYaml(model(), export);
    }

    private String fullServiceYaml(boolean custom) {
        ServiceConf service = new ServiceConf();
        service.setId("xgboost-service");
        service.setModelCheckpointDir("/fixture/xgboost/v1_export");
        service.setParams(custom ? Map.of("image", "custom/gbdt", "version", "v2", "replicas", "3",
                "pod_cpu_cores", "4", "pod_memory", "16Gi", "pod_cpu_limit", "8", "pod_memory_limit", "32Gi") : Map.of());
        return controller.getServiceK8sYaml(model(), service);
    }
}
