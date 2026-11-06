package com.sqlrec.model.huggingface;

import com.sqlrec.common.model.ModelConf;
import com.sqlrec.common.model.ModelTrainConf;
import com.sqlrec.common.model.ServiceConf;
import com.sqlrec.common.schema.FieldSchema;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.sqlrec.common.config.SqlRecConfigs.SQLREC_VERSION;
import static org.junit.jupiter.api.Assertions.assertEquals;

class HuggingFaceK8sYamlTest {
    private final HuggingFaceTransformersModel controller = new HuggingFaceTransformersModel();

    @Test
    void trainingJobMatchesCompleteYaml() {
        String expectedYaml = """
---
apiVersion: "v1"
kind: "ConfigMap"
metadata:
  name: "embedding-train-cm"
data:
  pipeline.config: |
    {
      "framework": "huggingface.transformers",
      "model_name": "embedding_model",
      "checkpoint_name": "v1",
      "repo_id": "org/test-model",
      "revision": "abc123",
      "task": "embedding",
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
        "model": "huggingface.transformers",
        "task": "embedding",
        "repo_id": "org/test-model",
        "text_column": "text",
        "revision": "abc123",
        "hf_token_secret": "huggingface-token",
        "hf_token_secret_key": "access-token",
        "pod_cpu_cores": "2",
        "pod_memory": "4Gi"
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
    MODEL_BASE_DIR='hdfs://namenode/models/embedding_model'
    MODEL_DIR='hdfs://namenode/models/embedding_model/v1'
    STAGING_DIR='hdfs://namenode/models/embedding_model/v1.__uploading__.embedding-train'
    "$HADOOP" fs -mkdir -p "$MODEL_BASE_DIR"
    "$HADOOP" fs -rm -r -f "$STAGING_DIR"
    "$HADOOP" fs -put -f "$LOCAL_MODEL_DIR" "$STAGING_DIR"
    "$HADOOP" fs -touchz "$STAGING_DIR/_SUCCESS"
    "$HADOOP" fs -mv "$STAGING_DIR" "$MODEL_DIR"
---
apiVersion: "batch/v1"
kind: "Job"
metadata:
  name: "embedding-train-job"
spec:
  backoffLimit: 1
  template:
    spec:
      containers:
      - command:
        - "bash"
        - "/data/start.sh"
        env:
        - name: "HF_TOKEN"
          valueFrom:
            secretKeyRef:
              key: "access-token"
              name: "huggingface-token"
              optional: false
        image: "sqlrec/transformers:%s"
        name: "transformers-download"
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
          name: "embedding-train-cm"
        name: "config-volume"
""".formatted(SQLREC_VERSION.getValue());
        assertEquals(expectedYaml, trainingYaml());
    }

    @Test
    void defaultServiceMatchesCompleteYaml() {
        String expectedYaml = """
---
apiVersion: "v1"
kind: "ConfigMap"
metadata:
  name: "embedding-service-cm"
data:
  service.config: |
    {
      "model": "huggingface.transformers",
      "task": "embedding",
      "repo_id": "org/test-model",
      "text_column": "text",
      "served_model_name": "embedding_model"
    }
  start.sh: |
    #!/bin/bash
    set -euo pipefail
    export PYTHONPATH=/app:${PYTHONPATH:-}
    LOCAL_MODEL_DIR=${LOCAL_MODEL_DIR:-/tmp/huggingface_model}
    rm -rf "$LOCAL_MODEL_DIR"
    mkdir -p "$LOCAL_MODEL_DIR"
    ${HADOOP_HOME}/bin/hadoop fs -get 'hdfs://namenode/models/embedding_model/v1/*' "$LOCAL_MODEL_DIR"/
    test -f "$LOCAL_MODEL_DIR/_SUCCESS"
    exec python -m huggingface.server --model_dir "$LOCAL_MODEL_DIR" --config /data/service.config --port 80
---
apiVersion: "apps/v1"
kind: "Deployment"
metadata:
  name: "embedding-service"
spec:
  replicas: 1
  selector:
    matchLabels:
      app: "embedding-service"
  template:
    metadata:
      annotations:
        sqlrec.io/config-checksum: "ebca95b621ea7658d30d5a3e82a4476984fe00c29efcbff3f41dfb2966f50e03"
      labels:
        app: "embedding-service"
    spec:
      containers:
      - command:
        - "bash"
        - "/data/start.sh"
        image: "sqlrec/transformers:%s"
        name: "transformers-service"
        ports:
        - containerPort: 80
          name: "http"
        readinessProbe:
          failureThreshold: 60
          httpGet:
            path: "/ready"
            port: 80
            scheme: "HTTP"
          initialDelaySeconds: 2
          periodSeconds: 5
          timeoutSeconds: 2
        resources:
          requests:
            cpu: "1"
            memory: "2Gi"
        volumeMounts:
        - mountPath: "/data"
          name: "config-volume"
      volumes:
      - configMap:
          name: "embedding-service-cm"
        name: "config-volume"
---
apiVersion: "v1"
kind: "Service"
metadata:
  name: "embedding-service"
spec:
  ports:
  - name: "server"
    port: 80
    targetPort: 80
  selector:
    app: "embedding-service"
""".formatted(SQLREC_VERSION.getValue());
        assertEquals(expectedYaml, serviceYaml(Map.of()));
    }

    @Test
    void gpuServiceWithSecretAndResourceOverridesMatchesCompleteYaml() {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("image", "custom/transformers");
        params.put("version", "v2");
        params.put("replicas", "3");
        params.put("pod_cpu_cores", "4");
        params.put("pod_memory", "8Gi");
        params.put("pod_cpu_limit", "8");
        params.put("pod_memory_limit", "16Gi");
        params.put("pod_gpu", "1");
        params.put("openai_api_key_secret", "serving-key");
        params.put("openai_api_key_secret_key", "api-token");
        params.put("served_model_name", "public-embedding");
        params.put("inference_batch_size", "16");
        String expectedYaml = """
---
apiVersion: "v1"
kind: "ConfigMap"
metadata:
  name: "embedding-service-cm"
data:
  service.config: |
    {
      "model": "huggingface.transformers",
      "task": "embedding",
      "repo_id": "org/test-model",
      "text_column": "text",
      "image": "custom/transformers",
      "version": "v2",
      "replicas": "3",
      "pod_cpu_cores": "4",
      "pod_memory": "8Gi",
      "pod_cpu_limit": "8",
      "pod_memory_limit": "16Gi",
      "pod_gpu": "1",
      "openai_api_key_secret": "serving-key",
      "openai_api_key_secret_key": "api-token",
      "served_model_name": "public-embedding",
      "inference_batch_size": "16"
    }
  start.sh: |
    #!/bin/bash
    set -euo pipefail
    export PYTHONPATH=/app:${PYTHONPATH:-}
    LOCAL_MODEL_DIR=${LOCAL_MODEL_DIR:-/tmp/huggingface_model}
    rm -rf "$LOCAL_MODEL_DIR"
    mkdir -p "$LOCAL_MODEL_DIR"
    ${HADOOP_HOME}/bin/hadoop fs -get 'hdfs://namenode/models/embedding_model/v1/*' "$LOCAL_MODEL_DIR"/
    test -f "$LOCAL_MODEL_DIR/_SUCCESS"
    exec python -m huggingface.server --model_dir "$LOCAL_MODEL_DIR" --config /data/service.config --port 80
---
apiVersion: "apps/v1"
kind: "Deployment"
metadata:
  name: "embedding-service"
spec:
  replicas: 3
  selector:
    matchLabels:
      app: "embedding-service"
  template:
    metadata:
      annotations:
        sqlrec.io/config-checksum: "e2a4aa0f02655abd7b4655bf57e3b5b2e77b8508b5392ba906a2f507d0f9193b"
      labels:
        app: "embedding-service"
    spec:
      containers:
      - command:
        - "bash"
        - "/data/start.sh"
        env:
        - name: "SQLREC_OPENAI_API_KEY"
          valueFrom:
            secretKeyRef:
              key: "api-token"
              name: "serving-key"
              optional: false
        image: "custom/transformers:v2"
        name: "transformers-service"
        ports:
        - containerPort: 80
          name: "http"
        readinessProbe:
          failureThreshold: 60
          httpGet:
            path: "/ready"
            port: 80
            scheme: "HTTP"
          initialDelaySeconds: 2
          periodSeconds: 5
          timeoutSeconds: 2
        resources:
          limits:
            cpu: "8"
            memory: "16Gi"
            nvidia.com/gpu: "1"
          requests:
            cpu: "4"
            memory: "8Gi"
            nvidia.com/gpu: "1"
        volumeMounts:
        - mountPath: "/data"
          name: "config-volume"
      volumes:
      - configMap:
          name: "embedding-service-cm"
        name: "config-volume"
---
apiVersion: "v1"
kind: "Service"
metadata:
  name: "embedding-service"
spec:
  ports:
  - name: "server"
    port: 80
    targetPort: 80
  selector:
    app: "embedding-service"
""";
        assertEquals(expectedYaml, serviceYaml(params));
    }

    private String trainingYaml() {
        ModelConf model = model();
        ModelTrainConf train = new ModelTrainConf();
        train.setId("embedding-train");
        train.setCheckpointName("v1");
        train.setModelDir(model.getPath() + "/v1");
        Map<String, String> params = new LinkedHashMap<>();
        params.put("revision", "abc123");
        params.put("hf_token_secret", "huggingface-token");
        params.put("hf_token_secret_key", "access-token");
        params.put("pod_cpu_cores", "2");
        params.put("pod_memory", "4Gi");
        train.setParams(params);
        return controller.genModelTrainK8sYaml(model, train);
    }

    private String serviceYaml(Map<String, String> params) {
        ServiceConf service = new ServiceConf();
        service.setId("embedding-service");
        service.setModelCheckpointDir("hdfs://namenode/models/embedding_model/v1");
        service.setParams(params);
        return controller.getServiceK8sYaml(model(), service);
    }

    private ModelConf model() {
        ModelConf model = new ModelConf();
        model.setModelName("embedding_model");
        model.setPath("hdfs://namenode/models/embedding_model");
        model.setInputFields(List.of(new FieldSchema("text", "STRING")));
        // Preserve parameter order so the embedded JSON has a stable textual representation.
        Map<String, String> params = new LinkedHashMap<>();
        params.put("model", "huggingface.transformers");
        params.put("task", "embedding");
        params.put("repo_id", "org/test-model");
        params.put("text_column", "text");
        model.setParams(params);
        return model;
    }

}
