package com.sqlrec.model.tzrec;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class K8sYamlUtilsTest {

    @Test
    public void testCreateConfigMapYamlWithSingleFile() {
        String configMapName = "test-configmap";
        Map<String, String> files = new HashMap<>();
        files.put("application.properties", "key1=value1\nkey2=value2");
        String yaml = K8sYamlUtils.createConfigMapYaml(configMapName, files);
        
        String expectedYaml = """
---
apiVersion: "v1"
kind: "ConfigMap"
metadata:
  name: "test-configmap"
data:
  application.properties: |-
    key1=value1
    key2=value2
""";
        assertEquals(expectedYaml, yaml);
    }

    @Test
    public void testCreateHeadlessServiceYaml() {
        String jobName = "test-job";
        String serviceName = "test-service";
        int masterPort = 29500;
        
        String yaml = K8sYamlUtils.createHeadlessServiceYaml(jobName, serviceName, masterPort);
        
        String expectedYaml = """
---
apiVersion: "v1"
kind: "Service"
metadata:
  name: "test-service"
spec:
  clusterIP: "None"
  ports:
  - name: "torch-distributed"
    port: 29500
    targetPort: 29500
  selector:
    job-name: "test-job"
""";
        assertEquals(expectedYaml, yaml);
    }

    @Test
    public void testCreateServiceYaml() {
        String serviceName = "test-service";
        int port = 80;
        String selectKey = "app";
        String selectValue = "test-app";
        
        String yaml = K8sYamlUtils.createServiceYaml(serviceName, port, selectKey, selectValue);
        
        String expectedYaml = """
---
apiVersion: "v1"
kind: "Service"
metadata:
  name: "test-service"
spec:
  ports:
  - name: "server"
    port: 80
    targetPort: 80
  selector:
    app: "test-app"
""";
        assertEquals(expectedYaml, yaml);
    }

    @Test
    public void testCreateJobYaml() {
        String jobName = "test-job";
        String configMapName = "test-configmap";
        String serviceName = "test-service";
        int nnodes = 1;
        int nprocPerNode = 1;
        int masterPort = 29500;
        Map<String, String> params = new HashMap<>();
        params.put("image", "sqlrec/tzrec");
        params.put("version", "0.1.0-cpu");
        params.put("pod_cpu_cores", "2");
        params.put("pod_memory", "8Gi");

        String yaml = K8sYamlUtils.createJobYaml(
                jobName, configMapName, serviceName, nnodes, nprocPerNode, masterPort, params
        );
        
        String expectedYaml = """
---
apiVersion: "batch/v1"
kind: "Job"
metadata:
  name: "test-job"
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
          value: "test-job"
        - name: "SERVICE_NAME"
          value: "test-service"
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
      subdomain: "test-service"
      volumes:
      - configMap:
          name: "test-configmap"
        name: "config-volume"
""";
        assertEquals(expectedYaml, yaml);
    }

    @Test
    public void testCreateJobYamlWithCustomParams() {
        String jobName = "custom-job";
        String configMapName = "custom-configmap";
        String serviceName = "custom-service";
        int nnodes = 4;
        int nprocPerNode = 2;
        int masterPort = 30000;
        Map<String, String> params = new HashMap<>();
        params.put("image", "custom/image");
        params.put("version", "v2.0");
        params.put("pod_cpu_cores", "8");
        params.put("pod_memory", "16Gi");

        String yaml = K8sYamlUtils.createJobYaml(
                jobName, configMapName, serviceName, nnodes, nprocPerNode, masterPort, params
        );
        
        String expectedYaml = """
---
apiVersion: "batch/v1"
kind: "Job"
metadata:
  name: "custom-job"
spec:
  backoffLimit: 1
  completionMode: "Indexed"
  completions: 4
  parallelism: 4
  template:
    spec:
      containers:
      - command:
        - "bash"
        - "/data/start.sh"
        env:
        - name: "JOB_NAME"
          value: "custom-job"
        - name: "SERVICE_NAME"
          value: "custom-service"
        - name: "MASTER_PORT"
          value: "30000"
        - name: "NNODES"
          value: "4"
        - name: "NPROC_PER_NODE"
          value: "2"
        - name: "USE_FSSPEC"
          value: "1"
        - name: "USE_SPAWN_MULTI_PROCESS"
          value: "1"
        - name: "USE_FARM_HASH_TO_BUCKETIZE"
          value: "true"
        image: "custom/image:v2.0"
        name: "tzrec-job"
        resources:
          limits:
            cpu: "8"
            memory: "16Gi"
          requests:
            cpu: "8"
            memory: "16Gi"
        volumeMounts:
        - mountPath: "/data"
          name: "config-volume"
      restartPolicy: "Never"
      subdomain: "custom-service"
      volumes:
      - configMap:
          name: "custom-configmap"
        name: "config-volume"
""";
        assertEquals(expectedYaml, yaml);
    }

    @Test
    public void testMergeK8sYamls() {
        String yaml1 = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: cm1\n";
        String yaml2 = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: cm2\n";

        String mergedYaml = K8sYamlUtils.mergeK8sYamls(yaml1, yaml2);
        
        String expectedYaml = """
apiVersion: v1
kind: ConfigMap
metadata:
  name: cm1
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: cm2
""";
        assertEquals(expectedYaml, mergedYaml);
    }

    @Test
    public void testMergeMultipleK8sYamls() {
        String yaml1 = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: cm1\n";
        String yaml2 = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: cm2\n";
        String yaml3 = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: cm3\n";

        String mergedYaml = K8sYamlUtils.mergeK8sYamls(yaml1, yaml2, yaml3);
        
        String expectedYaml = """
apiVersion: v1
kind: ConfigMap
metadata:
  name: cm1
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: cm2
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: cm3
""";
        assertEquals(expectedYaml, mergedYaml);
    }

    @Test
    public void testCreateDeploymentYaml() {
        String deployName = "test-deployment";
        String modelCheckpointDir = "/model/checkpoint/v1";
        Map<String, String> params = new HashMap<>();
        params.put("image", "sqlrec/tzrec");
        params.put("version", "0.1.0-cpu");
        params.put("pod_cpu_cores", "4");
        params.put("pod_memory", "16Gi");
        params.put("replicas", "3");

        String yaml = K8sYamlUtils.createDeploymentYaml(
                deployName, modelCheckpointDir, params
        );
        
        String expectedYaml = """
---
apiVersion: "apps/v1"
kind: "Deployment"
metadata:
  name: "test-deployment"
spec:
  replicas: 3
  selector:
    matchLabels:
      app: "test-deployment"
  template:
    metadata:
      labels:
        app: "test-deployment"
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
""";
        assertEquals(expectedYaml, yaml);
    }

    @Test
    public void testCreateDeploymentYamlWithDefaultParams() {
        String deployName = "default-deployment";
        String modelCheckpointDir = "/model/checkpoint/default";

        String yaml = K8sYamlUtils.createDeploymentYaml(
                deployName, modelCheckpointDir, new HashMap<>()
        );
        
        String expectedYaml = """
---
apiVersion: "apps/v1"
kind: "Deployment"
metadata:
  name: "default-deployment"
spec:
  replicas: 1
  selector:
    matchLabels:
      app: "default-deployment"
  template:
    metadata:
      labels:
        app: "default-deployment"
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
""";
        assertEquals(expectedYaml, yaml);
    }
}
