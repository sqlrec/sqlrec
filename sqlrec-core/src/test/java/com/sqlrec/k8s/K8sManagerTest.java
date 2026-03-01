package com.sqlrec.k8s;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class K8sManagerTest {

    @Test
    public void testInjectEnvVarsIntoYaml() {
        String yamlContent = """
apiVersion: batch/v1
kind: Job
metadata:
  name: test-job
spec:
  template:
    spec:
      containers:
      - name: test-container
        image: busybox
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: test-deployment
spec:
  replicas: 1
  selector:
    matchLabels:
      app: test
  template:
    metadata:
      labels:
        app: test
    spec:
      containers:
      - name: test-container
        image: nginx
""";

        Map<String, String> envVars = new HashMap<>();
        envVars.put("DB_HOST", "localhost");
        envVars.put("DB_PORT", "5432");

        String result = K8sManager.injectEnvVarsIntoYaml(yamlContent, envVars);

        String expectedYaml = """
---
apiVersion: "batch/v1"
kind: "Job"
metadata:
  name: "test-job"
spec:
  template:
    spec:
      containers:
      - env:
        - name: "DB_PORT"
          value: "5432"
        - name: "DB_HOST"
          value: "localhost"
        image: "busybox"
        name: "test-container"
---
apiVersion: "apps/v1"
kind: "Deployment"
metadata:
  name: "test-deployment"
spec:
  replicas: 1
  selector:
    matchLabels:
      app: "test"
  template:
    metadata:
      labels:
        app: "test"
    spec:
      containers:
      - env:
        - name: "DB_PORT"
          value: "5432"
        - name: "DB_HOST"
          value: "localhost"
        image: "nginx"
        name: "test-container"
""";
        assertEquals(expectedYaml, result);
    }

    @Test
    public void testInjectEnvVarsIntoYamlWithEmptyInput() {
        assertNull(K8sManager.injectEnvVarsIntoYaml(null, new HashMap<>()));
        assertEquals("", K8sManager.injectEnvVarsIntoYaml("", new HashMap<>()));
        assertEquals("test", K8sManager.injectEnvVarsIntoYaml("test", null));
        assertEquals("test", K8sManager.injectEnvVarsIntoYaml("test", new HashMap<>()));
    }

    @Test
    public void testParseK8sYamlAndGetJobs() {
        String yamlContent = """
apiVersion: batch/v1
kind: Job
metadata:
  name: test-job
spec:
  template:
    spec:
      containers:
      - name: test-container
        image: busybox
""";

        var jobs = K8sManager.parseK8sYamlAndGetJobs(yamlContent);
        assertEquals(1, jobs.size());
        assertEquals("test-job", jobs.get(0).getMetadata().getName());
    }

    @Test
    public void testParseK8sYamlAndGetDeployments() {
        String yamlContent = """
apiVersion: apps/v1
kind: Deployment
metadata:
  name: test-deployment
spec:
  replicas: 1
  selector:
    matchLabels:
      app: test
  template:
    metadata:
      labels:
        app: test
    spec:
      containers:
      - name: test-container
        image: nginx
""";

        var deployments = K8sManager.parseK8sYamlAndGetDeployments(yamlContent);
        assertEquals(1, deployments.size());
        assertEquals("test-deployment", deployments.get(0).getMetadata().getName());
    }

    @Test
    public void testConvertToValidK8sName() {
        assertEquals("test-name", K8sManager.convertToValidK8sName("Test_Name"));
        assertEquals("test-name", K8sManager.convertToValidK8sName("test@name"));
        assertEquals("test-name", K8sManager.convertToValidK8sName("  test-name  "));
        assertEquals("test-name", K8sManager.convertToValidK8sName("test--name"));
        assertEquals("test-name", K8sManager.convertToValidK8sName("-test-name-"));
        assertNull(K8sManager.convertToValidK8sName(null));
        assertEquals("", K8sManager.convertToValidK8sName(""));
    }

    @Test
    public void testInjectVolumeMountIntoYaml() {
        String yamlContent = """
apiVersion: batch/v1
kind: Job
metadata:
  name: test-job
spec:
  template:
    spec:
      containers:
      - name: test-container
        image: busybox
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: test-deployment
spec:
  replicas: 1
  selector:
    matchLabels:
      app: test
  template:
    metadata:
      labels:
        app: test
    spec:
      containers:
      - name: test-container
        image: nginx
""";

        String result = K8sManager.injectVolumeMountIntoYaml(
            yamlContent,
            "my-pvc",
            "my-volume",
            "/app/data",
            "subdir"
        );

        String expectedYaml = """
---
apiVersion: "batch/v1"
kind: "Job"
metadata:
  name: "test-job"
spec:
  template:
    spec:
      containers:
      - image: "busybox"
        name: "test-container"
        volumeMounts:
        - mountPath: "/app/data"
          name: "my-volume"
          subPath: "subdir"
      volumes:
      - name: "my-volume"
        persistentVolumeClaim:
          claimName: "my-pvc"
---
apiVersion: "apps/v1"
kind: "Deployment"
metadata:
  name: "test-deployment"
spec:
  replicas: 1
  selector:
    matchLabels:
      app: "test"
  template:
    metadata:
      labels:
        app: "test"
    spec:
      containers:
      - image: "nginx"
        name: "test-container"
        volumeMounts:
        - mountPath: "/app/data"
          name: "my-volume"
          subPath: "subdir"
      volumes:
      - name: "my-volume"
        persistentVolumeClaim:
          claimName: "my-pvc"
""";
        assertEquals(expectedYaml, result);
    }

    @Test
    public void testInjectVolumeMountIntoYamlWithEmptyInput() {
        assertNull(K8sManager.injectVolumeMountIntoYaml(null, "pvc", "volume", "/path", "subpath"));
        assertEquals("", K8sManager.injectVolumeMountIntoYaml("", "pvc", "volume", "/path", "subpath"));
        assertEquals("test", K8sManager.injectVolumeMountIntoYaml("test", null, "volume", "/path", "subpath"));
        assertEquals("test", K8sManager.injectVolumeMountIntoYaml("test", "pvc", null, "/path", "subpath"));
        assertEquals("test", K8sManager.injectVolumeMountIntoYaml("test", "pvc", "volume", null, "subpath"));
    }

    @Test
    public void testInjectVolumeMountIntoYamlWithoutSubpath() {
        String yamlContent = """
apiVersion: batch/v1
kind: Job
metadata:
  name: test-job
spec:
  template:
    spec:
      containers:
      - name: test-container
        image: busybox
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: test-deployment
spec:
  replicas: 1
  selector:
    matchLabels:
      app: test
  template:
    metadata:
      labels:
        app: test
    spec:
      containers:
      - name: test-container
        image: nginx
""";

        String result = K8sManager.injectVolumeMountIntoYaml(
            yamlContent,
            "my-pvc",
            "my-volume",
            "/app/data",
            null
        );

        String expectedYaml = """
---
apiVersion: "batch/v1"
kind: "Job"
metadata:
  name: "test-job"
spec:
  template:
    spec:
      containers:
      - image: "busybox"
        name: "test-container"
        volumeMounts:
        - mountPath: "/app/data"
          name: "my-volume"
      volumes:
      - name: "my-volume"
        persistentVolumeClaim:
          claimName: "my-pvc"
---
apiVersion: "apps/v1"
kind: "Deployment"
metadata:
  name: "test-deployment"
spec:
  replicas: 1
  selector:
    matchLabels:
      app: "test"
  template:
    metadata:
      labels:
        app: "test"
    spec:
      containers:
      - image: "nginx"
        name: "test-container"
        volumeMounts:
        - mountPath: "/app/data"
          name: "my-volume"
      volumes:
      - name: "my-volume"
        persistentVolumeClaim:
          claimName: "my-pvc"
""";
        assertEquals(expectedYaml, result);
    }

    @Test
    public void testInjectNamespaceIntoYaml() {
        String yamlContent = """
apiVersion: batch/v1
kind: Job
metadata:
  name: test-job
spec:
  template:
    spec:
      containers:
      - name: test-container
        image: busybox
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: test-deployment
spec:
  replicas: 1
  selector:
    matchLabels:
      app: test
  template:
    metadata:
      labels:
        app: test
    spec:
      containers:
      - name: test-container
        image: nginx
""";

        String result = K8sManager.injectNamespaceIntoYaml(yamlContent, "my-namespace");

        String expectedYaml = """
---
apiVersion: "batch/v1"
kind: "Job"
metadata:
  name: "test-job"
  namespace: "my-namespace"
spec:
  template:
    spec:
      containers:
      - image: "busybox"
        name: "test-container"
---
apiVersion: "apps/v1"
kind: "Deployment"
metadata:
  name: "test-deployment"
  namespace: "my-namespace"
spec:
  replicas: 1
  selector:
    matchLabels:
      app: "test"
  template:
    metadata:
      labels:
        app: "test"
    spec:
      containers:
      - image: "nginx"
        name: "test-container"
""";
        assertEquals(expectedYaml, result);
    }

    @Test
    public void testInjectNamespaceIntoYamlWithExistingNamespace() {
        String yamlContent = """
apiVersion: batch/v1
kind: Job
metadata:
  name: test-job
  namespace: existing-namespace
spec:
  template:
    spec:
      containers:
      - name: test-container
        image: busybox
""";

        String result = K8sManager.injectNamespaceIntoYaml(yamlContent, "new-namespace");

        String expectedYaml = """
---
apiVersion: "batch/v1"
kind: "Job"
metadata:
  name: "test-job"
  namespace: "existing-namespace"
spec:
  template:
    spec:
      containers:
      - image: "busybox"
        name: "test-container"
""";
        assertEquals(expectedYaml, result);
    }

    @Test
    public void testInjectNamespaceIntoYamlWithEmptyInput() {
        assertNull(K8sManager.injectNamespaceIntoYaml(null, "namespace"));
        assertEquals("", K8sManager.injectNamespaceIntoYaml("", "namespace"));
        assertEquals("test", K8sManager.injectNamespaceIntoYaml("test", null));
        assertEquals("test", K8sManager.injectNamespaceIntoYaml("test", ""));
    }
}
