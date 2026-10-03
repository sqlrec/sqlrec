package com.sqlrec.k8s;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.PodSpec;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class K8sYamlUtilsTest {

    @Test
    public void testInjectionReturnsOriginalYamlOnParseFailure() {
        String invalidYaml = "apiVersion: [unterminated";
        assertEquals(invalidYaml, K8sYamlUtils.injectEnvVarsIntoYaml(invalidYaml, Map.of("NAME", "value")));
        assertEquals(invalidYaml,
                K8sYamlUtils.injectVolumeMountIntoYaml(invalidYaml, "pvc", "data", "/data", null));
        assertEquals(invalidYaml, K8sYamlUtils.injectNamespaceIntoYaml(invalidYaml, "default"));
        assertEquals(invalidYaml,
                K8sYamlUtils.injectNodeSelectorIntoYaml(invalidYaml, Map.of("pool", "workers")));
    }

    @Test
    public void testInjectEnvVarsPreservesReferencesAndExplicitOverrides() {
        for (String kind : List.of("Job", "Deployment")) {
            String yaml = """
                    apiVersion: %s
                    kind: %s
                    metadata:
                      name: reference-test
                    spec:
                      template:
                        spec:
                          containers:
                          - name: main
                            image: busybox
                            env:
                            - name: TOKEN
                              valueFrom:
                                secretKeyRef:
                                  name: credentials
                                  key: token
                            - name: CONFIG
                              valueFrom:
                                configMapKeyRef:
                                  name: settings
                                  key: config
                            - name: POD_NAME
                              valueFrom:
                                fieldRef:
                                  fieldPath: metadata.name
                            - name: CPU_LIMIT
                              valueFrom:
                                resourceFieldRef:
                                  resource: limits.cpu
                            - name: PLAIN
                              value: original
                    """.formatted(kind.equals("Job") ? "batch/v1" : "apps/v1", kind);

            String injected = K8sYamlUtils.injectEnvVarsIntoYaml(yaml, Map.of("EXTRA", "added"));
            List<EnvVar> env = environment(injected, kind);
            assertEquals(List.of("TOKEN", "CONFIG", "POD_NAME", "CPU_LIMIT", "PLAIN", "EXTRA"),
                    env.stream().map(EnvVar::getName).toList());
            assertEquals("credentials", env.get(0).getValueFrom().getSecretKeyRef().getName());
            assertEquals("settings", env.get(1).getValueFrom().getConfigMapKeyRef().getName());
            assertEquals("metadata.name", env.get(2).getValueFrom().getFieldRef().getFieldPath());
            assertEquals("limits.cpu", env.get(3).getValueFrom().getResourceFieldRef().getResource());
            assertEquals("original", env.get(4).getValue());
            assertEquals("added", env.get(5).getValue());

            List<EnvVar> overridden = environment(
                    K8sYamlUtils.injectEnvVarsIntoYaml(injected, Map.of("TOKEN", "override")), kind);
            assertEquals("TOKEN", overridden.get(0).getName());
            assertEquals("override", overridden.get(0).getValue());
            assertNull(overridden.get(0).getValueFrom());
            assertNotNull(overridden.get(1).getValueFrom());
        }
    }

    private static List<EnvVar> environment(String yaml, String kind) {
        PodSpec spec = kind.equals("Job")
                ? K8sYamlUtils.parseK8sYamlAndGetJobs(yaml).get(0).getSpec().getTemplate().getSpec()
                : K8sYamlUtils.parseK8sYamlAndGetDeployments(yaml).get(0).getSpec().getTemplate().getSpec();
        return spec.getContainers().get(0).getEnv();
    }

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

        String result = K8sYamlUtils.injectEnvVarsIntoYaml(yamlContent, envVars);

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
        assertNull(K8sYamlUtils.injectEnvVarsIntoYaml(null, new HashMap<>()));
        assertEquals("", K8sYamlUtils.injectEnvVarsIntoYaml("", new HashMap<>()));
        assertEquals("test", K8sYamlUtils.injectEnvVarsIntoYaml("test", null));
        assertEquals("test", K8sYamlUtils.injectEnvVarsIntoYaml("test", new HashMap<>()));
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

        var jobs = K8sYamlUtils.parseK8sYamlAndGetJobs(yamlContent);
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

        var deployments = K8sYamlUtils.parseK8sYamlAndGetDeployments(yamlContent);
        assertEquals(1, deployments.size());
        assertEquals("test-deployment", deployments.get(0).getMetadata().getName());
    }

    @Test
    public void testConvertToValidK8sName() {
        assertEquals("test-name", K8sYamlUtils.convertToValidK8sName("Test_Name"));
        assertEquals("test-name", K8sYamlUtils.convertToValidK8sName("test@name"));
        assertEquals("test-name", K8sYamlUtils.convertToValidK8sName("  test-name  "));
        assertEquals("test-name", K8sYamlUtils.convertToValidK8sName("test--name"));
        assertEquals("test-name", K8sYamlUtils.convertToValidK8sName("-test-name-"));
        assertNull(K8sYamlUtils.convertToValidK8sName(null));
        assertEquals("", K8sYamlUtils.convertToValidK8sName(""));
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

        String result = K8sYamlUtils.injectVolumeMountIntoYaml(
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
        assertNull(K8sYamlUtils.injectVolumeMountIntoYaml(null, "pvc", "volume", "/path", "subpath"));
        assertEquals("", K8sYamlUtils.injectVolumeMountIntoYaml("", "pvc", "volume", "/path", "subpath"));
        assertEquals("test", K8sYamlUtils.injectVolumeMountIntoYaml("test", null, "volume", "/path", "subpath"));
        assertEquals("test", K8sYamlUtils.injectVolumeMountIntoYaml("test", "pvc", null, "/path", "subpath"));
        assertEquals("test", K8sYamlUtils.injectVolumeMountIntoYaml("test", "pvc", "volume", null, "subpath"));
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

        String result = K8sYamlUtils.injectVolumeMountIntoYaml(
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

        String result = K8sYamlUtils.injectNamespaceIntoYaml(yamlContent, "my-namespace");

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

        String result = K8sYamlUtils.injectNamespaceIntoYaml(yamlContent, "new-namespace");

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
        assertNull(K8sYamlUtils.injectNamespaceIntoYaml(null, "namespace"));
        assertEquals("", K8sYamlUtils.injectNamespaceIntoYaml("", "namespace"));
        assertEquals("test", K8sYamlUtils.injectNamespaceIntoYaml("test", null));
        assertEquals("test", K8sYamlUtils.injectNamespaceIntoYaml("test", ""));
    }

    @Test
    public void testInjectNodeSelectorIntoYaml() {
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

        Map<String, String> nodeSelectors = new HashMap<>();
        nodeSelectors.put("disk-type", "ssd");
        nodeSelectors.put("zone", "us-west-1");

        String result = K8sYamlUtils.injectNodeSelectorIntoYaml(yamlContent, nodeSelectors);

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
      nodeSelector:
        disk-type: "ssd"
        zone: "us-west-1"
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
      nodeSelector:
        disk-type: "ssd"
        zone: "us-west-1"
""";
        assertEquals(expectedYaml, result);
    }

    @Test
    public void testInjectNodeSelectorIntoYamlWithEmptyInput() {
        assertNull(K8sYamlUtils.injectNodeSelectorIntoYaml(null, new HashMap<>()));
        assertEquals("", K8sYamlUtils.injectNodeSelectorIntoYaml("", new HashMap<>()));
        assertEquals("test", K8sYamlUtils.injectNodeSelectorIntoYaml("test", null));
        assertEquals("test", K8sYamlUtils.injectNodeSelectorIntoYaml("test", new HashMap<>()));
    }

    @Test
    public void testInjectNodeSelectorIntoYamlWithExistingNodeSelector() {
        String yamlContent = """
apiVersion: batch/v1
kind: Job
metadata:
  name: test-job
spec:
  template:
    spec:
      nodeSelector:
        existing-key: existing-value
      containers:
      - name: test-container
        image: busybox
""";

        Map<String, String> nodeSelectors = new HashMap<>();
        nodeSelectors.put("new-key", "new-value");

        String result = K8sYamlUtils.injectNodeSelectorIntoYaml(yamlContent, nodeSelectors);

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
      nodeSelector:
        existing-key: "existing-value"
        new-key: "new-value"
""";
        assertEquals(expectedYaml, result);
    }
}
