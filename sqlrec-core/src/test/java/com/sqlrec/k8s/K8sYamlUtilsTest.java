package com.sqlrec.k8s;

import com.sqlrec.common.utils.SilenceLoggers;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.PodSpec;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class K8sYamlUtilsTest {

    @Test
    void serviceIdsUseTheNameWithoutSuffixOrLossyConversion() {
        for (String name : List.of("a", "rank-service", "rank--a", "rank1", "a".repeat(63))) {
            assertEquals(name, K8sYamlUtils.serviceResourceId(name));
        }
    }

    @Test
    void serviceIdsRejectInvalidNamesRatherThanRepairingThem() {
        for (String name : List.of("", " ", "Rank-service", "ITEM", "Krank", "rank_a", "rank__a", "rank.a", "rank/a", "rank a",
                " rank", "rank ", "模型", "1rank", "-rank", "rank-", "a".repeat(64))) {
            IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                    () -> K8sYamlUtils.serviceResourceId(name), name);
            assertTrue(error.getMessage().contains("Invalid service name"));
        }
        assertThrows(IllegalArgumentException.class, () -> K8sYamlUtils.serviceResourceId(null));
    }

    @Test
    @SilenceLoggers(K8sYamlUtils.class)
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
    void retryJobIdsKeepAttemptsResourcesSeparateAndFitResourceNames() {
        for (String operation : List.of("train", "export")) {
            String first = K8sYamlUtils.checkpointJobId(operation, "rank", "v1");
            String retry = K8sYamlUtils.checkpointAttemptJobId(operation, "rank", "v1");
            String next = K8sYamlUtils.checkpointAttemptJobId(operation, "rank", "v1");
            assertNotEquals(first, retry);
            assertNotEquals(retry, next);
            assertTrue(retry.startsWith(operation + "-rank-v1-"));
            assertTrue((retry + "-job-headless").length() <= 63);
        }
    }

    @Test
    void checkpointJobIdsAreStableAndPreserveDistinctLogicalNames() {
        String id = K8sYamlUtils.checkpointJobId("train", "rank", "v1.0");
        assertTrue(id.matches("train-rank-v1-0-[0-9a-f]{16}"), id);
        assertEquals(id, K8sYamlUtils.checkpointJobId("train", "rank", "v1.0"));
        assertNotEquals(id, K8sYamlUtils.checkpointJobId("train", "rank", "v1_0"));
        assertNotEquals(K8sYamlUtils.checkpointJobId("train", "rank", "v1--a"),
                K8sYamlUtils.checkpointJobId("train", "rank", "v1-a"));
        assertNotEquals(K8sYamlUtils.checkpointJobId("train", "a-b", "c"),
                K8sYamlUtils.checkpointJobId("train", "a", "b-c"));
        assertNotEquals(K8sYamlUtils.checkpointJobId("train", "rank", "v1-export"),
                K8sYamlUtils.checkpointJobId("export", "rank", "v1"));
        assertThrows(IllegalArgumentException.class,
                () -> K8sYamlUtils.checkpointJobId("other", "rank", "v1"));
    }

    @Test
    void checkpointJobIdsKeepReadablePartsAndHandleNonDnsCharacters() {
        String export = K8sYamlUtils.checkpointJobId("export", "Rank_Model", "v1.0/user");
        assertTrue(export.matches("export-rank-model-v1-0-user-[0-9a-f]{16}"), export);
        String fallback = K8sYamlUtils.checkpointJobId("train", "推荐", "版本");
        assertTrue(fallback.matches("train-x-x-[0-9a-f]{16}"), fallback);
        assertNotEquals(fallback, K8sYamlUtils.checkpointJobId("train", "推荐", "其他"));
        assertTrue(K8sYamlUtils.checkpointJobId("train", "very-long-model-name", "v1")
                .startsWith("train-very-long-model-name-v1-"));
        assertTrue(K8sYamlUtils.checkpointJobId("export", "rank", "checkpoint-with-long-name")
                .startsWith("export-rank-checkpoint-with-long-"));
    }

    @Test
    void checkpointJobIdsFitAllResourceSuffixesAndHashFullLogicalNames() {
        String model = "model".repeat(100);
        String checkpoint = "version".repeat(100);
        for (String operation : java.util.List.of("train", "export")) {
            String id = K8sYamlUtils.checkpointJobId(operation, model, checkpoint);
            assertTrue(id.matches(operation + "-model[a-z]*-version[a-z]*-[0-9a-f]{16}"), id);
            assertNotEquals(id, K8sYamlUtils.checkpointJobId(operation, model + "x", checkpoint));
            assertNotEquals(id, K8sYamlUtils.checkpointJobId(operation, model, checkpoint + "x"));
            for (String suffix : java.util.List.of("-cm", "-job", "-job-headless")) {
                String name = id + suffix;
                assertTrue(name.length() <= 63);
                assertTrue(name.matches("[a-z]([-a-z0-9]*[a-z0-9])?"), name);
            }
        }
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
