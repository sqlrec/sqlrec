package com.sqlrec.k8s;

import io.fabric8.kubernetes.api.model.APIResourceListBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
    void resourceListIsExpandedForStatusAndCleanup() {
        String yaml = """
                apiVersion: v1
                kind: List
                items:
                - apiVersion: batch/v1
                  kind: Job
                  metadata: {name: train}
                - apiVersion: v1
                  kind: ConfigMap
                  metadata: {name: train-config}
                - apiVersion: apps/v1
                  kind: Deployment
                  metadata: {name: serve}
                """;
        assertEquals(List.of("train", "train-config", "serve"), K8sYamlUtils.parseK8sYaml(yaml).stream()
                .map(resource -> resource.getMetadata().getName()).toList());
        assertEquals("train", K8sYamlUtils.parseK8sYamlAndGetJobs(yaml).getFirst().getMetadata().getName());
        assertEquals("serve", K8sYamlUtils.parseK8sYamlAndGetDeployments(yaml).getFirst().getMetadata().getName());
        assertTrue(K8sYamlUtils.parseK8sYaml("# no resource documents\n").isEmpty());
    }

    @Test
    void bindingPreservesExplicitNamespacesAndClusterScopedResources() {
        String yaml = """
                apiVersion: v1
                kind: ConfigMap
                metadata: {name: config, namespace: explicit}
                ---
                apiVersion: v1
                kind: Namespace
                metadata: {name: cluster-scoped}
                ---
                apiVersion: batch/v1
                kind: Job
                metadata: {name: train, namespace: ''}
                """;
        KubernetesClient client = mock(KubernetesClient.class);
        when(client.getNamespace()).thenReturn("client");
        String bound = K8sYamlUtils.bindNamespaces(yaml, K8sYamlUtils.parseK8sYaml(yaml), client);
        var resources = K8sYamlUtils.parseK8sYaml(bound);
        assertEquals("explicit", resources.get(0).getMetadata().getNamespace());
        assertNull(resources.get(1).getMetadata().getNamespace());
        assertEquals("client", resources.get(2).getMetadata().getNamespace());
        assertSame(bound, K8sYamlUtils.bindNamespaces(bound, resources, client));

        String invalid = "apiVersion: batch/v1\nkind: Job\nmetadata: {name: train, namespace: ' '}\n";
        assertSame(invalid, K8sYamlUtils.bindNamespaces(invalid, K8sYamlUtils.parseK8sYaml(invalid), client));
    }

    @Test
    void missingClientNamespaceRequiresExplicitScopeForNewResources() {
        String yaml = "apiVersion: batch/v1\nkind: Job\nmetadata: {name: train}\n";
        KubernetesClient client = mock(KubernetesClient.class);
        for (String namespace : new String[]{null, " "}) {
            when(client.getNamespace()).thenReturn(namespace);
            var failure = assertThrows(IllegalArgumentException.class,
                    () -> K8sYamlUtils.bindNamespaces(yaml, K8sYamlUtils.parseK8sYaml(yaml), client));
            assertTrue(failure.getMessage().contains("Set metadata.namespace for Job"));
        }

        Job job = new Job();
        job.setMetadata(null);
        when(client.getNamespace()).thenReturn("client");
        String bound = K8sYamlUtils.bindNamespaces(yaml, List.of(job), client);
        assertEquals("client", K8sYamlUtils.parseK8sYamlAndGetJobs(bound).getFirst().getMetadata().getNamespace());
    }

    @Test
    void discoveryDistinguishesCustomResourceScopes() {
        String yaml = """
                apiVersion: review.example/v1
                kind: WorkItem
                metadata: {name: work}
                ---
                apiVersion: review.example/v1
                kind: ClusterSetting
                metadata: {name: setting}
                """;
        KubernetesClient client = mock(KubernetesClient.class);
        when(client.getNamespace()).thenReturn("client");
        when(client.getApiResources("review.example/v1")).thenReturn(new APIResourceListBuilder()
                .addNewResource().withName("workitems/status").withKind("WorkItem").withNamespaced(false).endResource()
                .addNewResource().withName("workitems").withKind("WorkItem").withNamespaced(true).endResource()
                .addNewResource().withName("clustersettings").withKind("ClusterSetting").withNamespaced(false).endResource()
                .build());

        String bound = K8sYamlUtils.bindNamespaces(yaml, K8sYamlUtils.parseK8sYaml(yaml), client);
        var resources = K8sYamlUtils.parseK8sYaml(bound);
        assertEquals("client", resources.get(0).getMetadata().getNamespace());
        assertNull(resources.get(1).getMetadata().getNamespace());
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
}
