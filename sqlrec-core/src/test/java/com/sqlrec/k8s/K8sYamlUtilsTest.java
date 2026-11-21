package com.sqlrec.k8s;

import org.junit.jupiter.api.Test;

import java.util.List;

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
}
