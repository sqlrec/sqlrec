package com.sqlrec.k8s;

import io.fabric8.kubernetes.client.KubernetesClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

/**
 * Mock unit tests for K8sManager.
 * Injects a mock KubernetesClient via setKubernetesClientForTest.
 * Since fabric8's fluent API chain is hard to mock step by step, this test focuses on:
 * 1. Boundary conditions (empty / null input) should return without touching the client;
 * 2. Verifying the injection point works: non-empty input causes the mockClient to be used.
 */
@ExtendWith(MockitoExtension.class)
public class K8sManagerUnitTest {

    @Mock
    private KubernetesClient mockClient;

    @BeforeEach
    public void setUp() {
        // Inject mock client to avoid triggering real KubernetesClient creation
        K8sManager.setKubernetesClientForTest(mockClient);
    }

    @AfterEach
    public void tearDown() {
        // Clean up cached client to avoid polluting other tests
        K8sManager.resetClient();
    }

    @Test
    public void testApplyYamlEmpty() {
        // Empty string and null should return immediately without calling the client
        K8sManager.applyYaml("");
        K8sManager.applyYaml(null);

        verifyNoInteractions(mockClient);
    }

    @Test
    public void testDeleteYamlEmpty() {
        // Empty string and null should return immediately without calling the client
        K8sManager.deleteYaml("");
        K8sManager.deleteYaml(null);

        verifyNoInteractions(mockClient);
    }

    @Test
    public void testCheckJobsStatusFromYamlEmpty() {
        // Empty input means no Job to check; should return "succeeded" without calling the client
        assertEquals("succeeded", K8sManager.checkJobsStatusFromYaml(""));
        assertEquals("succeeded", K8sManager.checkJobsStatusFromYaml(null));

        verifyNoInteractions(mockClient);
    }

    @Test
    public void testIsDeploymentReadyFromYamlEmpty() {
        // Empty input means no Deployment to check; should return true without calling the client
        assertTrue(K8sManager.isDeploymentReadyFromYaml(""));
        assertTrue(K8sManager.isDeploymentReadyFromYaml(null));

        verifyNoInteractions(mockClient);
    }

    @Test
    public void testApplyYamlUsesInjectedClient() {
        // Non-empty YAML will call client.load(...).serverSideApply().
        // The fluent chain is not stubbed, so mock's load() returns null,
        // then serverSideApply() throws NPE which applyYaml catches and wraps as RuntimeException.
        // The main purpose is to verify the mockClient is actually used (injection point works).
        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> K8sManager.applyYaml("apiVersion: v1\nkind: ConfigMap"));

        assertTrue(ex.getMessage().contains("Failed to apply YAML"));
        // Verify the injected mockClient was called
        verify(mockClient).load(any(InputStream.class));
    }
}
