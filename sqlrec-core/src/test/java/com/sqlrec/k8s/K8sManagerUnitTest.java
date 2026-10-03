package com.sqlrec.k8s;

import com.sqlrec.common.utils.SilenceLoggers;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.PodListBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.apps.DeploymentStatusBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentList;
import io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.RollableScalableResource;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.JobList;
import io.fabric8.kubernetes.api.model.batch.v1.JobStatusBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.BatchAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.FilterWatchListDeletable;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.NamespaceableResource;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.fabric8.kubernetes.client.dsl.ScalableResource;
import io.fabric8.kubernetes.client.dsl.V1BatchAPIGroupDSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.InputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.inOrder;
import org.mockito.InOrder;

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
    @SuppressWarnings("unchecked")
    public void testDeleteYamlAndWaitDeletesJobBeforeConfigMap() {
        NamespaceableResource<HasMetadata> jobHandle = mock(NamespaceableResource.class);
        NamespaceableResource<HasMetadata> configHandle = mock(NamespaceableResource.class);
        when(mockClient.resource(any(HasMetadata.class))).thenAnswer(invocation ->
                "Job".equals(((HasMetadata) invocation.getArgument(0)).getKind())
                        ? jobHandle : configHandle);
        // A user may already have deleted the Job; Fabric8 returns an empty result for 404.
        when(jobHandle.delete()).thenReturn(List.of());

        K8sManager.deleteYamlAndWait("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: cfg\n"
                + "---\napiVersion: batch/v1\nkind: Job\nmetadata:\n  name: train\n");

        InOrder order = inOrder(jobHandle, configHandle);
        order.verify(jobHandle).delete();
        order.verify(jobHandle).waitUntilCondition(any(), eq(60L), eq(TimeUnit.SECONDS));
        order.verify(configHandle).delete();
        order.verify(configHandle).waitUntilCondition(any(), eq(60L), eq(TimeUnit.SECONDS));
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
    @SilenceLoggers(K8sManager.class)
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

    @Test
    public void testDeploymentReadinessPreservesDefaultCountsAndCriteria() {
        var resource = deploymentResource();
        var deployment = new DeploymentBuilder().withNewMetadata().withName("serve").endMetadata()
                .withNewSpec().withReplicas(1).endSpec().withNewStatus()
                .withReadyReplicas(1).withUpdatedReplicas(1).endStatus().build();
        when(resource.get()).thenReturn(deployment);
        String yaml = "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: serve\n";

        // Available replicas are diagnostic only.
        assertTrue(K8sManager.isDeploymentReadyFromYaml(yaml));
        deployment.getSpec().setReplicas(null);
        // The original ternary unboxes a null replica count and falls back to not ready.
        assertFalse(K8sManager.isDeploymentReadyFromYaml(yaml));
        deployment.setSpec(null);
        assertTrue(K8sManager.isDeploymentReadyFromYaml(yaml));
        deployment.setSpec(new DeploymentBuilder().withNewSpec().withReplicas(1).endSpec().build().getSpec());
        deployment.getStatus().setUnavailableReplicas(1);
        assertFalse(K8sManager.isDeploymentReadyFromYaml(yaml));
        deployment.getStatus().setUnavailableReplicas(null);
        deployment.getStatus().setUpdatedReplicas(0);
        assertFalse(K8sManager.isDeploymentReadyFromYaml(yaml));
        deployment.getStatus().setUpdatedReplicas(1);
        deployment.getStatus().setReadyReplicas(null);
        assertFalse(K8sManager.isDeploymentReadyFromYaml(yaml));

        deployment.getSpec().setReplicas(0);
        deployment.setStatus(new DeploymentStatusBuilder().build());
        assertTrue(K8sManager.isDeploymentReadyFromYaml(yaml));
        deployment.setStatus(null);
        assertFalse(K8sManager.isDeploymentReadyFromYaml(yaml));
        when(resource.get()).thenReturn(null);
        assertFalse(K8sManager.isDeploymentReadyFromYaml(yaml));
    }

    @Test
    @SilenceLoggers(K8sManager.class)
    public void testDeploymentTransportFailureResetsClientAndReturnsNotReady() {
        when(deploymentResource().get())
                .thenThrow(new RuntimeException(new IOException("connection reset")));

        assertFalse(K8sManager.isDeploymentReadyFromYaml(
                "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: serve\n"));

        verify(mockClient).close();
        assertNull(K8sManager.kubernetesClient);
    }

    @Test
    @SilenceLoggers(K8sManager.class)
    public void testJobTransportFailureResetsClientAndKeepsRunningFallback() {
        when(jobResource().get())
                .thenThrow(new RuntimeException(new IOException("connection reset")));

        assertEquals("running", K8sManager.checkJobsStatusFromYaml(
                "apiVersion: batch/v1\nkind: Job\nmetadata:\n  name: train\n"));

        verify(mockClient).close();
        assertNull(K8sManager.kubernetesClient);
    }

    @Test
    public void testImagePullFailureIncludesContainerReasonAndMessage() {
        Pod pod = new PodBuilder().withNewStatus()
                .addNewInitContainerStatus().withName("init")
                    .withNewState().withNewWaiting().withReason("ImagePullBackOff")
                        .withMessage("manifest not found").endWaiting().endState()
                .endInitContainerStatus()
                .endStatus().build();

        assertEquals("container init is waiting: ImagePullBackOff (manifest not found)",
                K8sManager.imagePullFailure(pod));
        assertNull(K8sManager.imagePullFailure(new PodBuilder().build()));
    }

    @SuppressWarnings("unchecked")
    private RollableScalableResource<Deployment> deploymentResource() {
        AppsAPIGroupDSL apps = mock(AppsAPIGroupDSL.class);
        MixedOperation<Deployment, DeploymentList, RollableScalableResource<Deployment>> deployments =
                mock(MixedOperation.class);
        NonNamespaceOperation<Deployment, DeploymentList, RollableScalableResource<Deployment>> namespaced =
                mock(NonNamespaceOperation.class);
        RollableScalableResource<Deployment> resource = mock(RollableScalableResource.class);
        when(mockClient.apps()).thenReturn(apps);
        when(apps.deployments()).thenReturn(deployments);
        when(deployments.inNamespace("default")).thenReturn(namespaced);
        when(namespaced.withName("serve")).thenReturn(resource);
        return resource;
    }

    @SuppressWarnings("unchecked")
    private ScalableResource<Job> jobResource() {
        BatchAPIGroupDSL batch = mock(BatchAPIGroupDSL.class);
        V1BatchAPIGroupDSL v1 = mock(V1BatchAPIGroupDSL.class);
        MixedOperation<Job, JobList, ScalableResource<Job>> jobs = mock(MixedOperation.class);
        NonNamespaceOperation<Job, JobList, ScalableResource<Job>> namespaced = mock(NonNamespaceOperation.class);
        ScalableResource<Job> resource = mock(ScalableResource.class);
        when(mockClient.batch()).thenReturn(batch);
        when(batch.v1()).thenReturn(v1);
        when(v1.jobs()).thenReturn(jobs);
        when(jobs.inNamespace("default")).thenReturn(namespaced);
        when(namespaced.withName("train")).thenReturn(resource);
        return resource;
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testPendingJobWithErrImagePullIsFailed() {
        KubernetesClient client = mock(KubernetesClient.class);
        K8sManager.setKubernetesClientForTest(client);
        BatchAPIGroupDSL batch = mock(BatchAPIGroupDSL.class);
        V1BatchAPIGroupDSL v1 = mock(V1BatchAPIGroupDSL.class);
        MixedOperation<Job, JobList, ScalableResource<Job>> jobs = mock(MixedOperation.class);
        NonNamespaceOperation<Job, JobList, ScalableResource<Job>> namespacedJobs = mock(NonNamespaceOperation.class);
        ScalableResource<Job> namedJob = mock(ScalableResource.class);
        MixedOperation<Pod, PodList, PodResource> pods = mock(MixedOperation.class);
        NonNamespaceOperation<Pod, PodList, PodResource> namespacedPods = mock(NonNamespaceOperation.class);
        FilterWatchListDeletable<Pod, PodList, PodResource> selectedPods = mock(FilterWatchListDeletable.class);
        Job job = new JobBuilder().withNewMetadata().withName("train").withNamespace("sqlrec")
                .endMetadata().withNewSpec().withNewSelector()
                .addToMatchLabels("batch.kubernetes.io/controller-uid", "job-uid")
                .endSelector().endSpec().build();
        Pod pod = new PodBuilder().withNewMetadata().withName("train-abc").endMetadata()
                .withNewStatus().addNewContainerStatus().withName("trainer")
                    .withNewState().withNewWaiting().withReason("ErrImagePull")
                        .withMessage("pull access denied").endWaiting().endState()
                .endContainerStatus().endStatus().build();
        when(client.batch()).thenReturn(batch);
        when(batch.v1()).thenReturn(v1);
        when(v1.jobs()).thenReturn(jobs);
        when(jobs.inNamespace("sqlrec")).thenReturn(namespacedJobs);
        when(namespacedJobs.withName("train")).thenReturn(namedJob);
        when(namedJob.get()).thenReturn(job);
        when(client.pods()).thenReturn(pods);
        when(pods.inNamespace("sqlrec")).thenReturn(namespacedPods);
        when(namespacedPods.withLabels(Map.of("batch.kubernetes.io/controller-uid", "job-uid")))
                .thenReturn(selectedPods);
        when(selectedPods.list()).thenReturn(new PodListBuilder().addToItems(pod).build());

        String yaml = "apiVersion: batch/v1\nkind: Job\nmetadata:\n  name: train\n  namespace: sqlrec\n";
        K8sManager.JobStatus status = K8sManager.checkJobsStatusDetailFromYaml(yaml);

        assertEquals("failed", status.state());
        assertTrue(status.detail().contains("Pod train-abc"));
        assertTrue(status.detail().contains("ErrImagePull"));
        assertTrue(status.detail().contains("pull access denied"));

        when(selectedPods.list()).thenReturn(new PodListBuilder().build());
        job.setStatus(new JobStatusBuilder().withFailed(1).build());
        assertEquals("running", K8sManager.checkJobsStatusDetailFromYaml(yaml).state());

        job.setStatus(new JobStatusBuilder().withFailed(1).withSucceeded(1).addNewCondition()
                .withType("Failed").withStatus("True").withMessage("BackoffLimitExceeded")
                .endCondition().build());
        K8sManager.JobStatus failedJob = K8sManager.checkJobsStatusDetailFromYaml(yaml);
        assertEquals("failed", failedJob.state());
        assertTrue(failedJob.detail().contains("BackoffLimitExceeded"));

        job.setStatus(new JobStatusBuilder().withSucceeded(1).build());
        assertEquals("succeeded", K8sManager.checkJobsStatusDetailFromYaml(yaml).state());

        job.setStatus(new JobStatusBuilder().withActive(1).withReady(1).build());
        clearInvocations(pods);
        assertEquals("running", K8sManager.checkJobsStatusDetailFromYaml(yaml).state());
        verifyNoInteractions(pods);

        // A new Pending Pod makes active > ready, so image-pull detection resumes.
        job.setStatus(new JobStatusBuilder().withActive(2).withReady(1).build());
        when(selectedPods.list()).thenReturn(new PodListBuilder().addToItems(pod).build());
        assertEquals("failed", K8sManager.checkJobsStatusDetailFromYaml(yaml).state());

        when(namedJob.get()).thenReturn(null);
        K8sManager.JobStatus missingJob = K8sManager.checkJobsStatusDetailFromYaml(yaml);
        assertEquals("failed", missingJob.state());
        assertTrue(missingJob.detail().contains("Job not found"));
    }
}
