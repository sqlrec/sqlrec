package com.sqlrec.k8s;

import com.sqlrec.common.utils.SilenceLoggers;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.ServiceAccountBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.informers.impl.cache.Reflector;
import io.fabric8.kubernetes.client.utils.Serialization;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Real-cluster tests, selected by the integration profile. Uses the minikube context and owns its entire namespace.
 * Run with {@code mvn -o -pl sqlrec-core -am test -Prun-integration-tests
 * -Dtest=JobCompletionWatcherMinikubeIntegrationTest
 * -Dsurefire.failIfNoSpecifiedTests=false}.
 * The node needs docker.io/library/busybox:1.37.0 cached locally; successful Jobs do not pull images.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Timeout(120)
class JobCompletionWatcherMinikubeIntegrationTest {
    private KubernetesClient client;
    private String namespace;
    private final List<CompletableFuture<?>> observations = new ArrayList<>();

    @BeforeAll
    void createNamespace() {
        Config config = Config.autoConfigure("minikube");
        assertNotNull(config.getCurrentContext(), "The minikube kubeconfig context must exist");
        assertEquals("minikube", config.getCurrentContext().getName());
        client = new KubernetesClientBuilder().withConfig(config).build();
        String name = "sqlrec-job-watch-it-" + UUID.randomUUID().toString().substring(0, 8);
        namespace = client.namespaces().resource(new NamespaceBuilder().withNewMetadata().withName(name)
                .addToLabels("app.kubernetes.io/managed-by", "sqlrec-integration-test")
                .endMetadata().build()).create().getMetadata().getName();
        System.out.println("Minikube Job watcher test namespace: " + namespace);
    }

    @AfterEach
    void stopObservations() {
        observations.forEach(observation -> observation.cancel(false));
        observations.clear();
    }

    @AfterAll
    void deleteNamespace() throws Exception {
        if (client == null) return;
        try {
            if (namespace != null) {
                client.namespaces().withName(namespace).delete();
                client.namespaces().withName(namespace).waitUntilCondition(Objects::isNull, 60, TimeUnit.SECONDS);
                System.out.println("Deleted Minikube test namespace: " + namespace);
            }
        } finally {
            KubernetesTestSupport.closeClient(client);
        }
    }

    @Test
    void observesRealJobSuccessAcrossAllCompletions() throws Exception {
        Job job = createJob("success", "sleep 1; exit 0", 2);
        var completion = watch(job);
        assertEquals("succeeded", completion.get(90, TimeUnit.SECONDS).state());
        Job finished = client.batch().v1().jobs().inNamespace(namespace).withName("success").get();
        assertEquals(2, finished.getStatus().getSucceeded());
        assertEquals(job.getMetadata().getUid(), finished.getMetadata().getUid());
    }

    @Test
    void initialSnapshotRecognizesAnAlreadyCompletedJob() throws Exception {
        Job job = createJob("already-complete", "exit 0", 1);
        assertEquals("succeeded", watch(job).get(90, TimeUnit.SECONDS).state());
        assertEquals(1, client.batch().v1().jobs().inNamespace(namespace).withName("already-complete")
                .get().getStatus().getSucceeded());
        assertEquals("succeeded", watch(job).get(15, TimeUnit.SECONDS).state());
    }

    @Test
    @SilenceLoggers(K8sManager.class)
    void nonzeroContainerExitProducesARealJobFailure() throws Exception {
        Job job = createJob("failure", "exit 17", 1);
        K8sManager.JobStatus status = watch(job).get(90, TimeUnit.SECONDS);
        assertEquals("failed", status.state());
        assertTrue(status.detail().contains("Job failed"), status.detail());
        Job failed = client.batch().v1().jobs().inNamespace(namespace).withName("failure").get();
        assertTrue(failed.getStatus().getConditions().stream().anyMatch(condition ->
                "Failed".equals(condition.getType()) && "True".equals(condition.getStatus())));
    }

    @Test
    void kubeletImageErrorIsReportedBeforeTheJobBecomesTerminal() throws Exception {
        Job definition = new JobBuilder(job("invalid-image", "exit 0", 1)).editSpec().editTemplate().editSpec()
                .editFirstContainer().withImage("invalid image reference").endContainer().endSpec().endTemplate().endSpec().build();
        Job created = client.batch().v1().jobs().inNamespace(namespace).resource(definition).create();
        K8sManager.JobStatus status = watch(created).get(90, TimeUnit.SECONDS);
        assertEquals("failed", status.state());
        assertTrue(status.detail().contains("InvalidImageName"), status.detail());
        Job current = client.batch().v1().jobs().inNamespace(namespace).withName("invalid-image").get();
        assertTrue(current.getStatus().getConditions() == null || current.getStatus().getConditions().stream()
                .noneMatch(condition -> "Failed".equals(condition.getType()) && "True".equals(condition.getStatus())));
    }

    @Test
    void waitTimeoutDoesNotStopObservationOrTheJob() throws Exception {
        Job job = createJob("timeout", "sleep 3; exit 0", 1);
        var completion = watch(job);
        assertThrows(TimeoutException.class, () -> completion.get(25, TimeUnit.MILLISECONDS));
        assertFalse(completion.isDone());
        assertNotNull(client.batch().v1().jobs().inNamespace(namespace).withName("timeout").get());
        assertEquals("succeeded", completion.get(90, TimeUnit.SECONDS).state());
    }

    @Test
    void cancellingObservationLeavesTheSameJobAlive() {
        Job job = createJob("cancel", "sleep 60", 1);
        var completion = watch(job);
        assertTrue(completion.cancel(false));
        assertTrue(completion.isCancelled());
        Job current = client.batch().v1().jobs().inNamespace(namespace).withName("cancel").get();
        assertNotNull(current);
        assertEquals(job.getMetadata().getUid(), current.getMetadata().getUid());
        assertNull(current.getMetadata().getDeletionTimestamp());
    }

    @Test
    void deletedJobCompletesAsFailed() throws Exception {
        Job job = createJob("delete", "sleep 60", 1);
        var completion = watch(job);
        client.batch().v1().jobs().inNamespace(namespace).withName("delete").delete();
        K8sManager.JobStatus status = completion.get(30, TimeUnit.SECONDS);
        assertEquals("failed", status.state());
        assertTrue(status.detail().contains("not found"), status.detail());
    }

    @Test
    void delayedCleanupOfAnOldAttemptLeavesTheNewAttemptsResourcesAlive() throws Exception {
        String previousId = K8sYamlUtils.checkpointAttemptJobId("train", "rank", "v1");
        String nextId = K8sYamlUtils.checkpointAttemptJobId("train", "rank", "v1");
        Job previous = createJob(previousId + "-job", "exit 0", 1);
        assertEquals("succeeded", watch(previous).get(90, TimeUnit.SECONDS).state());
        Job next = createJob(nextId + "-job", "sleep 60", 1);
        var previousConfig = client.configMaps().inNamespace(namespace).resource(new ConfigMapBuilder()
                .withNewMetadata().withName(previousId + "-cm").endMetadata().build()).create();
        client.configMaps().inNamespace(namespace).resource(new ConfigMapBuilder()
                .withNewMetadata().withName(nextId + "-cm").endMetadata().build()).create();
        var cleanupClient = new KubernetesClientBuilder().withConfig(client.getConfiguration()).build();
        K8sManager.setKubernetesClientForTest(cleanupClient);
        try {
            K8sManager.deleteYaml(Serialization.asYaml(previous) + "\n---\n" + Serialization.asYaml(previousConfig));
            assertNull(client.batch().v1().jobs().inNamespace(namespace).withName(previousId + "-job").get());
            assertNull(client.configMaps().inNamespace(namespace).withName(previousId + "-cm").get());
            assertEquals(next.getMetadata().getUid(), client.batch().v1().jobs().inNamespace(namespace)
                    .withName(nextId + "-job").get().getMetadata().getUid());
            assertNotNull(client.configMaps().inNamespace(namespace).withName(nextId + "-cm").get());
        } finally {
            K8sManager.kubernetesClient = null;
            KubernetesTestSupport.closeClient(cleanupClient);
        }
    }

    @Test
    void retryDeletionWaitsForTheJobAndItsPodsBeforeClientClose() throws Exception {
        Job job = createJob("retry-delete", "sleep 60", 1);
        var retryClient = new KubernetesClientBuilder().withConfig(client.getConfiguration()).build();
        K8sManager.setKubernetesClientForTest(retryClient);
        try {
            var completion = JobCompletionWatcher.watch(retryClient, job);
            observations.add(completion);
            K8sManager.deleteYamlAndWait(Serialization.asYaml(job));
            assertEquals("failed", completion.get(10, TimeUnit.SECONDS).state());
            assertNull(client.batch().v1().jobs().inNamespace(namespace).withName("retry-delete").get());
            assertTrue(client.pods().inNamespace(namespace).withLabel("job-name", "retry-delete").list().getItems().isEmpty());
        } finally {
            K8sManager.kubernetesClient = null;
            KubernetesTestSupport.closeClient(retryClient);
        }
    }

    @Test
    @SilenceLoggers(Reflector.class)
    void missingWatchPermissionsAreAnObserverErrorAndDoNotDeleteTheJob() throws Exception {
        Job job = createJob("forbidden", "sleep 60", 1);
        client.serviceAccounts().inNamespace(namespace).resource(new ServiceAccountBuilder()
                .withNewMetadata().withName("unprivileged").endMetadata().build()).create();
        Config config = new ConfigBuilder(client.getConfiguration())
                .withImpersonateUsername("system:serviceaccount:" + namespace + ":unprivileged").build();
        var unprivileged = new KubernetesClientBuilder().withConfig(config).build();
        try {
            var completion = JobCompletionWatcher.watch(unprivileged, job);
            observations.add(completion);
            ExecutionException error = assertThrows(ExecutionException.class, () -> completion.get(20, TimeUnit.SECONDS));
            assertTrue(error.toString().toLowerCase().contains("forbidden"), error.toString());
            assertTrue(completion.isCompletedExceptionally());
        } finally {
            KubernetesTestSupport.closeClient(unprivileged);
        }
        assertNotNull(client.batch().v1().jobs().inNamespace(namespace).withName("forbidden").get());
    }

    private CompletableFuture<K8sManager.JobStatus> watch(Job job) {
        var completion = JobCompletionWatcher.watch(client, job);
        observations.add(completion);
        return completion;
    }

    private Job createJob(String name, String command, int completions) {
        return client.batch().v1().jobs().inNamespace(namespace).resource(job(name, command, completions)).create();
    }

    private Job job(String name, String command, int completions) {
        return new JobBuilder().withNewMetadata().withName(name).withNamespace(namespace).endMetadata()
                .withNewSpec().withBackoffLimit(0).withCompletions(completions).withParallelism(1).withActiveDeadlineSeconds(110L)
                .withNewTemplate().withNewSpec().withRestartPolicy("Never").withTerminationGracePeriodSeconds(1L)
                .addNewContainer().withName("task").withImage("docker.io/library/busybox:1.37.0").withImagePullPolicy("Never")
                .withCommand("sh", "-c", command).withResources(new ResourceRequirementsBuilder()
                        .addToRequests("cpu", new Quantity("10m")).addToRequests("memory", new Quantity("8Mi"))
                        .addToLimits("cpu", new Quantity("100m")).addToLimits("memory", new Quantity("32Mi")).build())
                .endContainer().endSpec().endTemplate().endSpec().build();
    }
}
