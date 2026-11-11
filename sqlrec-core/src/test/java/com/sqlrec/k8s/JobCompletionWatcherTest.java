package com.sqlrec.k8s;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.JobList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.ScalableResource;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.fabric8.kubernetes.client.dsl.FilterWatchListDeletable;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.cache.Store;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class JobCompletionWatcherTest {
    @Test
    void queuedSuccessIsNotLostWhenTheCacheHasAlreadyRemovedTheJob() throws Exception {
        Fixture f = new Fixture();
        var completion = f.watch();
        f.jobs.items = List.of();

        f.jobs.handler.onUpdate(f.job(0), f.job(2));

        assertEquals("succeeded", completion.get(5, TimeUnit.SECONDS).state());
        f.jobs.assertStopped();
    }

    @Test
    void queuedEventsBindTheOriginalUidEvenIfTheCacheAlreadyContainsItsReplacement() throws Exception {
        Fixture f = new Fixture();
        var completion = f.watch();
        Job replacement = new JobBuilder(f.job(0)).editMetadata().withUid("new-uid").endMetadata().build();
        f.jobs.items = List.of(replacement);
        f.jobs.handler.onAdd(f.job(0));
        f.jobs.handler.onAdd(replacement);

        var error = assertThrows(java.util.concurrent.ExecutionException.class,
                () -> completion.get(5, TimeUnit.SECONDS));
        assertTrue(error.getCause().getMessage().contains("replaced"));
        f.jobs.assertStopped();
        f.pods.assertStopped();
    }

    @Test
    void cleanupFailureStillStopsTheOtherObserver() {
        Fixture f = new Fixture();
        IllegalStateException failure = new IllegalStateException("stop failed");
        f.jobs.onStop = () -> { throw failure; };
        var completion = f.watch();
        f.jobs.emit(List.of(f.job(0)));
        f.jobs.emit(List.of(f.job(2)));
        assertSame(failure, assertThrows(CompletionException.class, completion::join).getCause());
        f.pods.assertStopped();
    }

    @Test
    void completionIsPublishedOnlyAfterObserverCleanup() throws Exception {
        Fixture f = new Fixture();
        CountDownLatch stopping = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        f.jobs.onStop = () -> {
            stopping.countDown();
            assertDoesNotThrow(() -> assertTrue(release.await(5, TimeUnit.SECONDS)));
        };
        var completion = f.watch();
        try {
            f.jobs.emit(List.of(f.job(2)));
            assertTrue(stopping.await(5, TimeUnit.SECONDS));
            assertFalse(completion.isDone(), "Callers must not close the client while observer cleanup is pending");
        } finally {
            release.countDown();
        }
        assertEquals("succeeded", completion.get(5, TimeUnit.SECONDS).state());
        f.jobs.assertStopped();
    }

    @Test
    void alreadySucceededJobCompletesWithoutStartingPodObservation() {
        Fixture f = new Fixture();
        f.jobs.initial = List.of(f.job(2));
        var completion = f.watch();
        assertEquals("succeeded", completion.join().state());
        assertNull(f.pods.handler);
        f.jobs.assertStopped();
    }

    @Test
    void waitsForAllCompletionsAndClosesBothObservations() {
        Fixture f = new Fixture();
        var completion = f.watch();
        f.jobs.emit(List.of(f.job(0)));
        assertFalse(completion.isDone());
        f.jobs.emit(List.of(f.job(1)));
        assertFalse(completion.isDone());
        f.jobs.emit(List.of(f.job(2)));
        assertEquals("succeeded", completion.join().state());
        f.jobs.assertStopped();
        f.pods.assertStopped();
        verify(f.client.batch().v1().jobs().inNamespace("test").withName("train"), never()).get();
    }

    @Test
    void failedConditionWinsOverSucceededCount() {
        Fixture f = new Fixture();
        var completion = f.watch();
        Job failed = new JobBuilder(f.job(2)).editStatus().addNewCondition()
                .withType("Failed").withStatus("True").withMessage("trainer crashed")
                .endCondition().endStatus().build();
        f.jobs.emit(List.of(failed));
        assertEquals("failed", completion.join().state());
        assertTrue(completion.join().detail().contains("trainer crashed"));
    }

    @Test
    void podFailureOnlyBelongsToTheObservedJobUid() {
        Fixture f = new Fixture();
        var completion = f.watch();
        f.jobs.emit(List.of(f.job(0)));
        f.pods.emit(List.of(f.failedPod("previous-uid")));
        assertFalse(completion.isDone());
        f.pods.emit(List.of(f.failedPod("uid")));
        assertEquals("failed", completion.join().state());
        assertTrue(completion.join().detail().contains("ImagePullBackOff"));
        f.jobs.assertStopped();
    }

    @Test
    void replacementAndDeletionDoNotWaitForAnotherJob() {
        Fixture replaced = new Fixture();
        var completion = replaced.watch();
        replaced.jobs.emit(List.of(replaced.job(0)));
        replaced.jobs.emit(List.of(new JobBuilder(replaced.job(0)).editMetadata()
                .withUid("new-uid").endMetadata().build()));
        assertTrue(assertThrows(CompletionException.class, completion::join).getCause().getMessage().contains("replaced"));
        replaced.pods.assertStopped();

        Fixture deleted = new Fixture();
        var missing = deleted.watch();
        deleted.jobs.emit(List.of());
        assertEquals("failed", missing.join().state());
        assertTrue(missing.join().detail().contains("not found"));
    }

    @Test
    void observerFailureAndCallerCancellationReleaseBothSources() {
        Fixture failed = new Fixture();
        var completion = failed.watch();
        failed.jobs.emit(List.of(failed.job(0)));
        RuntimeException failure = new RuntimeException("watch unavailable");
        failed.pods.stopped.completeExceptionally(failure);
        assertSame(failure, assertThrows(CompletionException.class, completion::join).getCause());
        failed.jobs.assertStopped();

        Fixture cancelled = new Fixture();
        var pending = cancelled.watch();
        cancelled.jobs.emit(List.of(cancelled.job(0)));
        pending.cancel(false);
        cancelled.jobs.assertStopped();
        cancelled.pods.assertStopped();
        verify(cancelled.client.batch().v1().jobs().inNamespace("test").withName("train"), never()).delete();
    }

    @Test
    void startupFailureIsAnExceptionalCompletion() {
        Fixture f = new Fixture();
        RuntimeException failure = new RuntimeException("watch forbidden");
        when(f.client.batch().v1().jobs().inNamespace("test").withName("train").runnableInformer(0))
                .thenThrow(failure);
        assertSame(failure, assertThrows(CompletionException.class, () -> f.watch().join()).getCause());
    }

    @Test
    void multipleJobsFailFastAndCancelOtherObservations() {
        Fixture f = new Fixture();
        Source<Job> other = new Source<>();
        @SuppressWarnings("unchecked")
        ScalableResource<Job> otherResource = mock(ScalableResource.class);
        when(f.client.batch().v1().jobs().inNamespace("test").withName("other")).thenReturn(otherResource);
        when(otherResource.runnableInformer(0)).thenReturn(other.informer);
        K8sManager.setKubernetesClientForTest(f.client);
        try {
            var completion = K8sManager.watchJobsFromYaml("""
                    apiVersion: batch/v1
                    kind: Job
                    metadata: {name: train, namespace: test}
                    ---
                    apiVersion: batch/v1
                    kind: Job
                    metadata: {name: other, namespace: test}
                    """);
            f.jobs.emit(List.of());
            assertEquals("failed", completion.join().state());
            other.assertStopped();
        } finally {
            K8sManager.resetClient();
        }
    }

    private static final class Source<T> {
        final SharedIndexInformer<T> informer;
        final CompletableFuture<Void> stopped = new CompletableFuture<>();
        final CompletableFuture<Void> released = new CompletableFuture<>();
        ResourceEventHandler<T> handler;
        List<T> initial;
        volatile List<T> items = List.of();
        Runnable onStop = () -> {};

        @SuppressWarnings("unchecked")
        Source() {
            informer = mock(SharedIndexInformer.class);
            Store<T> store = mock(Store.class);
            when(informer.getStore()).thenReturn(store);
            when(store.list()).thenAnswer(call -> items);
            when(informer.stopped()).thenReturn(stopped);
            when(informer.addEventHandler(any())).thenAnswer(call -> {
                handler = call.getArgument(0);
                return informer;
            });
            when(informer.start()).thenAnswer(call -> {
                if (initial != null) emit(initial);
                return CompletableFuture.completedFuture(null);
            });
            doAnswer(call -> {
                onStop.run();
                stopped.complete(null);
                released.complete(null);
                return null;
            }).when(informer).stop();
        }

        void emit(List<T> items) {
            this.items = items;
            if (items.isEmpty()) handler.onNothing();
            else handler.onAdd(items.getFirst());
        }

        void assertStopped() {
            assertDoesNotThrow(() -> released.get(5, TimeUnit.SECONDS));
            verify(informer, times(1)).stop();
        }
    }

    private static final class Fixture {
        final KubernetesClient client = mock(KubernetesClient.class, RETURNS_DEEP_STUBS);
        final Source<Job> jobs = new Source<>();
        final Source<Pod> pods = new Source<>();

        @SuppressWarnings("unchecked")
        Fixture() {
            MixedOperation<Job, JobList, ScalableResource<Job>> jobOperations = mock(MixedOperation.class);
            NonNamespaceOperation<Job, JobList, ScalableResource<Job>> namespacedJobs = mock(NonNamespaceOperation.class);
            ScalableResource<Job> namedJob = mock(ScalableResource.class);
            when(client.batch().v1().jobs()).thenReturn(jobOperations);
            when(jobOperations.inNamespace("test")).thenReturn(namespacedJobs);
            when(namespacedJobs.withName("train")).thenReturn(namedJob);
            MixedOperation<Pod, PodList, PodResource> podOperations = mock(MixedOperation.class);
            NonNamespaceOperation<Pod, PodList, PodResource> namespacedPods = mock(NonNamespaceOperation.class);
            FilterWatchListDeletable<Pod, PodList, PodResource> selectedPods = mock(FilterWatchListDeletable.class);
            when(client.pods()).thenReturn(podOperations);
            when(podOperations.inNamespace("test")).thenReturn(namespacedPods);
            when(namespacedPods.withLabels(Map.of("controller-uid", "uid"))).thenReturn(selectedPods);
            when(namedJob.runnableInformer(0)).thenReturn(jobs.informer);
            when(selectedPods.runnableInformer(0)).thenReturn(pods.informer);
        }

        CompletableFuture<K8sManager.JobStatus> watch() {
            // Submitted YAML has no UID; the first server snapshot establishes the Job incarnation.
            return JobCompletionWatcher.watch(client, new JobBuilder(job(0)).editMetadata().withUid(null).endMetadata().build());
        }

        Job job(int succeeded) {
            return new JobBuilder().withNewMetadata().withName("train").withNamespace("test").withUid("uid").endMetadata()
                    .withNewSpec().withCompletions(2).withNewSelector().withMatchLabels(Map.of("controller-uid", "uid"))
                    .endSelector().endSpec().withNewStatus().withSucceeded(succeeded).endStatus().build();
        }

        Pod failedPod(String ownerUid) {
            return new PodBuilder().withNewMetadata().withName("trainer").addNewOwnerReference()
                    .withUid(ownerUid).endOwnerReference().endMetadata()
                    .withNewStatus().addNewContainerStatus().withName("trainer").withNewState().withNewWaiting()
                    .withReason("ImagePullBackOff").endWaiting().endState().endContainerStatus().endStatus().build();
        }
    }
}
