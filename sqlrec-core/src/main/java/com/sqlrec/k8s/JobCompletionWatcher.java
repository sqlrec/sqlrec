package com.sqlrec.k8s;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Informable;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Stream;

/** Observes one Job incarnation and its Pods; never changes or deletes resources. */
final class JobCompletionWatcher {
    private final KubernetesClient client;
    private final String namespace;
    private final String name;
    private final CompletableFuture<K8sManager.JobStatus> completion = new CompletableFuture<>();
    private final CompletableFuture<K8sManager.JobStatus> outcome = new CompletableFuture<>();
    private final List<SharedIndexInformer<?>> informers = new ArrayList<>();
    private String uid;
    private boolean watchingPods;

    private JobCompletionWatcher(KubernetesClient client, Job target) {
        this.client = client;
        this.namespace = Objects.requireNonNullElse(target.getMetadata().getNamespace(), "default");
        this.name = Objects.requireNonNull(target.getMetadata().getName(), "Job name");
        this.uid = target.getMetadata().getUid();
        // Event handlers only signal the outcome. Stop observers off the event thread and
        // finish cleanup before exposing the result to callers that may immediately close the client.
        // Cleanup must still run if a shared executor has already been shut down.
        outcome.whenComplete((status, failure) -> Thread.startVirtualThread(() -> finish(status, failure)));
        completion.whenComplete((result, failure) -> {
            if (completion.isCancelled()) outcome.cancel(false);
        });
    }

    static CompletableFuture<K8sManager.JobStatus> watch(KubernetesClient client, Job target) {
        JobCompletionWatcher watcher = new JobCompletionWatcher(client, target);
        watcher.start();
        return watcher.completion;
    }

    private synchronized void start() {
        try {
            observe(client.batch().v1().jobs().inNamespace(namespace).withName(name), this::onJobs);
        } catch (Exception failure) {
            outcome.completeExceptionally(failure);
        }
    }

    private synchronized void onJobs(List<Job> jobs) {
        if (outcome.isDone()) return;
        if (jobs.isEmpty()) {
            outcome.complete(new K8sManager.JobStatus("failed", "Job not found: " + namespace + "/" + name));
            return;
        }
        Job job = jobs.getFirst();
        String observedUid = job.getMetadata().getUid();
        if (uid != null && !uid.equals(observedUid)) {
            outcome.completeExceptionally(new IllegalStateException("Job was replaced: " + namespace + "/" + name));
            return;
        }
        uid = observedUid;
        K8sManager.JobStatus status = K8sManager.knownJobStatus(job, name, namespace);
        if (status != null && !"running".equals(status.state())) {
            outcome.complete(status);
            return;
        }
        if (!watchingPods) {
            watchingPods = true;
            watchPods(job);
        }
    }

    private void watchPods(Job job) {
        Map<String, String> labels = job.getSpec() != null && job.getSpec().getSelector() != null
                ? job.getSpec().getSelector().getMatchLabels() : null;
        var pods = client.pods().inNamespace(namespace);
        var selected = labels != null && !labels.isEmpty()
                ? pods.withLabels(labels) : pods.withLabel("job-name", name);
        observe(selected, this::onPods);
    }

    private synchronized void onPods(List<Pod> pods) {
        if (outcome.isDone()) return;
        for (Pod pod : pods) {
            // Labels can survive a same-name retry; only this Job's Pods belong to the operation.
            if (uid != null && (pod.getMetadata() == null || pod.getMetadata().getOwnerReferences() == null
                    || pod.getMetadata().getOwnerReferences().stream().noneMatch(owner -> uid.equals(owner.getUid())))) {
                continue;
            }
            String failure = K8sManager.imagePullFailure(pod);
            if (failure != null) {
                String podName = pod.getMetadata() == null ? "unknown" : pod.getMetadata().getName();
                outcome.complete(new K8sManager.JobStatus("failed",
                        "Job " + namespace + "/" + name + ", Pod " + podName + ": " + failure));
                return;
            }
        }
    }

    private <T> void observe(Informable<T> resource, Consumer<List<T>> listener) {
        SharedIndexInformer<T> informer = resource.runnableInformer(0);
        // start/onJobs hold this monitor, so finish cannot miss an observer being registered.
        informers.add(informer);
        informer.initialState(Stream.empty());
        informer.addEventHandler(new ResourceEventHandler<>() {
            private void changed(List<T> objects) {
                try {
                    listener.accept(objects);
                } catch (Throwable failure) {
                    outcome.completeExceptionally(failure);
                }
            }

            // The cache can advance while callbacks are queued. Preserve each event's state.
            @Override public void onAdd(T object) { changed(List.of(object)); }
            @Override public void onUpdate(T oldObject, T newObject) { changed(List.of(newObject)); }
            @Override public void onDelete(T object, boolean unknown) { changed(List.of()); }
            @Override public void onNothing() { changed(List.of()); }
        });
        informer.stopped().whenComplete((ignored, failure) -> {
            if (!outcome.isDone()) outcome.completeExceptionally(failure != null ? failure
                    : new IllegalStateException("Job observation stopped: " + namespace + "/" + name));
        });
        informer.start().whenComplete((ignored, failure) -> {
            if (failure != null) outcome.completeExceptionally(failure);
        });
    }

    private void finish(K8sManager.JobStatus status, Throwable failure) {
        List<SharedIndexInformer<?>> observers;
        synchronized (this) {
            observers = List.copyOf(informers);
            informers.clear();
        }
        Throwable resultFailure = failure;
        for (SharedIndexInformer<?> observer : observers) {
            try {
                observer.stop();
            } catch (Throwable cleanupFailure) {
                if (resultFailure == null) resultFailure = cleanupFailure;
                else if (resultFailure != cleanupFailure) resultFailure.addSuppressed(cleanupFailure);
            }
        }
        if (resultFailure == null) completion.complete(status);
        else completion.completeExceptionally(resultFailure);
    }
}
