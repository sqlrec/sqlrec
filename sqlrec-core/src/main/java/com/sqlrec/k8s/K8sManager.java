package com.sqlrec.k8s;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobCondition;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class K8sManager {
    private static final Logger log = LoggerFactory.getLogger(K8sManager.class);
    private static final long DELETE_WAIT_SECONDS = 60;
    private static final Set<Class<? extends HasMetadata>> CLEANUP_RESOURCE_TYPES =
            Set.of(Deployment.class, Service.class, ConfigMap.class, Secret.class, Pod.class, Job.class);
    static volatile KubernetesClient kubernetesClient;

    static {
        // Close the cached KubernetesClient on JVM exit so its underlying OkHttp
        // connection pool and watch threads are not leaked when the process terminates.
        Runtime.getRuntime().addShutdownHook(new Thread(K8sManager::resetClient, "K8sManager-shutdown"));
    }

    private static KubernetesClient getKubernetesClient() {
        KubernetesClient current = kubernetesClient;
        if (current == null) {
            synchronized (K8sManager.class) {
                current = kubernetesClient;
                if (current == null) {
                    current = new KubernetesClientBuilder().build();
                    kubernetesClient = current;
                }
            }
        }
        return current;
    }

    static String requireResourceName(HasMetadata resource) {
        var metadata = resource.getMetadata();
        String name = metadata == null ? null : metadata.getName();
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Missing metadata.name for " + resource.getKind());
        }
        return name;
    }

    static String requireResourceNamespace(HasMetadata resource) {
        var metadata = resource.getMetadata();
        String namespace = metadata == null ? null : metadata.getNamespace();
        if (namespace == null || namespace.isBlank()) {
            throw new IllegalArgumentException("Missing metadata.namespace for " + resource.getKind()
                    + " " + requireResourceName(resource) + ". Restore the resource's original namespace in the stored YAML.");
        }
        return namespace;
    }

    private static void requireStoredNamespaces(List<HasMetadata> resources, KubernetesClient client) {
        for (HasMetadata resource : resources) {
            var metadata = resource.getMetadata();
            String namespace = metadata == null ? null : metadata.getNamespace();
            if ((namespace == null || namespace.isBlank()) && K8sYamlUtils.isNamespaced(resource, client)) {
                requireResourceNamespace(resource);
            }
        }
    }

    /**
     * Close and discard the cached KubernetesClient so the next call builds a fresh one.
     * Called after a client-level failure (API server blip, token expiry, broken state)
     * so subsequent calls recover instead of failing forever on the broken client.
     */
    public static void resetClient() {
        resetClient(kubernetesClient);
    }

    private static void resetClient(KubernetesClient stale) {
        if (stale == null) return;
        synchronized (K8sManager.class) {
            // A late failure from an old request must not close a recovered client's watches.
            if (kubernetesClient != stale) return;
            kubernetesClient = null;
        }
        // Closing may wait for SDK callbacks. Never hold the client-factory lock while doing so.
        try {
            stale.close();
        } catch (Exception e) {
            log.warn("Failed to close stale KubernetesClient during reset: {}", e.getMessage());
        }
    }

    /**
     * Test-only: inject a mock client. Clears the field first to avoid leaking prior client.
     */
    static void setKubernetesClientForTest(KubernetesClient mockClient) {
        resetClient();
        kubernetesClient = mockClient;
    }

    /**
     * Returns true if the throwable indicates a Kubernetes client transport/server-level
     * failure that warrants discarding the cached client. 4xx business errors (404 not
     * found, 400 bad request, 409 conflict, ...) are excluded so a misapplied YAML does
     * not needlessly tear down the shared client.
     */
    private static boolean isK8sClientFailure(Throwable t) {
        Throwable cur = t;
        while (cur != null) {
            if (cur instanceof KubernetesClientException) {
                int code = ((KubernetesClientException) cur).getCode();
                // code == 0: no HTTP response (pure connection failure, e.g. API server
                //   unreachable, TLS/auth handshake failure). code >= 500: server error.
                if (code == 0 || code >= 500) {
                    return true;
                }
            } else if (cur instanceof IOException) {
                // Transport-level IOException (socket reset, connection refused, etc.)
                return true;
            }
            cur = cur.getCause();
        }
        return false;
    }

    public static void applyYaml(String yamlContent) {
        applyYaml(yamlContent, false);
    }

    /** Dry-run the manifest and return the exact YAML to persist and submit. */
    public static String validateYaml(String yamlContent) {
        return applyYaml(yamlContent, true);
    }

    private static String applyYaml(String yamlContent, boolean dryRun) {
        if (yamlContent == null || yamlContent.isEmpty()) {
            return yamlContent;
        }
        KubernetesClient client = null;
        try {
            client = getKubernetesClient();
            var loaded = client.load(new ByteArrayInputStream(yamlContent.getBytes(StandardCharsets.UTF_8)));
            if (dryRun) {
                String validatedYaml = K8sYamlUtils.bindNamespaces(yamlContent, loaded.items(), client);
                if (!validatedYaml.equals(yamlContent)) {
                    yamlContent = validatedYaml;
                    loaded = client.load(new ByteArrayInputStream(yamlContent.getBytes(StandardCharsets.UTF_8)));
                }
            }
            loaded.dryRun(dryRun).serverSideApply();
            return yamlContent;
        } catch (Exception e) {
            String action = dryRun ? "validate" : "apply";
            log.error("Failed to {} YAML: {}", action, e.getMessage(), e);
            if (isK8sClientFailure(e)) {
                resetClient(client);
            }
            throw new RuntimeException("Failed to " + action + " YAML: " + e.getMessage(), e);
        }
    }

    public static void deleteYaml(String yamlContent) {
        if (yamlContent == null || yamlContent.isEmpty()) {
            return;
        }

        KubernetesClient client = null;
        try {
            List<HasMetadata> resources = K8sYamlUtils.parseK8sYaml(yamlContent);
            client = getKubernetesClient();
            requireStoredNamespaces(resources, client);

            for (HasMetadata resource : resources) {
                String kind = resource.getKind();
                var metadata = resource.getMetadata();
                String name = metadata == null ? null : metadata.getName();
                if (name == null || name.isEmpty()) {
                    log.warn("Skipping resource with no name, kind: {}", kind);
                    continue;
                }
                if (!CLEANUP_RESOURCE_TYPES.contains(resource.getClass())) {
                    throw new RuntimeException("unsupport k8s resource: " + kind);
                }

                String namespace = metadata.getNamespace();
                Resource<HasMetadata> handle = client.resource(resource).inNamespace(namespace);
                try {
                    handle.delete();
                    log.info("Deleted {} if present: {}/{}", kind, namespace, name);
                } catch (KubernetesClientException e) {
                    if (!alreadyDeleted(handle, e)) {
                        throw new RuntimeException("Failed to delete " + kind + ": " + namespace + "/" + name, e);
                    }
                }
            }
        } catch (Exception e) {
            log.error("Failed to delete YAML: {}", e.getMessage(), e);
            if (isK8sClientFailure(e)) {
                resetClient(client);
            }
            throw new RuntimeException("Failed to delete YAML: " + e.getMessage(), e);
        }
    }

    private static boolean alreadyDeleted(Resource<HasMetadata> handle, KubernetesClientException failure) {
        if (failure.getCode() == 404) {
            return true;
        }
        if (failure.getCode() != 403) {
            return false;
        }
        try {
            return handle.get() == null;
        } catch (RuntimeException lookupFailure) {
            if (lookupFailure != failure) {
                failure.addSuppressed(lookupFailure);
            }
            return false;
        }
    }

    /**
     * Delete resources before retrying a checkpoint.
     * Unlike best-effort cleanup, a retry must not apply new YAML while the old Job
     * or ConfigMap is still terminating.
     */
    public static void deleteYamlAndWait(String yamlContent) {
        if (yamlContent == null || yamlContent.isEmpty()) {
            return;
        }

        KubernetesClient client = null;
        try {
            List<HasMetadata> resources = K8sYamlUtils.parseK8sYaml(yamlContent);
            client = getKubernetesClient();
            requireStoredNamespaces(resources, client);
            // Stop the old workload before removing the resources it depends on.
            resources.sort(Comparator.comparingInt(resource -> "Job".equals(resource.getKind()) ? 0 : 1));
            for (HasMetadata resource : resources) {
                String name = requireResourceName(resource);
                Resource<HasMetadata> handle = client.resource(resource);
                if ("Job".equals(resource.getKind())) {
                    // Keep the Job until its Pods are gone, so retries cannot share a writer.
                    handle.withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
                } else {
                    handle.delete();
                }
                var deletion = handle.informOnCondition(List::isEmpty);
                try {
                    deletion.get(DELETE_WAIT_SECONDS, TimeUnit.SECONDS);
                } catch (TimeoutException e) {
                    throw new IllegalStateException("Timed out after " + DELETE_WAIT_SECONDS
                            + "s waiting for deletion of " + resource.getKind() + " " + name, e);
                } finally {
                    deletion.cancel(false);
                }
                log.info("Deleted {} before checkpoint retry: {}/{}", resource.getKind(),
                        resource.getMetadata().getNamespace(), name);
            }
        } catch (Exception e) {
            if (e instanceof InterruptedException || Thread.currentThread().isInterrupted()) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while deleting old checkpoint resources", e);
            }
            log.error("Failed to delete old checkpoint resources: {}", e.getMessage(), e);
            if (isK8sClientFailure(e)) {
                resetClient(client);
            }
            throw new RuntimeException("Failed to delete old checkpoint resources: " + e.getMessage(), e);
        }
    }

    public enum JobState {
        RUNNING,
        SUCCEEDED,
        FAILED
    }

    public record JobStatus(JobState state, String detail) {
    }

    private static JobStatus checkJobStatusByName(String jobName, String namespace) {
        KubernetesClient client = null;
        try {
            client = getKubernetesClient();
            Job job = client.batch().v1().jobs()
                    .inNamespace(namespace)
                    .withName(jobName)
                    .get();

            JobStatus knownStatus = knownJobStatus(job, jobName, namespace);
            if (knownStatus != null) {
                return knownStatus;
            }

            String imagePullFailure = findImagePullFailure(client, job, namespace);
            return imagePullFailure == null
                    ? new JobStatus(JobState.RUNNING, null)
                    : new JobStatus(JobState.FAILED, imagePullFailure);
        } catch (Exception e) {
            log.error("Failed to check job status: {}", e.getMessage(), e);
            if (isK8sClientFailure(e)) {
                resetClient(client);
            }
            throw new IllegalStateException("Cannot determine Job status: "
                    + namespace + "/" + jobName, e);
        }
    }

    /** Returns a status determined by the Job itself, or null when its Pods need checking. */
    static JobStatus knownJobStatus(Job job, String jobName, String resolvedNamespace) {
        if (job == null) {
            String detail = "Job not found: " + resolvedNamespace + "/" + jobName;
            log.error(detail);
            return new JobStatus(JobState.FAILED, detail);
        }

        if (job.getStatus() != null) {
            if (job.getStatus().getConditions() != null) {
                for (JobCondition condition : job.getStatus().getConditions()) {
                    if ("Failed".equals(condition.getType()) && "True".equals(condition.getStatus())) {
                        String detail = condition.getMessage();
                        log.error("Job failed: {}/{}, reason: {}, message: {}",
                                resolvedNamespace, jobName, condition.getReason(), detail);
                        return new JobStatus(JobState.FAILED, "Job failed: " + resolvedNamespace + "/" + jobName
                                + (detail == null || detail.isBlank() ? "" : " (" + detail + ")"));
                    }
                }
            }
            Integer succeeded = job.getStatus().getSucceeded();
            Integer completions = job.getSpec() != null ? job.getSpec().getCompletions() : null;
            // Kubernetes defaults completions to one when the field is omitted.
            if (succeeded != null && succeeded >= (completions != null ? completions : 1)) {
                log.info("Job completed successfully: {}/{}", resolvedNamespace, jobName);
                return new JobStatus(JobState.SUCCEEDED, null);
            }

            // Ready Pods have already started all their containers. Avoid listing Pods
            // while every active Pod is ready; a new or replacement Pod makes the
            // counts diverge, so image-pull checks resume automatically.
            Integer active = job.getStatus().getActive();
            Integer ready = job.getStatus().getReady();
            if (active != null && active > 0 && active.equals(ready)) {
                return new JobStatus(JobState.RUNNING, null);
            }
        }
        return null;
    }

    private static String findImagePullFailure(KubernetesClient client, Job job, String namespace) {
        String jobName = job.getMetadata().getName();
        Map<String, String> labels = job.getSpec() != null && job.getSpec().getSelector() != null
                ? job.getSpec().getSelector().getMatchLabels() : null;
        PodList podList = labels != null && !labels.isEmpty()
                ? client.pods().inNamespace(namespace).withLabels(labels).list()
                : client.pods().inNamespace(namespace)
                        .withLabel("batch.kubernetes.io/job-name", jobName).list();
        // Older Kubernetes versions use the unprefixed Job label.
        if ((podList == null || podList.getItems() == null || podList.getItems().isEmpty())
                && (labels == null || labels.isEmpty())) {
            podList = client.pods().inNamespace(namespace)
                    .withLabel("job-name", jobName).list();
        }
        if (podList == null || podList.getItems() == null) {
            return null;
        }
        return jobPodImagePullFailure(podList.getItems(), namespace, jobName, job.getMetadata().getUid());
    }

    static String jobPodImagePullFailure(List<Pod> pods, String namespace, String jobName, String jobUid) {
        for (Pod pod : pods) {
            var metadata = pod == null ? null : pod.getMetadata();
            // Labels can survive a same-name retry. Check ownership when the Job UID is known.
            if (jobUid != null && (metadata == null || metadata.getOwnerReferences() == null
                    || metadata.getOwnerReferences().stream().noneMatch(owner -> jobUid.equals(owner.getUid())))) {
                continue;
            }
            String failure = imagePullFailure(pod);
            if (failure != null) {
                String podName = metadata != null ? metadata.getName() : "unknown";
                return "Job " + namespace + "/" + jobName + ", Pod " + podName + ": " + failure;
            }
        }
        return null;
    }

    static String imagePullFailure(Pod pod) {
        if (pod == null || pod.getStatus() == null) {
            return null;
        }
        for (List<ContainerStatus> statuses : List.of(
                pod.getStatus().getInitContainerStatuses() != null
                        ? pod.getStatus().getInitContainerStatuses() : List.<ContainerStatus>of(),
                pod.getStatus().getContainerStatuses() != null
                        ? pod.getStatus().getContainerStatuses() : List.<ContainerStatus>of())) {
            for (ContainerStatus status : statuses) {
                if (status.getState() == null || status.getState().getWaiting() == null) {
                    continue;
                }
                String reason = status.getState().getWaiting().getReason();
                if ("ErrImagePull".equals(reason) || "ImagePullBackOff".equals(reason)
                        || "InvalidImageName".equals(reason)) {
                    String message = status.getState().getWaiting().getMessage();
                    return "container " + status.getName() + " is waiting: " + reason
                            + (message == null || message.isBlank() ? "" : " (" + message + ")");
                }
            }
        }
        return null;
    }

    private static boolean isDeploymentReadyByName(String deploymentName, String namespace) {
        KubernetesClient client = null;
        try {
            client = getKubernetesClient();
            Deployment deployment = client.apps().deployments()
                    .inNamespace(namespace)
                    .withName(deploymentName)
                    .get();

            return isDeploymentReady(deployment, deploymentName, namespace);
        } catch (Exception e) {
            log.error("Failed to check deployment status: {}", e.getMessage(), e);
            if (isK8sClientFailure(e)) {
                resetClient(client);
            }
            return false;
        }
    }

    private static boolean isDeploymentReady(Deployment deployment, String deploymentName, String namespace) {
        if (deployment == null) {
            log.warn("Deployment not found: {}/{}", namespace, deploymentName);
            return false;
        }

        if (deployment.getStatus() != null) {
            Long generation = deployment.getMetadata() == null ? null : deployment.getMetadata().getGeneration();
            Long observedGeneration = deployment.getStatus().getObservedGeneration();
            if (generation != null && (observedGeneration == null || observedGeneration < generation)) {
                log.info("Deployment {}/{} is waiting for generation {} to be observed", namespace, deploymentName, generation);
                return false;
            }
            int replicas = valueOrDefault(deployment.getSpec() != null ? deployment.getSpec().getReplicas() : null, 1);
            int readyReplicas = valueOrDefault(deployment.getStatus().getReadyReplicas(), 0);
            int updatedReplicas = valueOrDefault(deployment.getStatus().getUpdatedReplicas(), 0);
            int availableReplicas = valueOrDefault(deployment.getStatus().getAvailableReplicas(), 0);
            int unavailableReplicas = valueOrDefault(deployment.getStatus().getUnavailableReplicas(), 0);

            boolean allReady = readyReplicas == replicas;
            boolean allUpdated = updatedReplicas == replicas;
            boolean noneUnavailable = unavailableReplicas == 0;

            if (allReady && allUpdated && noneUnavailable) {
                log.info("Deployment {} is fully ready: replicas={}, ready={}, updated={}, available={}, unavailable={}",
                        deploymentName, replicas, readyReplicas, updatedReplicas, availableReplicas, unavailableReplicas);
                return true;
            } else {
                log.info("Deployment {} is not ready yet: replicas={}, ready={}, updated={}, available={}, unavailable={}",
                        deploymentName, replicas, readyReplicas, updatedReplicas, availableReplicas, unavailableReplicas);
                return false;
            }
        }

        return false;
    }

    private static int valueOrDefault(Integer value, int defaultValue) {
        return value == null ? defaultValue : value;
    }

    /** Completes once all Jobs succeed, or immediately when any Job fails. Cancellation only stops observation. */
    public static CompletableFuture<JobStatus> watchJobsFromYaml(String yaml) {
        CompletableFuture<JobStatus> completion = new CompletableFuture<>();
        try {
            List<Job> jobs = K8sYamlUtils.parseK8sYamlAndGetJobs(yaml);
            if (jobs.isEmpty()) return CompletableFuture.completedFuture(new JobStatus(JobState.SUCCEEDED, null));
            AtomicInteger remaining = new AtomicInteger(jobs.size());
            KubernetesClient client = getKubernetesClient();
            for (Job job : jobs) {
                if (completion.isDone()) break;
                CompletableFuture<JobStatus> observation = JobCompletionWatcher.watch(client, job);
                completion.whenComplete((result, failure) -> observation.cancel(false));
                observation.whenComplete((status, failure) -> {
                    if (failure != null) completion.completeExceptionally(failure);
                    else if (status.state() == JobState.FAILED) completion.complete(status);
                    else if (remaining.decrementAndGet() == 0) completion.complete(new JobStatus(JobState.SUCCEEDED, null));
                });
            }
        } catch (Exception failure) {
            completion.completeExceptionally(failure);
        }
        return completion;
    }

    public static JobStatus checkJobsStatusDetailFromYaml(String k8sYaml) {
        boolean anyRunning = false;
        for (Job job : K8sYamlUtils.parseK8sYamlAndGetJobs(k8sYaml)) {
            String name = requireResourceName(job);
            JobStatus status = checkJobStatusByName(name, requireResourceNamespace(job));
            if (status.state() == JobState.FAILED) {
                return status;
            }
            anyRunning |= status.state() == JobState.RUNNING;
        }
        return new JobStatus(anyRunning ? JobState.RUNNING : JobState.SUCCEEDED, null);
    }

    public static boolean isDeploymentReadyFromYaml(String k8sYaml) {
        for (Deployment deployment : K8sYamlUtils.parseK8sYamlAndGetDeployments(k8sYaml)) {
            String name = requireResourceName(deployment);
            if (!isDeploymentReadyByName(name, requireResourceNamespace(deployment))) {
                return false;
            }
        }
        return true;
    }
}
