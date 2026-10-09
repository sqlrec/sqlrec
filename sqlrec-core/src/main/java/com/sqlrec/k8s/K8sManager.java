package com.sqlrec.k8s;

import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
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
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class K8sManager {
    private static final Logger log = LoggerFactory.getLogger(K8sManager.class);
    private static final long DELETE_WAIT_SECONDS = 60;
    static volatile KubernetesClient kubernetesClient;

    static {
        // Close the cached KubernetesClient on JVM exit so its underlying OkHttp
        // connection pool and watch threads are not leaked when the process terminates.
        Runtime.getRuntime().addShutdownHook(new Thread(K8sManager::resetClient, "K8sManager-shutdown"));
    }

    private static KubernetesClient getKubernetesClient() {
        if (kubernetesClient == null) {
            synchronized (K8sManager.class) {
                if (kubernetesClient == null) {
                    kubernetesClient = new KubernetesClientBuilder().build();
                }
            }
        }
        return kubernetesClient;
    }

    /**
     * Close and discard the cached KubernetesClient so the next call builds a fresh one.
     * Called after a client-level failure (API server blip, token expiry, broken state)
     * so subsequent calls recover instead of failing forever on the broken client.
     */
    public static synchronized void resetClient() {
        if (kubernetesClient != null) {
            try {
                kubernetesClient.close();
            } catch (Exception e) {
                log.warn("Failed to close stale KubernetesClient during reset: {}", e.getMessage());
            }
            kubernetesClient = null;
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
        if (yamlContent == null || yamlContent.isEmpty()) {
            return;
        }
        try {
            InputStream inputStream = new ByteArrayInputStream(yamlContent.getBytes(StandardCharsets.UTF_8));
            getKubernetesClient().load(inputStream).serverSideApply();
        } catch (Exception e) {
            log.error("Failed to apply YAML: {}", e.getMessage(), e);
            if (isK8sClientFailure(e)) {
                resetClient();
            }
            throw new RuntimeException("Failed to apply YAML: " + e.getMessage(), e);
        }
    }

    public static void deleteYaml(String yamlContent) {
        if (yamlContent == null || yamlContent.isEmpty()) {
            return;
        }

        try {
            KubernetesClient client = getKubernetesClient();
            List<HasMetadata> resources = K8sYamlUtils.parseK8sYaml(yamlContent);

            for (HasMetadata resource : resources) {
                String kind = resource.getKind();
                String name = resource.getMetadata() != null ? resource.getMetadata().getName() : null;
                String namespace = resource.getMetadata() != null ? resource.getMetadata().getNamespace() : null;

                if (name == null || name.isEmpty()) {
                    log.warn("Skipping resource with no name, kind: {}", kind);
                    continue;
                }

                boolean exists = checkResourceExists(client, kind, name, namespace);

                if (exists) {
                    try {
                        client.resource(resource).delete();
                        log.info("Successfully deleted {}: {}/{}", kind, namespace != null ? namespace : "default", name);
                    } catch (Exception e) {
                        log.error("Failed to delete {}: {}/{}, error: {}", kind, namespace != null ? namespace : "default", name, e.getMessage());
                        if (!(e instanceof KubernetesClientException)
                                || ((KubernetesClientException) e).getCode() != 404) {
                            throw new RuntimeException("Failed to delete " + kind + ": "
                                    + (namespace != null ? namespace : "default") + "/" + name, e);
                        }
                    }
                } else {
                    log.info("Skipping non-existent {}: {}/{}", kind, namespace != null ? namespace : "default", name);
                }
            }
        } catch (Exception e) {
            log.error("Failed to delete YAML: {}", e.getMessage(), e);
            if (isK8sClientFailure(e)) {
                resetClient();
            }
            throw new RuntimeException("Failed to delete YAML: " + e.getMessage(), e);
        }
    }

    /**
     * Delete resources before recreating a checkpoint with the same Kubernetes names.
     * Unlike best-effort cleanup, a retry must not apply new YAML while the old Job
     * or ConfigMap is still terminating.
     */
    public static void deleteYamlAndWait(String yamlContent) {
        if (yamlContent == null || yamlContent.isEmpty()) {
            return;
        }

        try {
            KubernetesClient client = getKubernetesClient();
            List<HasMetadata> resources = K8sYamlUtils.parseK8sYaml(yamlContent);
            // Stop the old workload before removing the resources it depends on.
            resources.sort(Comparator.comparingInt(resource -> "Job".equals(resource.getKind()) ? 0 : 1));
            for (HasMetadata resource : resources) {
                String name = resource.getMetadata() != null ? resource.getMetadata().getName() : null;
                if (name == null || name.isEmpty()) {
                    throw new IllegalArgumentException("Cannot delete " + resource.getKind() + " without a name");
                }

                Resource<HasMetadata> handle = client.resource(resource);
                handle.delete();
                handle.waitUntilCondition(Objects::isNull, DELETE_WAIT_SECONDS, TimeUnit.SECONDS);
                log.info("Deleted {} before checkpoint retry: {}/{}", resource.getKind(),
                        resource.getMetadata().getNamespace(), name);
            }
        } catch (Exception e) {
            log.error("Failed to delete old checkpoint resources: {}", e.getMessage(), e);
            if (isK8sClientFailure(e)) {
                resetClient();
            }
            throw new RuntimeException("Failed to delete old checkpoint resources: " + e.getMessage(), e);
        }
    }

    private static boolean checkResourceExists(KubernetesClient client, String kind, String name, String namespace) {
        switch (kind) {
            case "Deployment":
                return client.apps().deployments().inNamespace(namespace != null ? namespace : "default").withName(name).get() != null;
            case "Service":
                return client.services().inNamespace(namespace != null ? namespace : "default").withName(name).get() != null;
            case "ConfigMap":
                return client.configMaps().inNamespace(namespace != null ? namespace : "default").withName(name).get() != null;
            case "Secret":
                return client.secrets().inNamespace(namespace != null ? namespace : "default").withName(name).get() != null;
            case "Pod":
                return client.pods().inNamespace(namespace != null ? namespace : "default").withName(name).get() != null;
            case "Job":
                return client.batch().v1().jobs().inNamespace(namespace != null ? namespace : "default").withName(name).get() != null;
            default:
                throw new RuntimeException("unsupport k8s resource: " + kind);
        }
    }

    public record JobStatus(String state, String detail) {
    }

    private static JobStatus checkJobStatusByName(String jobName, String namespace) {
        try {
            String resolvedNamespace = namespace != null ? namespace : "default";
            Job job = getKubernetesClient().batch().v1().jobs()
                    .inNamespace(resolvedNamespace)
                    .withName(jobName)
                    .get();

            JobStatus knownStatus = knownJobStatus(job, jobName, resolvedNamespace);
            if (knownStatus != null) {
                return knownStatus;
            }

            String imagePullFailure = findImagePullFailure(job, resolvedNamespace);
            return imagePullFailure == null
                    ? new JobStatus("running", null)
                    : new JobStatus("failed", imagePullFailure);
        } catch (Exception e) {
            log.error("Failed to check job status: {}", e.getMessage(), e);
            if (isK8sClientFailure(e)) {
                resetClient();
            }
            return new JobStatus("running", null);
        }
    }

    /** Returns a status determined by the Job itself, or null when its Pods need checking. */
    private static JobStatus knownJobStatus(Job job, String jobName, String resolvedNamespace) {
        if (job == null) {
            String detail = "Job not found: " + resolvedNamespace + "/" + jobName;
            log.error(detail);
            return new JobStatus("failed", detail);
        }

        if (job.getStatus() != null) {
            if (job.getStatus().getConditions() != null) {
                for (JobCondition condition : job.getStatus().getConditions()) {
                    if ("Failed".equals(condition.getType()) && "True".equals(condition.getStatus())) {
                        String detail = condition.getMessage();
                        log.error("Job failed: {}/{}, reason: {}, message: {}",
                                resolvedNamespace, jobName, condition.getReason(), detail);
                        return new JobStatus("failed", "Job failed: " + resolvedNamespace + "/" + jobName
                                + (detail == null || detail.isBlank() ? "" : " (" + detail + ")"));
                    }
                }
            }
            Integer succeeded = job.getStatus().getSucceeded();
            Integer completions = job.getSpec() != null ? job.getSpec().getCompletions() : null;
            // Kubernetes defaults completions to one when the field is omitted.
            if (succeeded != null && succeeded >= (completions != null ? completions : 1)) {
                log.info("Job completed successfully: {}/{}", resolvedNamespace, jobName);
                return new JobStatus("succeeded", null);
            }

            // Ready Pods have already started all their containers. Avoid listing Pods
            // while every active Pod is ready; a new or replacement Pod makes the
            // counts diverge, so image-pull checks resume automatically.
            Integer active = job.getStatus().getActive();
            Integer ready = job.getStatus().getReady();
            if (active != null && active > 0 && active.equals(ready)) {
                return new JobStatus("running", null);
            }
        }
        return null;
    }

    private static String findImagePullFailure(Job job, String namespace) {
        String jobName = job.getMetadata().getName();
        Map<String, String> labels = job.getSpec() != null && job.getSpec().getSelector() != null
                ? job.getSpec().getSelector().getMatchLabels() : null;
        PodList podList = labels != null && !labels.isEmpty()
                ? getKubernetesClient().pods().inNamespace(namespace).withLabels(labels).list()
                : getKubernetesClient().pods().inNamespace(namespace)
                        .withLabel("batch.kubernetes.io/job-name", jobName).list();
        // Older Kubernetes versions use the unprefixed Job label.
        if ((podList == null || podList.getItems() == null || podList.getItems().isEmpty())
                && (labels == null || labels.isEmpty())) {
            podList = getKubernetesClient().pods().inNamespace(namespace)
                    .withLabel("job-name", jobName).list();
        }
        if (podList == null || podList.getItems() == null) {
            return null;
        }
        for (Pod pod : podList.getItems()) {
            String failure = imagePullFailure(pod);
            if (failure != null) {
                String podName = pod.getMetadata() != null ? pod.getMetadata().getName() : "unknown";
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
        try {
            Deployment deployment = getKubernetesClient().apps().deployments()
                    .inNamespace(namespace != null ? namespace : "default")
                    .withName(deploymentName)
                    .get();

            return isDeploymentReady(deployment, deploymentName, namespace);
        } catch (Exception e) {
            log.error("Failed to check deployment status: {}", e.getMessage(), e);
            if (isK8sClientFailure(e)) {
                resetClient();
            }
            return false;
        }
    }

    private static boolean isDeploymentReady(Deployment deployment, String deploymentName, String namespace) {
        if (deployment == null) {
            log.warn("Deployment not found: {}/{}", namespace != null ? namespace : "default", deploymentName);
            return false;
        }

        if (deployment.getStatus() != null) {
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

    public static String checkJobsStatusFromYaml(String k8sYaml) {
        return checkJobsStatusDetailFromYaml(k8sYaml).state();
    }

    public static JobStatus checkJobsStatusDetailFromYaml(String k8sYaml) {
        if (k8sYaml == null || k8sYaml.isEmpty()) {
            return new JobStatus("succeeded", null);
        }

        List<Job> jobs = K8sYamlUtils.parseK8sYamlAndGetJobs(k8sYaml);

        if (jobs.isEmpty()) {
            return new JobStatus("succeeded", null);
        }

        boolean anyFailed = false;
        boolean anyRunning = false;
        String failureDetail = null;

        for (Job job : jobs) {
            String namespace = job.getMetadata() != null ? job.getMetadata().getNamespace() : null;
            String name = job.getMetadata() != null ? job.getMetadata().getName() : null;
            if (name != null) {
                JobStatus status = checkJobStatusByName(name, namespace);
                if ("failed".equals(status.state())) {
                    anyFailed = true;
                    if (failureDetail == null) {
                        failureDetail = status.detail();
                    }
                } else if ("running".equals(status.state())) {
                    anyRunning = true;
                }
            }
        }

        if (anyFailed) {
            return new JobStatus("failed", failureDetail);
        }
        if (anyRunning) {
            return new JobStatus("running", null);
        }
        return new JobStatus("succeeded", null);
    }

    public static boolean isDeploymentReadyFromYaml(String k8sYaml) {
        if (k8sYaml == null || k8sYaml.isEmpty()) {
            return true;
        }

        List<Deployment> deployments = K8sYamlUtils.parseK8sYamlAndGetDeployments(k8sYaml);

        if (deployments.isEmpty()) {
            return true;
        }

        boolean allReady = true;

        for (Deployment deployment : deployments) {
            String namespace = deployment.getMetadata() != null ? deployment.getMetadata().getNamespace() : null;
            String name = deployment.getMetadata() != null ? deployment.getMetadata().getName() : null;
            if (name != null) {
                if (!isDeploymentReadyByName(name, namespace)) {
                    allReady = false;
                }
            }
        }

        return allReady;
    }
}
