package com.sqlrec.k8s;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class K8sManager {
    private static final Logger log = LoggerFactory.getLogger(K8sManager.class);
    private static volatile KubernetesClient kubernetesClient;

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

    public static void applyYaml(String yamlContent) {
        if (yamlContent == null || yamlContent.isEmpty()) {
            return;
        }
        try {
            InputStream inputStream = new ByteArrayInputStream(yamlContent.getBytes(StandardCharsets.UTF_8));
            getKubernetesClient().load(inputStream).serverSideApply();
        } catch (Exception e) {
            log.error("Failed to apply YAML: {}", e.getMessage(), e);
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
                    }
                } else {
                    log.info("Skipping non-existent {}: {}/{}", kind, namespace != null ? namespace : "default", name);
                }
            }
        } catch (Exception e) {
            log.error("Failed to delete YAML: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to delete YAML: " + e.getMessage(), e);
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

    private static String checkJobStatusByName(String jobName, String namespace) {
        try {
            Job job = getKubernetesClient().batch().v1().jobs()
                    .inNamespace(namespace != null ? namespace : "default")
                    .withName(jobName)
                    .get();

            if (job == null) {
                log.error("Job not found: {}/{}", namespace != null ? namespace : "default", jobName);
                return "failed";
            }

            if (job.getStatus() != null) {
                Integer succeeded = job.getStatus().getSucceeded();
                Integer completions = job.getSpec() != null ? job.getSpec().getCompletions() : null;
                if (succeeded != null && completions != null && succeeded >= completions) {
                    log.info("Job completed successfully: {}/{}", namespace != null ? namespace : "default", jobName);
                    return "succeeded";
                }
                if (job.getStatus().getFailed() != null && job.getStatus().getFailed() > 0) {
                    log.error("Job failed: {}/{}", namespace != null ? namespace : "default", jobName);
                    return "failed";
                }
            }

            return "running";
        } catch (Exception e) {
            log.error("Failed to check job status: {}", e.getMessage(), e);
            return "running";
        }
    }

    private static boolean isDeploymentReadyByName(String deploymentName, String namespace) {
        try {
            Deployment deployment = getKubernetesClient().apps().deployments()
                    .inNamespace(namespace != null ? namespace : "default")
                    .withName(deploymentName)
                    .get();

            if (deployment == null) {
                log.warn("Deployment not found: {}/{}", namespace != null ? namespace : "default", deploymentName);
                return false;
            }

            if (deployment.getStatus() != null) {
                Integer replicas = deployment.getSpec() != null ? deployment.getSpec().getReplicas() : 1;
                if (replicas == null) {
                    replicas = 1;
                }

                Integer readyReplicas = deployment.getStatus().getReadyReplicas();
                Integer updatedReplicas = deployment.getStatus().getUpdatedReplicas();
                Integer availableReplicas = deployment.getStatus().getAvailableReplicas();
                Integer unavailableReplicas = deployment.getStatus().getUnavailableReplicas();

                if (readyReplicas == null) {
                    readyReplicas = 0;
                }
                if (updatedReplicas == null) {
                    updatedReplicas = 0;
                }
                if (availableReplicas == null) {
                    availableReplicas = 0;
                }
                if (unavailableReplicas == null) {
                    unavailableReplicas = 0;
                }

                boolean allReady = readyReplicas.equals(replicas);
                boolean allUpdated = updatedReplicas.equals(replicas);
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
        } catch (Exception e) {
            log.error("Failed to check deployment status: {}", e.getMessage(), e);
            return false;
        }
    }

    public static String checkJobsStatusFromYaml(String k8sYaml) {
        if (k8sYaml == null || k8sYaml.isEmpty()) {
            return "succeeded";
        }

        List<Job> jobs = K8sYamlUtils.parseK8sYamlAndGetJobs(k8sYaml);

        if (jobs.isEmpty()) {
            return "succeeded";
        }

        boolean anyFailed = false;
        boolean anyRunning = false;

        for (Job job : jobs) {
            String namespace = job.getMetadata() != null ? job.getMetadata().getNamespace() : null;
            String name = job.getMetadata() != null ? job.getMetadata().getName() : null;
            if (name != null) {
                String status = checkJobStatusByName(name, namespace);
                if ("failed".equals(status)) {
                    anyFailed = true;
                } else if ("running".equals(status)) {
                    anyRunning = true;
                }
            }
        }

        if (anyFailed) {
            return "failed";
        }
        if (anyRunning) {
            return "running";
        }
        return "succeeded";
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
