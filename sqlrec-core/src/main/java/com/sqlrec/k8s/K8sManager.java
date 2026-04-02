package com.sqlrec.k8s;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimVolumeSource;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.utils.Serialization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class K8sManager {
    private static final Logger log = LoggerFactory.getLogger(K8sManager.class);

    public static String convertToValidK8sName(String name) {
        if (name == null || name.isEmpty()) {
            return name;
        }

        String validName = name.toLowerCase();
        validName = validName.replace('_', '-');
        validName = validName.replaceAll("[^a-z0-9.-]", "-");
        validName = validName.trim().replaceAll("^[.-]+|[.-]+$", "");
        validName = validName.replaceAll("[.-]+", "-");

        return validName;
    }

    private static List<HasMetadata> parseK8sYaml(String yamlContent) {
        List<HasMetadata> resources = new ArrayList<>();

        if (yamlContent == null || yamlContent.isEmpty()) {
            return resources;
        }

        try (KubernetesClient client = new KubernetesClientBuilder().build()) {
            InputStream inputStream = new ByteArrayInputStream(yamlContent.getBytes());
            resources = client.load(inputStream).items();
        } catch (Exception e) {
            log.error("Failed to parse YAML: {}", e.getMessage(), e);
        }

        return resources;
    }

    public static List<Job> parseK8sYamlAndGetJobs(String yamlContent) {
        List<Job> jobs = new ArrayList<>();
        List<HasMetadata> resources = parseK8sYaml(yamlContent);

        for (HasMetadata resource : resources) {
            if ("Job".equals(resource.getKind())) {
                jobs.add((Job) resource);
            }
        }

        return jobs;
    }

    public static List<Deployment> parseK8sYamlAndGetDeployments(String yamlContent) {
        List<Deployment> deployments = new ArrayList<>();
        List<HasMetadata> resources = parseK8sYaml(yamlContent);

        for (HasMetadata resource : resources) {
            if ("Deployment".equals(resource.getKind())) {
                deployments.add((Deployment) resource);
            }
        }

        return deployments;
    }

    public static String injectEnvVarsIntoYaml(String yamlContent, Map<String, String> envVars) {
        if (yamlContent == null || yamlContent.isEmpty() || envVars == null || envVars.isEmpty()) {
            return yamlContent;
        }

        try {
            List<HasMetadata> resources = parseK8sYaml(yamlContent);

            for (HasMetadata resource : resources) {
                if ("Job".equals(resource.getKind())) {
                    Job job = (Job) resource;
                    if (job.getSpec() != null && job.getSpec().getTemplate() != null &&
                            job.getSpec().getTemplate().getSpec() != null) {
                        injectEnvVarsIntoContainers(job.getSpec().getTemplate().getSpec().getContainers(), envVars);
                    }
                } else if ("Deployment".equals(resource.getKind())) {
                    Deployment deployment = (Deployment) resource;
                    if (deployment.getSpec() != null && deployment.getSpec().getTemplate() != null &&
                            deployment.getSpec().getTemplate().getSpec() != null) {
                        injectEnvVarsIntoContainers(deployment.getSpec().getTemplate().getSpec().getContainers(), envVars);
                    }
                }
            }

            return mergeYamlResources(resources);
        } catch (Exception e) {
            log.error("Failed to inject env vars into YAML: {}", e.getMessage(), e);
            return yamlContent;
        }
    }

    private static String mergeYamlResources(List<HasMetadata> resources) {
        if (resources == null || resources.isEmpty()) {
            return "";
        }

        StringBuilder yamlBuilder = new StringBuilder();
        for (int i = 0; i < resources.size(); i++) {
            if (i > 0) {
                String nextYaml = Serialization.asYaml(resources.get(i));
                if (!nextYaml.trim().startsWith("---")) {
                    yamlBuilder.append("---\n");
                }
                yamlBuilder.append(nextYaml);
            } else {
                yamlBuilder.append(Serialization.asYaml(resources.get(i)));
            }
        }
        return yamlBuilder.toString();
    }

    private static void injectEnvVarsIntoContainers(List<?> containers, Map<String, String> envVars) {
        if (containers == null || containers.isEmpty()) {
            return;
        }

        for (Object container : containers) {
            try {
                java.lang.reflect.Method getEnvMethod = container.getClass().getMethod("getEnv");
                List<EnvVar> existingEnvVars = (List<EnvVar>) getEnvMethod.invoke(container);

                if (existingEnvVars == null) {
                    existingEnvVars = new ArrayList<>();
                }

                for (Map.Entry<String, String> entry : envVars.entrySet()) {
                    EnvVar envVar = new EnvVar();
                    envVar.setName(entry.getKey());
                    envVar.setValue(entry.getValue());
                    existingEnvVars.add(envVar);
                }

                java.lang.reflect.Method setEnvMethod = container.getClass().getMethod("setEnv", List.class);
                setEnvMethod.invoke(container, existingEnvVars);
            } catch (Exception e) {
                log.error("Failed to inject env vars into container: {}", e.getMessage(), e);
            }
        }
    }

    public static String injectVolumeMountIntoYaml(String yamlContent, String pvcName, String volumeName, String mountPath, String subPath) {
        if (yamlContent == null || yamlContent.isEmpty() || pvcName == null || pvcName.isEmpty() ||
                volumeName == null || volumeName.isEmpty() || mountPath == null || mountPath.isEmpty()) {
            return yamlContent;
        }

        try {
            List<HasMetadata> resources = parseK8sYaml(yamlContent);

            for (HasMetadata resource : resources) {
                if ("Job".equals(resource.getKind())) {
                    Job job = (Job) resource;
                    if (job.getSpec() != null && job.getSpec().getTemplate() != null &&
                            job.getSpec().getTemplate().getSpec() != null) {
                        injectVolumeMount(job.getSpec().getTemplate().getSpec(), pvcName, volumeName, mountPath, subPath);
                    }
                } else if ("Deployment".equals(resource.getKind())) {
                    Deployment deployment = (Deployment) resource;
                    if (deployment.getSpec() != null && deployment.getSpec().getTemplate() != null &&
                            deployment.getSpec().getTemplate().getSpec() != null) {
                        injectVolumeMount(deployment.getSpec().getTemplate().getSpec(), pvcName, volumeName, mountPath, subPath);
                    }
                }
            }

            return mergeYamlResources(resources);
        } catch (Exception e) {
            log.error("Failed to inject namespace into YAML: {}", e.getMessage(), e);
            return yamlContent;
        }
    }

    public static String injectNamespaceIntoYaml(String yamlContent, String namespace) {
        if (yamlContent == null || yamlContent.isEmpty() || namespace == null || namespace.isEmpty()) {
            return yamlContent;
        }

        try {
            List<HasMetadata> resources = parseK8sYaml(yamlContent);

            for (HasMetadata resource : resources) {
                if (resource.getMetadata() != null) {
                    String existingNamespace = resource.getMetadata().getNamespace();
                    if (existingNamespace == null || existingNamespace.isEmpty()) {
                        resource.getMetadata().setNamespace(namespace);
                    }
                }
            }

            return mergeYamlResources(resources);
        } catch (Exception e) {
            log.error("Failed to inject volume mount into YAML: {}", e.getMessage(), e);
            return yamlContent;
        }
    }

    public static String injectNodeSelectorIntoYaml(String yamlContent, Map<String, String> nodeSelectors) {
        if (yamlContent == null || yamlContent.isEmpty() || nodeSelectors == null || nodeSelectors.isEmpty()) {
            return yamlContent;
        }

        try {
            List<HasMetadata> resources = parseK8sYaml(yamlContent);

            for (HasMetadata resource : resources) {
                if ("Job".equals(resource.getKind())) {
                    Job job = (Job) resource;
                    if (job.getSpec() != null && job.getSpec().getTemplate() != null &&
                            job.getSpec().getTemplate().getSpec() != null) {
                        injectNodeSelector(job.getSpec().getTemplate().getSpec(), nodeSelectors);
                    }
                } else if ("Deployment".equals(resource.getKind())) {
                    Deployment deployment = (Deployment) resource;
                    if (deployment.getSpec() != null && deployment.getSpec().getTemplate() != null &&
                            deployment.getSpec().getTemplate().getSpec() != null) {
                        injectNodeSelector(deployment.getSpec().getTemplate().getSpec(), nodeSelectors);
                    }
                }
            }

            return mergeYamlResources(resources);
        } catch (Exception e) {
            log.error("Failed to inject node selector into YAML: {}", e.getMessage(), e);
            return yamlContent;
        }
    }

    private static void injectNodeSelector(PodSpec podSpec, Map<String, String> nodeSelectors) {
        if (podSpec == null || nodeSelectors == null || nodeSelectors.isEmpty()) {
            return;
        }

        Map<String, String> existingNodeSelector = podSpec.getNodeSelector();
        if (existingNodeSelector == null) {
            existingNodeSelector = new java.util.HashMap<>();
        }

        existingNodeSelector.putAll(nodeSelectors);
        podSpec.setNodeSelector(existingNodeSelector);
    }

    public static void applyYaml(String yamlContent) {
        if (yamlContent == null || yamlContent.isEmpty()) {
            return;
        }
        try (KubernetesClient client = new KubernetesClientBuilder().build()) {
            InputStream inputStream = new ByteArrayInputStream(yamlContent.getBytes());
            client.load(inputStream).create();
        } catch (Exception e) {
            log.error("Failed to apply YAML: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to apply YAML: " + e.getMessage(), e);
        }
    }

    public static void deleteYaml(String yamlContent) {
        if (yamlContent == null || yamlContent.isEmpty()) {
            return;
        }

        try (KubernetesClient client = new KubernetesClientBuilder().build()) {
            List<HasMetadata> resources = parseK8sYaml(yamlContent);

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


    private static void injectVolumeMount(Object podSpec, String pvcName, String volumeName, String mountPath, String subPath) {
        if (podSpec == null) {
            return;
        }

        try {
            java.lang.reflect.Method getVolumesMethod = podSpec.getClass().getMethod("getVolumes");
            List<Volume> volumes = (List<Volume>) getVolumesMethod.invoke(podSpec);

            if (volumes == null) {
                volumes = new ArrayList<>();
            }

            boolean volumeExists = false;
            for (Volume volume : volumes) {
                if (volumeName.equals(volume.getName())) {
                    volumeExists = true;
                    break;
                }
            }

            if (!volumeExists) {
                Volume volume = new Volume();
                volume.setName(volumeName);

                PersistentVolumeClaimVolumeSource pvcSource = new PersistentVolumeClaimVolumeSource();
                pvcSource.setClaimName(pvcName);
                volume.setPersistentVolumeClaim(pvcSource);

                volumes.add(volume);

                java.lang.reflect.Method setVolumesMethod = podSpec.getClass().getMethod("setVolumes", List.class);
                setVolumesMethod.invoke(podSpec, volumes);
            }

            java.lang.reflect.Method getContainersMethod = podSpec.getClass().getMethod("getContainers");
            List<?> containers = (List<?>) getContainersMethod.invoke(podSpec);

            for (Object container : containers) {
                java.lang.reflect.Method getVolumeMountsMethod = container.getClass().getMethod("getVolumeMounts");
                List<VolumeMount> volumeMounts = (List<VolumeMount>) getVolumeMountsMethod.invoke(container);

                if (volumeMounts == null) {
                    volumeMounts = new ArrayList<>();
                }

                boolean volumeMountExists = false;
                for (VolumeMount volumeMount : volumeMounts) {
                    if (volumeName.equals(volumeMount.getName())) {
                        volumeMountExists = true;
                        break;
                    }
                }

                if (!volumeMountExists) {
                    VolumeMount volumeMount = new VolumeMount();
                    volumeMount.setName(volumeName);
                    volumeMount.setMountPath(mountPath);
                    if (subPath != null && !subPath.isEmpty()) {
                        volumeMount.setSubPath(subPath);
                    }

                    volumeMounts.add(volumeMount);

                    java.lang.reflect.Method setVolumeMountsMethod = container.getClass().getMethod("setVolumeMounts", List.class);
                    setVolumeMountsMethod.invoke(container, volumeMounts);
                }
            }
        } catch (Exception e) {
            log.error("Failed to inject volume mount: {}", e.getMessage(), e);
        }
    }

    private static String checkJobStatus(String jobName, String namespace) {
        try (KubernetesClient client = new KubernetesClientBuilder().build()) {
            io.fabric8.kubernetes.api.model.batch.v1.Job job = client.batch().v1().jobs()
                    .inNamespace(namespace != null ? namespace : "default")
                    .withName(jobName)
                    .get();
            
            if (job == null) {
                log.warn("Job not found: {}/{}", namespace != null ? namespace : "default", jobName);
                return "running";
            }

            if (job.getStatus() != null) {
                Integer succeeded = job.getStatus().getSucceeded();
                Integer completions = job.getSpec().getCompletions();
                if (succeeded != null && succeeded >= completions) {
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

    private static boolean isDeploymentReady(String deploymentName, String namespace) {
        try (KubernetesClient client = new KubernetesClientBuilder().build()) {
            Deployment deployment = client.apps().deployments()
                    .inNamespace(namespace != null ? namespace : "default")
                    .withName(deploymentName)
                    .get();
            
            if (deployment == null) {
                log.warn("Deployment not found: {}/{}", namespace != null ? namespace : "default", deploymentName);
                return false;
            }
            
            if (deployment.getStatus() != null) {
                Integer readyReplicas = deployment.getStatus().getReadyReplicas();
                if (readyReplicas != null && readyReplicas >= 1) {
                    return true;
                }
            }
            
            return false;
        } catch (Exception e) {
            log.error("Failed to check deployment status: {}", e.getMessage(), e);
            return false;
        }
    }

    public static String checkJobStatus(String k8sYaml) {
        if (k8sYaml == null || k8sYaml.isEmpty()) {
            return "succeeded";
        }
        
        List<Job> jobs = parseK8sYamlAndGetJobs(k8sYaml);
        
        if (jobs.isEmpty()) {
            return "succeeded";
        }
        
        boolean anyFailed = false;
        boolean anyRunning = false;
        
        for (Job job : jobs) {
            String namespace = job.getMetadata() != null ? job.getMetadata().getNamespace() : null;
            String name = job.getMetadata() != null ? job.getMetadata().getName() : null;
            if (name != null) {
                String status = checkJobStatus(name, namespace);
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

    public static boolean isDeploymentReady(String k8sYaml) {
        if (k8sYaml == null || k8sYaml.isEmpty()) {
            return true;
        }
        
        List<Deployment> deployments = parseK8sYamlAndGetDeployments(k8sYaml);
        
        if (deployments.isEmpty()) {
            return true;
        }
        
        boolean allReady = true;
        
        for (Deployment deployment : deployments) {
            String namespace = deployment.getMetadata() != null ? deployment.getMetadata().getNamespace() : null;
            String name = deployment.getMetadata() != null ? deployment.getMetadata().getName() : null;
            if (name != null) {
                if (!isDeploymentReady(name, namespace)) {
                    allReady = false;
                }
            }
        }
        
        return allReady;
    }
}
