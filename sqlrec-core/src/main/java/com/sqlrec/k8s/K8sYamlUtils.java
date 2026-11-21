package com.sqlrec.k8s;

import com.sqlrec.common.config.ModelConfigs;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.client.utils.Serialization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class K8sYamlUtils {
    private static final Logger log = LoggerFactory.getLogger(K8sYamlUtils.class);

    private K8sYamlUtils() {
    }

    /** Readable names plus a digest of the full logical names, leaving room for -job-headless. */
    public static String checkpointJobId(String operation, String modelName, String checkpointName) {
        return checkpointJobId(operation, modelName, checkpointName, "");
    }

    /** Old completion callbacks must only be able to clean up their own attempt's resources. */
    public static String checkpointAttemptJobId(String operation, String modelName, String checkpointName) {
        return checkpointJobId(operation, modelName, checkpointName, "\0" + UUID.randomUUID());
    }

    private static String checkpointJobId(String operation, String modelName, String checkpointName, String attempt) {
        if (!Set.of("train", "export").contains(operation)) {
            throw new IllegalArgumentException("Unsupported checkpoint operation: " + operation);
        }
        String key = operation + "\0" + modelName + "\0" + checkpointName + attempt;
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(key.getBytes(StandardCharsets.UTF_8));
            String hash = HexFormat.of().formatHex(digest, 0, 8);
            String prefix = operation + "-";
            String model = jobNamePart(modelName);
            String checkpoint = jobNamePart(checkpointName);
            int nameBudget = 63 - "-job-headless".length() - prefix.length() - hash.length() - 2;
            // Share the readable space, letting the longer name use any remainder.
            int modelLength = Math.min(model.length(), nameBudget / 2);
            int checkpointLength = Math.min(checkpoint.length(), nameBudget - modelLength);
            modelLength = Math.min(model.length(), nameBudget - checkpointLength);
            return prefix + model.substring(0, modelLength).replaceAll("-+$", "") + "-"
                    + checkpoint.substring(0, checkpointLength).replaceAll("-+$", "") + "-" + hash;
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    private static String jobNamePart(String name) {
        String part = name.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]+", "-").replaceAll("^-|-$", "");
        return part.isEmpty() ? "x" : part;
    }

    /** Kubernetes Service RFC 1035 DNS labels, without normalization or suffixes. */
    public static String serviceResourceId(String serviceName) {
        if (serviceName == null || serviceName.length() > 63
                || !serviceName.matches("[a-z]([-a-z0-9]*[a-z0-9])?")) {
            throw new IllegalArgumentException("Invalid service name: " + serviceName
                    + ". Follow Kubernetes Service DNS (RFC 1035) naming rules: 1-63 lowercase letters,"
                    + " digits or hyphens, starting with a lowercase letter and ending with a lowercase"
                    + " letter or digit. Uppercase letters and other characters are not allowed.");
        }
        return serviceName;
    }

    @SuppressWarnings("deprecation")
    static List<HasMetadata> parseK8sYaml(String yamlContent) {
        List<HasMetadata> resources = new ArrayList<>();

        if (yamlContent == null || yamlContent.isEmpty()) {
            return resources;
        }

        try (InputStream inputStream = new ByteArrayInputStream(yamlContent.getBytes(StandardCharsets.UTF_8))) {
            Object resource = Serialization.unmarshal(inputStream);
            if (resource instanceof Iterable) {
                for (Object item : (Iterable<?>) resource) {
                    if (item instanceof HasMetadata metadata) {
                        resources.add(metadata);
                    }
                }
            } else if (resource instanceof HasMetadata metadata) {
                resources.add(metadata);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse YAML: " + e.getMessage(), e);
        }

        return resources;
    }

    private static <T extends HasMetadata> List<T> parseK8sYamlAndGetResources(String yamlContent, Class<T> resourceType) {
        List<T> result = new ArrayList<>();
        List<HasMetadata> resources = parseK8sYaml(yamlContent);

        for (HasMetadata resource : resources) {
            if (resourceType.isInstance(resource)) {
                result.add(resourceType.cast(resource));
            }
        }

        return result;
    }

    public static List<Job> parseK8sYamlAndGetJobs(String yamlContent) {
        return parseK8sYamlAndGetResources(yamlContent, Job.class);
    }

    public static List<Deployment> parseK8sYamlAndGetDeployments(String yamlContent) {
        return parseK8sYamlAndGetResources(yamlContent, Deployment.class);
    }

    private static String mergeYamlResources(List<HasMetadata> resources) {
        if (resources == null || resources.isEmpty()) {
            return "";
        }

        StringBuilder yamlBuilder = new StringBuilder();
        for (int i = 0; i < resources.size(); i++) {
            String resourceYaml = Serialization.asYaml(resources.get(i));
            if (i > 0 && !resourceYaml.trim().startsWith("---")) {
                yamlBuilder.append("---\n");
            }
            yamlBuilder.append(resourceYaml);
        }
        return yamlBuilder.toString();
    }

    private static PodSpec getPodSpec(HasMetadata resource) {
        if (resource instanceof Job job
                && job.getSpec() != null
                && job.getSpec().getTemplate() != null) {
            return job.getSpec().getTemplate().getSpec();
        }
        if (resource instanceof Deployment deployment
                && deployment.getSpec() != null
                && deployment.getSpec().getTemplate() != null) {
            return deployment.getSpec().getTemplate().getSpec();
        }
        return null;
    }

    private static void injectEnvVarsIntoContainers(List<Container> containers, Map<String, String> envVars) {
        if (containers == null || containers.isEmpty()) {
            return;
        }

        for (Container container : containers) {
            try {
                List<EnvVar> existingEnvVars = container.getEnv();
                if (existingEnvVars == null) {
                    existingEnvVars = new ArrayList<>();
                }

                Map<String, EnvVar> envByName = new LinkedHashMap<>();
                for (EnvVar envVar : existingEnvVars) {
                    // Keep references such as secretKeyRef and fieldRef intact.
                    envByName.put(envVar.getName(), envVar);
                }

                for (Map.Entry<String, String> entry : envVars.entrySet()) {
                    EnvVar envVar = new EnvVar();
                    envVar.setName(entry.getKey());
                    envVar.setValue(entry.getValue());
                    // An explicit value replaces any previous valueFrom for this name.
                    envByName.put(entry.getKey(), envVar);
                }

                container.setEnv(new ArrayList<>(envByName.values()));
            } catch (Exception e) {
                log.error("Failed to inject env vars into container: {}", e.getMessage(), e);
            }
        }
    }

    private static void injectNamespace(HasMetadata resource, String namespace) {
        if (namespace.isEmpty() || resource.getMetadata() == null) {
            return;
        }
        String existingNamespace = resource.getMetadata().getNamespace();
        if (existingNamespace == null || existingNamespace.isEmpty()) {
            resource.getMetadata().setNamespace(namespace);
        }
    }

    private static void injectNodeSelector(PodSpec podSpec, Map<String, String> nodeSelectors) {
        if (nodeSelectors.isEmpty()) {
            return;
        }

        Map<String, String> existingNodeSelector = podSpec.getNodeSelector();
        if (existingNodeSelector == null) {
            existingNodeSelector = new HashMap<>();
        }

        existingNodeSelector.putAll(nodeSelectors);
        podSpec.setNodeSelector(existingNodeSelector);
    }

    private static void injectVolumeMount(PodSpec podSpec, String pvcName, String volumeName, String mountPath) {
        if (pvcName.isEmpty() || volumeName.isEmpty() || mountPath.isEmpty()) {
            return;
        }

        try {
            ensureVolume(podSpec, pvcName, volumeName);
            for (Container container : podSpec.getContainers()) {
                ensureVolumeMount(container, volumeName, mountPath);
            }
        } catch (Exception e) {
            log.error("Failed to inject volume mount: {}", e.getMessage(), e);
        }
    }

    private static void ensureVolume(PodSpec podSpec, String pvcName, String volumeName) {
        List<Volume> volumes = podSpec.getVolumes();
        if (volumes == null) {
            volumes = new ArrayList<>();
        }

        for (Volume volume : volumes) {
            if (volumeName.equals(volume.getName())) {
                return;
            }
        }

        Volume volume = new Volume();
        volume.setName(volumeName);
        PersistentVolumeClaimVolumeSource pvcSource = new PersistentVolumeClaimVolumeSource();
        pvcSource.setClaimName(pvcName);
        volume.setPersistentVolumeClaim(pvcSource);
        volumes.add(volume);
        podSpec.setVolumes(volumes);
    }

    private static void ensureVolumeMount(Container container, String volumeName, String mountPath) {
        List<VolumeMount> volumeMounts = container.getVolumeMounts();
        if (volumeMounts == null) {
            volumeMounts = new ArrayList<>();
        }

        for (VolumeMount volumeMount : volumeMounts) {
            if (volumeName.equals(volumeMount.getName())) {
                return;
            }
        }

        VolumeMount volumeMount = new VolumeMount();
        volumeMount.setName(volumeName);
        volumeMount.setMountPath(mountPath);
        volumeMounts.add(volumeMount);
        container.setVolumeMounts(volumeMounts);
    }

    public static String injectPodConfig(String k8sYaml, Map<String, String> params) {
        String namespace = ModelConfigs.NAMESPACE.getValueWithEnvFallback(params);

        Map<String, String> envVars = new HashMap<>();
        envVars.put("JAVA_HOME", ModelConfigs.JAVA_HOME.getValue());
        envVars.put("HADOOP_HOME", ModelConfigs.HADOOP_HOME.getValue());
        envVars.put("CLASSPATH", ModelConfigs.CLASSPATH.getValue());
        envVars.put("HADOOP_CONF_DIR", ModelConfigs.HADOOP_CONF_DIR.getValue());
        String clientDir = ModelConfigs.CLIENT_DIR.getValue();
        envVars.put("CLIENT_DIR", clientDir);
        envVars.putAll(parsePrefixedEntries(params, "kubernetes.env."));

        String pvcName = ModelConfigs.CLIENT_PVC_NAME.getValue();
        String pvName = ModelConfigs.CLIENT_PV_NAME.getValue();
        Map<String, String> nodeSelectors = parsePrefixedEntries(params, "kubernetes.node.selector.");
        if (k8sYaml == null || k8sYaml.isEmpty()) {
            return k8sYaml;
        }

        try {
            List<HasMetadata> resources = parseK8sYaml(k8sYaml);
            for (HasMetadata resource : resources) {
                injectNamespace(resource, namespace);
                PodSpec podSpec = getPodSpec(resource);
                if (podSpec != null) {
                    injectEnvVarsIntoContainers(podSpec.getContainers(), envVars);
                    injectVolumeMount(podSpec, pvcName, pvName, clientDir);
                    injectNodeSelector(podSpec, nodeSelectors);
                }
            }
            return mergeYamlResources(resources);
        } catch (Exception e) {
            log.error("Failed to inject pod config into YAML: {}", e.getMessage(), e);
            return k8sYaml;
        }
    }

    private static Map<String, String> parsePrefixedEntries(
            Map<String, String> params,
            String prefix
    ) {
        Map<String, String> entries = new HashMap<>();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            if (entry.getKey().startsWith(prefix)) {
                String key = entry.getKey().substring(prefix.length());
                entries.put(key, entry.getValue());
            }
        }
        return entries;
    }
}
