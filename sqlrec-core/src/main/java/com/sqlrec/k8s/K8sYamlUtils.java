package com.sqlrec.k8s;

import com.sqlrec.common.config.ModelConfigs;
import com.sqlrec.common.model.ModelConf;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.client.utils.Serialization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class K8sYamlUtils {
    private static final Logger log = LoggerFactory.getLogger(K8sYamlUtils.class);

    private K8sYamlUtils() {
    }

    public static String convertToValidK8sName(String name) {
        if (name == null || name.isEmpty()) {
            return name;
        }

        String validName = name.toLowerCase();
        validName = validName.replace('_', '-');
        validName = validName.replaceAll("[^a-z0-9.-]", "-");
        validName = validName.trim().replaceAll("^[.-]+|[.-]+$", "");
        validName = validName.replaceAll("[.-]+", "-");

        if (validName.length() > 63) {
            validName = validName.substring(0, 63);
            validName = validName.replaceAll("[.-]+$", "");
        }

        return validName;
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
                    if (item instanceof HasMetadata) {
                        resources.add((HasMetadata) item);
                    }
                }
            } else if (resource instanceof HasMetadata) {
                resources.add((HasMetadata) resource);
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

    public static String injectEnvVarsIntoYaml(String yamlContent, Map<String, String> envVars) {
        if (yamlContent == null || yamlContent.isEmpty() || envVars == null || envVars.isEmpty()) {
            return yamlContent;
        }

        try {
            List<HasMetadata> resources = parseK8sYaml(yamlContent);

            for (HasMetadata resource : resources) {
                if (resource instanceof Job) {
                    Job job = (Job) resource;
                    if (job.getSpec() != null && job.getSpec().getTemplate() != null &&
                            job.getSpec().getTemplate().getSpec() != null) {
                        injectEnvVarsIntoContainers(job.getSpec().getTemplate().getSpec().getContainers(), envVars);
                    }
                } else if (resource instanceof Deployment) {
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

                Map<String, String> envMap = new LinkedHashMap<>();
                for (EnvVar envVar : existingEnvVars) {
                    envMap.put(envVar.getName(), envVar.getValue());
                }
                envMap.putAll(envVars);

                List<EnvVar> newEnvVars = new ArrayList<>();
                for (Map.Entry<String, String> entry : envMap.entrySet()) {
                    EnvVar envVar = new EnvVar();
                    envVar.setName(entry.getKey());
                    envVar.setValue(entry.getValue());
                    newEnvVars.add(envVar);
                }

                container.setEnv(newEnvVars);
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
                if (resource instanceof Job) {
                    Job job = (Job) resource;
                    if (job.getSpec() != null && job.getSpec().getTemplate() != null &&
                            job.getSpec().getTemplate().getSpec() != null) {
                        injectVolumeMount(job.getSpec().getTemplate().getSpec(), pvcName, volumeName, mountPath, subPath);
                    }
                } else if (resource instanceof Deployment) {
                    Deployment deployment = (Deployment) resource;
                    if (deployment.getSpec() != null && deployment.getSpec().getTemplate() != null &&
                            deployment.getSpec().getTemplate().getSpec() != null) {
                        injectVolumeMount(deployment.getSpec().getTemplate().getSpec(), pvcName, volumeName, mountPath, subPath);
                    }
                }
            }

            return mergeYamlResources(resources);
        } catch (Exception e) {
            log.error("Failed to inject volume mount into YAML: {}", e.getMessage(), e);
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
            log.error("Failed to inject namespace into YAML: {}", e.getMessage(), e);
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
                if (resource instanceof Job) {
                    Job job = (Job) resource;
                    if (job.getSpec() != null && job.getSpec().getTemplate() != null &&
                            job.getSpec().getTemplate().getSpec() != null) {
                        injectNodeSelector(job.getSpec().getTemplate().getSpec(), nodeSelectors);
                    }
                } else if (resource instanceof Deployment) {
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
            existingNodeSelector = new HashMap<>();
        }

        existingNodeSelector.putAll(nodeSelectors);
        podSpec.setNodeSelector(existingNodeSelector);
    }

    private static void injectVolumeMount(PodSpec podSpec, String pvcName, String volumeName, String mountPath, String subPath) {
        if (podSpec == null) {
            return;
        }

        try {
            List<Volume> volumes = podSpec.getVolumes();

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

                podSpec.setVolumes(volumes);
            }

            List<Container> containers = podSpec.getContainers();

            for (Container container : containers) {
                List<VolumeMount> volumeMounts = container.getVolumeMounts();

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

                    container.setVolumeMounts(volumeMounts);
                }
            }
        } catch (Exception e) {
            log.error("Failed to inject volume mount: {}", e.getMessage(), e);
        }
    }

    public static String injectPodConfig(String k8sYaml, ModelConf model, Map<String, String> params) {
        String namespace;
        if (params.containsKey(ModelConfigs.NAMESPACE.getKey())) {
            namespace = params.get(ModelConfigs.NAMESPACE.getKey());
        } else {
            namespace = ModelConfigs.NAMESPACE.getValue();
        }

        k8sYaml = injectNamespaceIntoYaml(k8sYaml, namespace);

        Map<String, String> envVars = new HashMap<>();
        String javaHome = ModelConfigs.JAVA_HOME.getValue();
        if (javaHome != null) {
            envVars.put("JAVA_HOME", javaHome);
        }
        String hadoopHome = ModelConfigs.HADOOP_HOME.getValue();
        if (hadoopHome != null) {
            envVars.put("HADOOP_HOME", hadoopHome);
        }
        String classpath = ModelConfigs.CLASSPATH.getValue();
        if (classpath != null) {
            envVars.put("CLASSPATH", classpath);
        }
        String hadoopConfDir = ModelConfigs.HADOOP_CONF_DIR.getValue();
        if (hadoopConfDir != null) {
            envVars.put("HADOOP_CONF_DIR", hadoopConfDir);
        }
        String clientDir = ModelConfigs.CLIENT_DIR.getValue();
        if (clientDir != null) {
            envVars.put("CLIENT_DIR", clientDir);
        }
        envVars.putAll(parseEnvVars(params));

        if (!envVars.isEmpty()) {
            k8sYaml = injectEnvVarsIntoYaml(k8sYaml, envVars);
        }

        String pvcName = ModelConfigs.CLIENT_PVC_NAME.getValue();
        String pvName = ModelConfigs.CLIENT_PV_NAME.getValue();
        String clientDirValue = ModelConfigs.CLIENT_DIR.getValue();
        k8sYaml = injectVolumeMountIntoYaml(k8sYaml, pvcName, pvName, clientDirValue, null);

        Map<String, String> nodeSelectors = parseNodeSelectors(params);
        if (!nodeSelectors.isEmpty()) {
            k8sYaml = injectNodeSelectorIntoYaml(k8sYaml, nodeSelectors);
        }

        return k8sYaml;
    }

    private static Map<String, String> parseNodeSelectors(Map<String, String> params) {
        Map<String, String> nodeSelectors = new HashMap<>();
        String prefix = "kubernetes.node.selector.";
        for (Map.Entry<String, String> entry : params.entrySet()) {
            if (entry.getKey().startsWith(prefix)) {
                String labelKey = entry.getKey().substring(prefix.length());
                nodeSelectors.put(labelKey, entry.getValue());
            }
        }
        return nodeSelectors;
    }

    private static Map<String, String> parseEnvVars(Map<String, String> params) {
        Map<String, String> envVars = new HashMap<>();
        String prefix = "kubernetes.env.";
        for (Map.Entry<String, String> entry : params.entrySet()) {
            if (entry.getKey().startsWith(prefix)) {
                String envKey = entry.getKey().substring(prefix.length());
                envVars.put(envKey, entry.getValue());
            }
        }
        return envVars;
    }
}
