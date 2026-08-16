package com.sqlrec.model.common;

import com.sqlrec.common.config.ModelConfigs;
import com.sqlrec.common.model.ServiceConf;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.client.utils.Serialization;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Shared Kubernetes YAML generation primitives used by every model backend.
 *
 * <p>GBDT ({@code com.sqlrec.model.gbdt.GbdtK8sYamlUtils}) and TZRec
 * ({@code com.sqlrec.model.tzrec.TzrecK8sYamlUtils}) both extend this class so that the identical
 * ConfigMap / Service / Deployment / resource-requirements / YAML-merge / service-URL helpers
 * exist in a single place. Only the backend-specific {@code createJobYaml} / {@code genJobYaml}
 * / {@code getServiceK8sYaml} stay in the subclasses.
 *
 * <p>Note: this is the YAML <em>generator</em>. The unrelated YAML parser/injector in
 * {@code com.sqlrec.k8s.K8sYamlUtils} (sqlrec-core) only reads/mutates existing YAML.
 */
public class K8sYamlBuilder {

    protected K8sYamlBuilder() {
    }

    public static String createConfigMapYaml(String name, Map<String, String> files) {
        ConfigMap configMap = new ConfigMapBuilder()
                .withNewMetadata()
                    .withName(name)
                .endMetadata()
                .withData(files)
                .build();

        return Serialization.asYaml(configMap);
    }

    public static String createServiceYaml(String serviceName, int port, String selectKey, String selectValue) {
        Map<String, String> selector = new HashMap<>();
        selector.put(selectKey, selectValue);

        Service service = new ServiceBuilder()
                .withNewMetadata()
                    .withName(serviceName)
                .endMetadata()
                .withNewSpec()
                    .withSelector(selector)
                    .addNewPort()
                        .withName("server")
                        .withPort(port)
                        .withNewTargetPort(port)
                    .endPort()
                .endSpec()
                .build();

        return Serialization.asYaml(service);
    }

    /**
     * Builds a single-container serving Deployment: replicas from {@link ModelConfigBase#REPLICAS},
     * an {@code app: <deployName>} selector/label, an http container port and shared pod resources.
     * Backends only supply the container name, image, command and optional env vars.
     *
     * @param envVars may be {@code null} to omit the env section entirely.
     */
    protected static String createDeploymentYaml(
            String deployName,
            String containerName,
            String image,
            List<String> command,
            List<EnvVar> envVars,
            Map<String, String> params
    ) {
        Deployment deployment = new DeploymentBuilder()
                .withNewMetadata()
                    .withName(deployName)
                .endMetadata()
                .withNewSpec()
                    .withReplicas(ModelConfigBase.REPLICAS.getValue(params))
                    .withNewSelector()
                        .withMatchLabels(Map.of("app", deployName))
                    .endSelector()
                    .withNewTemplate()
                        .withNewMetadata()
                            .withLabels(Map.of("app", deployName))
                        .endMetadata()
                        .withNewSpec()
                            .addNewContainer()
                                .withName(containerName)
                                .withImage(image)
                                .withCommand(command)
                                .withPorts(
                                        new ContainerPortBuilder()
                                                .withName("http")
                                                .withContainerPort(80)
                                                .build()
                                )
                                .withEnv(envVars)
                                .withResources(buildResourceRequirements(params))
                            .endContainer()
                        .endSpec()
                    .endTemplate()
                .endSpec()
                .build();

        return Serialization.asYaml(deployment);
    }

    /**
     * Builds pod resource requirements. Requests are always configured from
     * {@link ModelConfigBase#POD_CPU_CORES} and {@link ModelConfigBase#POD_MEMORY}. Limits are only
     * added when the corresponding limit option ({@link ModelConfigBase#POD_CPU_LIMIT} /
     * {@link ModelConfigBase#POD_MEMORY_LIMIT}) is present in params.
     */
    protected static ResourceRequirements buildResourceRequirements(Map<String, String> params) {
        ResourceRequirementsBuilder builder = new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity(String.valueOf(ModelConfigBase.POD_CPU_CORES.getValue(params))))
                .addToRequests("memory", new Quantity(ModelConfigBase.POD_MEMORY.getValue(params)));

        if (ModelConfigBase.POD_CPU_LIMIT.isSet(params)) {
            builder.addToLimits("cpu", new Quantity(ModelConfigBase.POD_CPU_LIMIT.getValue(params)));
        }
        if (ModelConfigBase.POD_MEMORY_LIMIT.isSet(params)) {
            builder.addToLimits("memory", new Quantity(ModelConfigBase.POD_MEMORY_LIMIT.getValue(params)));
        }

        return builder.build();
    }

    public static String mergeK8sYamls(String... yamls) {
        StringBuilder mergedYaml = new StringBuilder();
        for (int i = 0; i < yamls.length; i++) {
            if (i > 0 && !yamls[i].startsWith("---")) {
                mergedYaml.append("---\n");
            }
            mergedYaml.append(yamls[i]);
            if (!yamls[i].endsWith("\n")) {
                mergedYaml.append("\n");
            }
        }
        return mergedYaml.toString();
    }

    public static String getServiceUrl(ServiceConf serviceConf) {
        String namespace = ModelConfigs.NAMESPACE.getValue(serviceConf.getParams());
        return "http://" + serviceConf.getId() + "." + namespace + ".svc.cluster.local:80/predict";
    }
}
