package com.sqlrec.model.huggingface;

import com.sqlrec.common.model.ModelConf;
import com.sqlrec.common.model.ServiceConf;
import com.sqlrec.model.common.K8sYamlBuilder;
import com.sqlrec.model.common.ModelConfigBase;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.HTTPGetActionBuilder;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.ProbeBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.SecretKeySelectorBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.fabric8.kubernetes.client.utils.Serialization;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** Kubernetes resources for snapshot download jobs and Transformers services. */
public class HuggingFaceK8sYamlUtils extends K8sYamlBuilder {
    private static final String CONFIG_VOLUME = "config-volume";

    private HuggingFaceK8sYamlUtils() {
    }

    public static String genTrainJobYaml(String pipelineConfig, String shell, String id, Map<String, String> params) {
        String configMapName = id + "-cm";
        String jobName = id + "-job";
        String configMapYaml = createConfigMapYaml(configMapName, new TreeMap<>() {{
            put(ModelConfigBase.PIPELINE_CONFIG_NAME, pipelineConfig);
            put(ModelConfigBase.START_SHELL_NAME, shell);
        }});
        return mergeK8sYamls(configMapYaml, createTrainJobYaml(jobName, configMapName, params));
    }

    static String createTrainJobYaml(String jobName, String configMapName, Map<String, String> params) {
        List<EnvVar> env = new ArrayList<>();
        String secretName = Config.HF_TOKEN_SECRET.getValueOrNull(params);
        if (StringUtils.isNotBlank(secretName)) {
            env.add(new EnvVarBuilder()
                    .withName("HF_TOKEN")
                    .withNewValueFrom()
                        .withSecretKeyRef(new SecretKeySelectorBuilder()
                                .withName(secretName)
                                .withKey(Config.HF_TOKEN_SECRET_KEY.getValue(params))
                                .withOptional(false)
                                .build())
                    .endValueFrom()
                    .build());
        }
        String image = Config.IMAGE.getValue(params) + ":" + Config.VERSION.getValue(params);
        Job job = new JobBuilder()
                .withNewMetadata().withName(jobName).endMetadata()
                .withNewSpec()
                    .withBackoffLimit(1)
                    .withNewTemplate()
                        .withNewSpec()
                            .addNewContainer()
                                .withName("transformers-download")
                                .withImage(image)
                                .withCommand("bash", Config.SHELL_DIR + "/" + Config.START_SHELL_NAME)
                                .withEnv(env)
                                .withResources(buildResourceRequirements(params))
                                .addNewVolumeMount().withName(CONFIG_VOLUME).withMountPath(Config.SHELL_DIR).endVolumeMount()
                            .endContainer()
                            .addNewVolume().withName(CONFIG_VOLUME)
                                .withNewConfigMap().withName(configMapName).endConfigMap()
                            .endVolume()
                            .withRestartPolicy("Never")
                        .endSpec()
                    .endTemplate()
                .endSpec()
                .build();
        return Serialization.asYaml(job);
    }

    public static String getServiceK8sYaml(ModelConf model, ServiceConf serviceConf) {
        if (StringUtils.isBlank(serviceConf.getModelCheckpointDir())) {
            throw new IllegalArgumentException("model checkpoint is required for huggingface.transformers service");
        }
        String name = serviceConf.getId();
        Map<String, String> params = PipelineConfigUtils.mergeParams(model.getParams(), serviceConf.getParams());
        String serviceConfig = PipelineConfigUtils.generateServiceConfig(model, serviceConf.getParams());
        String shell = ShellUtils.genServeShell(serviceConf.getModelCheckpointDir());

        ConfigMap configMap = new ConfigMapBuilder()
                .withNewMetadata().withName(name + "-cm").endMetadata()
                .addToData("service.config", serviceConfig)
                .addToData(ModelConfigBase.START_SHELL_NAME, shell)
                .build();
        String configMapYaml = Serialization.asYaml(configMap);
        String deploymentYaml = createServiceDeploymentYaml(name, name + "-cm", params);
        String serviceYaml = createServiceYaml(name, 80, "app", name);
        return mergeK8sYamls(configMapYaml, deploymentYaml, serviceYaml);
    }

    static String createServiceDeploymentYaml(String name, String configMapName, Map<String, String> params) {
        String image = Config.IMAGE.getValue(params) + ":" + Config.VERSION.getValue(params);
        Deployment deployment = new DeploymentBuilder()
                .withNewMetadata().withName(name).endMetadata()
                .withNewSpec()
                    .withReplicas(Config.REPLICAS.getValue(params))
                    .withNewSelector().withMatchLabels(Map.of("app", name)).endSelector()
                    .withNewTemplate()
                        .withNewMetadata().withLabels(Map.of("app", name)).endMetadata()
                        .withNewSpec()
                            .addNewContainer()
                                .withName("transformers-service")
                                .withImage(image)
                                .withCommand("bash", Config.SHELL_DIR + "/" + Config.START_SHELL_NAME)
                                .withPorts(new ContainerPortBuilder().withName("http").withContainerPort(80).build())
                                .withResources(buildServiceResources(params))
                                .withReadinessProbe(new ProbeBuilder()
                                        .withHttpGet(new HTTPGetActionBuilder()
                                                .withPath("/ready")
                                                .withPort(new IntOrString(80))
                                                .withScheme("HTTP")
                                                .build())
                                        .withInitialDelaySeconds(2)
                                        .withPeriodSeconds(5)
                                        .withTimeoutSeconds(2)
                                        .withFailureThreshold(60)
                                        .build())
                                .addNewVolumeMount().withName(CONFIG_VOLUME).withMountPath(Config.SHELL_DIR).endVolumeMount()
                            .endContainer()
                            .addNewVolume().withName(CONFIG_VOLUME)
                                .withNewConfigMap().withName(configMapName).endConfigMap()
                            .endVolume()
                        .endSpec()
                    .endTemplate()
                .endSpec()
                .build();
        return Serialization.asYaml(deployment);
    }

    private static ResourceRequirements buildServiceResources(Map<String, String> params) {
        ResourceRequirementsBuilder builder = new ResourceRequirementsBuilder(buildResourceRequirements(params));
        int gpuCount = Config.POD_GPU.getValue(params);
        if (gpuCount > 0) {
            String resourceName = Config.POD_GPU_RESOURCE.getValue(params);
            Quantity quantity = new Quantity(String.valueOf(gpuCount));
            builder.addToRequests(resourceName, quantity);
            builder.addToLimits(resourceName, quantity);
        }
        return builder.build();
    }
}
