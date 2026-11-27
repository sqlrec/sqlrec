package com.sqlrec.model.huggingface;

import com.sqlrec.common.model.ModelConf;
import com.sqlrec.common.model.ServiceConf;
import com.sqlrec.model.common.K8sYamlBuilder;
import com.sqlrec.model.common.ModelConfigBase;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.HTTPGetActionBuilder;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.ProbeBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.SecretKeySelectorBuilder;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.utils.Serialization;
import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

/** Kubernetes resources for snapshot download jobs and Transformers services. */
public class HuggingFaceK8sYamlUtils extends K8sYamlBuilder {
    private static final String CONFIG_VOLUME = "config-volume";

    private HuggingFaceK8sYamlUtils() {
    }

    public static String genTrainJobYaml(String pipelineConfig, String shell, String id, Map<String, String> params) {
        String configMapName = id + "-cm";
        String jobName = id + "-job";
        String configMapYaml = createPipelineConfigMapYaml(configMapName, pipelineConfig, shell);
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
        return createSingleNodeJobYaml(
                jobName,
                configMapName,
                "transformers-download",
                image,
                env,
                params
        );
    }

    public static String getServiceK8sYaml(ModelConf model, ServiceConf serviceConf) {
        if (StringUtils.isBlank(serviceConf.getModelCheckpointDir())) {
            throw new IllegalArgumentException("model checkpoint is required for huggingface.transformers service");
        }
        String name = serviceConf.getId();
        Map<String, String> params = PipelineConfigUtils.operationParams(model, serviceConf.getParams());
        String serviceConfig = PipelineConfigUtils.buildServiceConfig(model, params);
        String shell = ShellUtils.genServeShell(serviceConf.getModelCheckpointDir());
        ConfigMap configMap = new ConfigMapBuilder()
                .withNewMetadata().withName(name + "-cm").endMetadata()
                .addToData("service.config", serviceConfig)
                .addToData(ModelConfigBase.START_SHELL_NAME, shell)
                .build();
        String configMapYaml = Serialization.asYaml(configMap);
        String deploymentYaml = createServiceDeploymentYaml(
                name, name + "-cm", params, configChecksum(serviceConfig, shell));
        return mergeK8sYamls(configMapYaml, createServingResourcesYaml(name, deploymentYaml));
    }

    private static String configChecksum(String serviceConfig, String shell) {
        try {
            byte[] content = (serviceConfig + "\n" + shell).getBytes(StandardCharsets.UTF_8);
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(content));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    static String createServiceDeploymentYaml(String name, String configMapName,
                                             Map<String, String> params, String configChecksum) {
        String image = Config.IMAGE.getValue(params) + ":" + Config.VERSION.getValue(params);
        List<EnvVar> env = new ArrayList<>();
        String secretName = Config.OPENAI_API_KEY_SECRET.getValueOrNull(params);
        if (StringUtils.isNotBlank(secretName)) {
            env.add(new EnvVarBuilder().withName("SQLREC_OPENAI_API_KEY")
                    .withNewValueFrom().withNewSecretKeyRef()
                        .withName(secretName).withKey(Config.OPENAI_API_KEY_SECRET_KEY.getValue(params))
                        .withOptional(false)
                    .endSecretKeyRef().endValueFrom().build());
        }
        Deployment deployment = createDeployment(name, "transformers-service", image,
                List.of("bash", Config.SHELL_DIR + "/" + Config.START_SHELL_NAME), env, params);
        var template = deployment.getSpec().getTemplate();
        // Updating the checksum triggers a rollout when the ConfigMap changes.
        template.getMetadata().setAnnotations(Map.of("sqlrec.io/config-checksum", configChecksum));
        var pod = template.getSpec();
        pod.setVolumes(List.of(new VolumeBuilder().withName(CONFIG_VOLUME)
                .withNewConfigMap().withName(configMapName).endConfigMap().build()));

        Container container = pod.getContainers().getFirst();
        container.setVolumeMounts(List.of(new VolumeMountBuilder().withName(CONFIG_VOLUME)
                .withMountPath(Config.SHELL_DIR).build()));
        container.setReadinessProbe(new ProbeBuilder()
                .withHttpGet(new HTTPGetActionBuilder()
                        .withPath("/ready")
                        .withPort(new IntOrString(80))
                        .withScheme("HTTP")
                        .build())
                .withInitialDelaySeconds(2)
                .withPeriodSeconds(5)
                .withTimeoutSeconds(2)
                .withFailureThreshold(60)
                .build());
        addGpuResources(container, params);
        return Serialization.asYaml(deployment);
    }

    private static void addGpuResources(Container container, Map<String, String> params) {
        int gpuCount = Config.POD_GPU.getValue(params);
        if (gpuCount > 0) {
            String resourceName = Config.POD_GPU_RESOURCE.getValue(params);
            Quantity quantity = new Quantity(String.valueOf(gpuCount));
            container.setResources(new ResourceRequirementsBuilder(container.getResources())
                    .addToRequests(resourceName, quantity)
                    .addToLimits(resourceName, quantity)
                    .build());
        }
    }
}
