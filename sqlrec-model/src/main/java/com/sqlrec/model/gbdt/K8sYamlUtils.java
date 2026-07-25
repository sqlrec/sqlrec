package com.sqlrec.model.gbdt;

import com.sqlrec.common.config.ModelConfigs;
import com.sqlrec.common.model.ServiceConfig;
import com.sqlrec.model.gbdt.PipelineConfigUtils.ModelType;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.fabric8.kubernetes.client.utils.Serialization;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Kubernetes YAML generation for GBDT (CatBoost / LightGBM) train, export and service.
 *
 * <p>Training runs as a single-replica K8s Job (no distributed mode).
 * The container command runs the GBDT Python entry points and the image is
 * {@code sqlrec/gbdt}.
 */
public class K8sYamlUtils {

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
        HashMap<String, String> selector = new HashMap<>();
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

    public static String createJobYaml(
            String jobName,
            String configMapName,
            Map<String, String> params
    ) {
        String image = Config.IMAGE.getValue(params) + ":" + Config.VERSION.getValue(params);

        Job job = new JobBuilder()
                .withNewMetadata()
                    .withName(jobName)
                .endMetadata()
                .withNewSpec()
                    .withBackoffLimit(1)
                    .withNewTemplate()
                        .withNewSpec()
                            .addNewContainer()
                                .withName("gbdt-job")
                                .withImage(image)
                                .withCommand("bash", Config.SHELL_DIR + "/" + Config.START_SHELL_NAME)
                                .withResources(buildResourceRequirements(params))
                                .addNewVolumeMount()
                                    .withName("config-volume")
                                    .withMountPath(Config.SHELL_DIR)
                                .endVolumeMount()
                            .endContainer()
                            .addNewVolume()
                                .withName("config-volume")
                                .withNewConfigMap()
                                    .withName(configMapName)
                                .endConfigMap()
                            .endVolume()
                            .withRestartPolicy("Never")
                        .endSpec()
                    .endTemplate()
                .endSpec()
                .build();

        return Serialization.asYaml(job);
    }

    public static String createDeploymentYaml(
            String deployName,
            String modelCheckpointDir,
            ModelType modelType,
            Map<String, String> params
    ) {
        if (StringUtils.isEmpty(modelCheckpointDir)) {
            throw new RuntimeException("createDeploymentYaml failed, modelCheckpointDir is empty");
        }

        String image = Config.IMAGE.getValue(params) + ":" + Config.VERSION.getValue(params);

        // Generate the serving shell script that downloads model from HDFS and
        // launches the C++ server binary (catboost_server / lightgbm_server).
        String serveShell = ShellUtils.genServeModelShell(modelType, modelCheckpointDir);

        Deployment deployment = new DeploymentBuilder()
                .withNewMetadata()
                    .withName(deployName)
                .endMetadata()
                .withNewSpec()
                    .withReplicas(Config.REPLICAS.getValue(params))
                    .withNewSelector()
                        .withMatchLabels(new HashMap<String, String>() {{
                            put("app", deployName);
                        }})
                    .endSelector()
                    .withNewTemplate()
                        .withNewMetadata()
                            .withLabels(new HashMap<String, String>() {{
                                put("app", deployName);
                            }})
                        .endMetadata()
                        .withNewSpec()
                            .addNewContainer()
                                .withName("gbdt-service")
                                .withImage(image)
                                .withCommand("bash", "-c", serveShell)
                                .withPorts(
                                        new ContainerPortBuilder()
                                                .withName("http")
                                                .withContainerPort(80)
                                                .build()
                                )
                                .withResources(buildResourceRequirements(params))
                            .endContainer()
                        .endSpec()
                    .endTemplate()
                .endSpec()
                .build();

        return Serialization.asYaml(deployment);
    }

    /**
     * Builds pod resource requirements. Requests are always configured from {@link Config#POD_CPU_CORES}
     * and {@link Config#POD_MEMORY}. Limits are only added when the corresponding limit option
     * ({@link Config#POD_CPU_LIMIT} / {@link Config#POD_MEMORY_LIMIT}) is present in params.
     */
    private static ResourceRequirements buildResourceRequirements(Map<String, String> params) {
        ResourceRequirementsBuilder builder = new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity(String.valueOf(Config.POD_CPU_CORES.getValue(params))))
                .addToRequests("memory", new Quantity(Config.POD_MEMORY.getValue(params)));

        if (Config.POD_CPU_LIMIT.isSet(params)) {
            builder.addToLimits("cpu", new Quantity(Config.POD_CPU_LIMIT.getValue(params)));
        }
        if (Config.POD_MEMORY_LIMIT.isSet(params)) {
            builder.addToLimits("memory", new Quantity(Config.POD_MEMORY_LIMIT.getValue(params)));
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

    public static String genJobYaml(String pipelineConfig, String shell, String id, Map<String, String> params) {
        String configMapName = id + "-cm";
        String jobName = id + "-job";

        String configMapYaml = createConfigMapYaml(
                configMapName,
                new HashMap<String, String>() {{
                    put(Config.PIPELINE_CONFIG_NAME, pipelineConfig);
                    put(Config.START_SHELL_NAME, shell);
                }}
        );

        String jobYaml = createJobYaml(jobName, configMapName, params);

        return mergeK8sYamls(configMapYaml, jobYaml);
    }

    public static String getServiceUrl(ServiceConfig serviceConf) {
        String namespace = ModelConfigs.NAMESPACE.getValue(serviceConf.getParams());
        return "http://" + serviceConf.getId() + "." + namespace + ".svc.cluster.local:80/predict";
    }

    public static String getServiceK8sYaml(ModelType modelType, ServiceConfig serviceConf) {
        String deploymentName = serviceConf.getId();
        String serviceName = serviceConf.getId();

        String serviceYaml = createServiceYaml(serviceName, 80, "app", deploymentName);
        String deploymentYaml = createDeploymentYaml(
                deploymentName, serviceConf.getModelCheckpointDir(), modelType, serviceConf.getParams()
        );

        return mergeK8sYamls(deploymentYaml, serviceYaml);
    }
}
