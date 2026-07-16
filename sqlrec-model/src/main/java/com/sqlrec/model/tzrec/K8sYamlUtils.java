package com.sqlrec.model.tzrec;

import com.sqlrec.common.config.ModelConfigs;
import com.sqlrec.common.model.ServiceConfig;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.fabric8.kubernetes.client.utils.Serialization;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

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

    public static String createHeadlessServiceYaml(String jobName, String serviceName, int masterPort) {
        HashMap<String, String> selector = new HashMap<>();
        selector.put("job-name", jobName);
        
        Service service = new ServiceBuilder()
                .withNewMetadata()
                    .withName(serviceName)
                .endMetadata()
                .withNewSpec()
                    .withClusterIP("None")
                    .withSelector(selector)
                    .addNewPort()
                        .withName("torch-distributed")
                        .withPort(masterPort)
                        .withNewTargetPort(masterPort)
                    .endPort()
                .endSpec()
                .build();

        return Serialization.asYaml(service);
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
            String serviceName,
            int nnodes,
            int nprocPerNode,
            int masterPort,
            Map<String, String> params
    ) {
        String image = Config.IMAGE.getValue(params) + ":" + Config.VERSION.getValue(params);

        Job job = new JobBuilder()
                .withNewMetadata()
                    .withName(jobName)
                .endMetadata()
                .withNewSpec()
                    .withCompletions(nnodes)
                    .withParallelism(nnodes)
                    .withCompletionMode("Indexed")
                    .withBackoffLimit(1)
                    .withNewTemplate()
                        .withNewSpec()
                            .withSubdomain(serviceName)
                            .addNewContainer()
                                .withName("tzrec-job")
                                .withImage(image)
                                .withCommand("bash", Config.SHELL_DIR + "/" + Config.START_SHELL_NAME)
                                .withResources(buildResourceRequirements(params))
                                .withEnv(
                                    new ArrayList<EnvVar>() {{
                                        add(new EnvVarBuilder().withName("JOB_NAME").withValue(jobName).build());
                                        add(new EnvVarBuilder().withName("SERVICE_NAME").withValue(serviceName).build());
                                        add(new EnvVarBuilder().withName("MASTER_PORT").withValue(String.valueOf(masterPort)).build());
                                        add(new EnvVarBuilder().withName("NNODES").withValue(String.valueOf(nnodes)).build());
                                        add(new EnvVarBuilder().withName("NPROC_PER_NODE").withValue(String.valueOf(nprocPerNode)).build());
                                        add(new EnvVarBuilder().withName("USE_FSSPEC").withValue(Config.USE_FSSPEC.getDefaultValue()).build());
                                        add(new EnvVarBuilder().withName("USE_SPAWN_MULTI_PROCESS").withValue(Config.USE_SPAWN_MULTI_PROCESS.getDefaultValue()).build());
                                        add(new EnvVarBuilder().withName("USE_FARM_HASH_TO_BUCKETIZE").withValue(Config.USE_FARM_HASH_TO_BUCKETIZE.getDefaultValue()).build());
                                    }}
                                )
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
            Map<String, String> params
    ) {
        if (StringUtils.isEmpty(modelCheckpointDir)) {
            throw new RuntimeException("createDeploymentYaml failed, modelCheckpointDir is empty");
        }

        String image = Config.IMAGE.getValue(params) + ":" + Config.VERSION.getValue(params);

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
                                .withName("tzrec-service")
                                .withImage(image)
                                .withCommand("bash", Config.SERVICE_SHELL_PATH, "--scripted_model_dir", modelCheckpointDir)
                                .withPorts(
                                        new ContainerPortBuilder()
                                                .withName("http")
                                                .withContainerPort(80)
                                                .build()
                                )
                                .withEnv(
                                        new ArrayList<EnvVar>() {{
                                            add(new EnvVarBuilder().withName("USE_FSSPEC").withValue(Config.USE_FSSPEC.getDefaultValue()).build());
                                            add(new EnvVarBuilder().withName("USE_SPAWN_MULTI_PROCESS").withValue(Config.USE_SPAWN_MULTI_PROCESS.getDefaultValue()).build());
                                            add(new EnvVarBuilder().withName("USE_FARM_HASH_TO_BUCKETIZE").withValue(Config.USE_FARM_HASH_TO_BUCKETIZE.getDefaultValue()).build());
                                        }}
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
        String serviceName = jobName + "-headless";
        int nnodes = Config.NNODES.getValue(params);
        int nprocPerNode = Config.NPROC_PER_NODE.getValue(params);
        int masterPort = Config.MASTER_PORT.getValue(params);

        String configMapYaml = createConfigMapYaml(
                configMapName,
                new HashMap<String, String>() {{
                    put(Config.PIPELINE_CONFIG_NAME, pipelineConfig);
                    put(Config.START_SHELL_NAME, shell);
                }}
        );

        String serviceYaml = createHeadlessServiceYaml(jobName, serviceName, masterPort);

        String jobYaml = createJobYaml(
                jobName, configMapName, serviceName, nnodes, nprocPerNode, masterPort, params
        );

        return mergeK8sYamls(configMapYaml, serviceYaml, jobYaml);
    }

    public static String getServiceUrl(ServiceConfig serviceConf) {
        String namespace = ModelConfigs.NAMESPACE.getValue(serviceConf.getParams());
        return "http://" + serviceConf.getId() + "." + namespace + ".svc.cluster.local:80/predict";
    }

    public static String getServiceK8sYaml(ServiceConfig serviceConf) {
        String deploymentName = serviceConf.getId();
        String serviceName = serviceConf.getId();

        String serviceYaml = createServiceYaml(serviceName, 80, "app", deploymentName);
        String deploymentYaml = createDeploymentYaml(
                deploymentName, serviceConf.getModelCheckpointDir(), serviceConf.getParams()
        );

        return mergeK8sYamls(deploymentYaml, serviceYaml);
    }
}
