package com.sqlrec.model.tzrec;

import com.sqlrec.common.model.ServiceConf;
import com.sqlrec.model.common.K8sYamlBuilder;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.fabric8.kubernetes.client.utils.Serialization;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

/**
 * Kubernetes YAML generation for TZRec (DSSM / WideAndDeep) train, export and service.
 *
 * <p>Training runs as a distributed K8s Job (indexed completion + headless service for torchrun).
 * The container command runs the TZRec torchrun entry points and the image is {@code sqlrec/tzrec}.
 * Common YAML primitives (ConfigMap / Service / resource requirements / YAML merge / service URL)
 * are inherited from {@link K8sYamlBuilder}.
 */
public class K8sYamlUtils extends K8sYamlBuilder {

    public static String createHeadlessServiceYaml(String jobName, String serviceName, int masterPort) {
        Map<String, String> selector = new HashMap<>();
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

        List<EnvVar> envVars = new ArrayList<>();
        envVars.add(new EnvVarBuilder().withName("JOB_NAME").withValue(jobName).build());
        envVars.add(new EnvVarBuilder().withName("SERVICE_NAME").withValue(serviceName).build());
        envVars.add(new EnvVarBuilder().withName("MASTER_PORT").withValue(String.valueOf(masterPort)).build());
        envVars.add(new EnvVarBuilder().withName("NNODES").withValue(String.valueOf(nnodes)).build());
        envVars.add(new EnvVarBuilder().withName("NPROC_PER_NODE").withValue(String.valueOf(nprocPerNode)).build());
        envVars.addAll(buildRuntimeEnvVars());

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
                                .withEnv(envVars)
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
                        .withMatchLabels(Map.of("app", deployName))
                    .endSelector()
                    .withNewTemplate()
                        .withNewMetadata()
                            .withLabels(Map.of("app", deployName))
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
                                .withEnv(buildRuntimeEnvVars())
                                .withResources(buildResourceRequirements(params))
                            .endContainer()
                        .endSpec()
                    .endTemplate()
                .endSpec()
                .build();

        return Serialization.asYaml(deployment);
    }

    /**
     * TZRec runtime env vars shared by both the training Job and the serving Deployment.
     *
     * <p>Note: these currently read {@code getDefaultValue()}; passing per-job overrides via
     * {@code params} is a known follow-up (tracked separately).
     */
    private static List<EnvVar> buildRuntimeEnvVars() {
        return List.of(
                new EnvVarBuilder().withName("USE_FSSPEC").withValue(Config.USE_FSSPEC.getDefaultValue()).build(),
                new EnvVarBuilder().withName("USE_SPAWN_MULTI_PROCESS").withValue(Config.USE_SPAWN_MULTI_PROCESS.getDefaultValue()).build(),
                new EnvVarBuilder().withName("USE_FARM_HASH_TO_BUCKETIZE").withValue(Config.USE_FARM_HASH_TO_BUCKETIZE.getDefaultValue()).build()
        );
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
                new TreeMap<>() {{
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

    public static String getServiceK8sYaml(ServiceConf serviceConf) {
        String deploymentName = serviceConf.getId();
        String serviceName = serviceConf.getId();

        String serviceYaml = createServiceYaml(serviceName, 80, "app", deploymentName);
        String deploymentYaml = createDeploymentYaml(
                deploymentName, serviceConf.getModelCheckpointDir(), serviceConf.getParams()
        );

        return mergeK8sYamls(deploymentYaml, serviceYaml);
    }
}
