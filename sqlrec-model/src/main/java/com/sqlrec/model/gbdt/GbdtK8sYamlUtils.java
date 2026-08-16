package com.sqlrec.model.gbdt;

import com.sqlrec.common.model.ServiceConf;
import com.sqlrec.model.common.K8sYamlBuilder;
import com.sqlrec.model.common.ModelConfigBase;
import com.sqlrec.model.gbdt.PipelineConfigUtils.ModelType;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.fabric8.kubernetes.client.utils.Serialization;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Kubernetes YAML generation for GBDT (CatBoost / LightGBM) train, export and service.
 *
 * <p>Training runs as a single-replica K8s Job (no distributed mode). The container command runs
 * the GBDT Python entry points and the image is {@code sqlrec/gbdt}. Common YAML primitives
 * (ConfigMap / Service / Deployment / resource requirements / YAML merge / service URL) are
 * inherited from {@link K8sYamlBuilder}.
 */
public class GbdtK8sYamlUtils extends K8sYamlBuilder {

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

        return createDeploymentYaml(
                deployName,
                "gbdt-service",
                image,
                List.of("bash", "-c", serveShell),
                null,
                params
        );
    }

    public static String genJobYaml(String pipelineConfig, String shell, String id, Map<String, String> params) {
        String configMapName = id + "-cm";
        String jobName = id + "-job";

        String configMapYaml = createConfigMapYaml(
                configMapName,
                new TreeMap<>() {{
                    put(ModelConfigBase.PIPELINE_CONFIG_NAME, pipelineConfig);
                    put(ModelConfigBase.START_SHELL_NAME, shell);
                }}
        );

        String jobYaml = createJobYaml(jobName, configMapName, params);

        return mergeK8sYamls(configMapYaml, jobYaml);
    }

    public static String getServiceK8sYaml(ModelType modelType, ServiceConf serviceConf) {
        String deploymentName = serviceConf.getId();
        String serviceName = serviceConf.getId();

        String serviceYaml = createServiceYaml(serviceName, 80, "app", deploymentName);
        String deploymentYaml = createDeploymentYaml(
                deploymentName, serviceConf.getModelCheckpointDir(), modelType, serviceConf.getParams()
        );

        return mergeK8sYamls(deploymentYaml, serviceYaml);
    }
}
