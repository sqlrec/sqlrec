package com.sqlrec.model.gbdt;

import com.sqlrec.common.model.ServiceConf;
import com.sqlrec.model.common.K8sYamlBuilder;
import com.sqlrec.model.gbdt.PipelineConfigUtils.ModelType;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

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
        return createSingleNodeJobYaml(
                jobName,
                configMapName,
                "gbdt-job",
                image,
                null,
                params
        );
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

        String configMapYaml = createPipelineConfigMapYaml(configMapName, pipelineConfig, shell);

        String jobYaml = createJobYaml(jobName, configMapName, params);

        return mergeK8sYamls(configMapYaml, jobYaml);
    }

    public static String getServiceK8sYaml(ModelType modelType, ServiceConf serviceConf) {
        String deploymentYaml = createDeploymentYaml(
                serviceConf.getId(), serviceConf.getModelCheckpointDir(), modelType, serviceConf.getParams()
        );
        return createServingResourcesYaml(serviceConf.getId(), deploymentYaml);
    }
}
