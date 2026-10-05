package com.sqlrec.model.gbdt;

import com.sqlrec.common.model.ServiceConf;
import com.sqlrec.model.common.K8sYamlBuilder;
import com.sqlrec.model.gbdt.PipelineConfigUtils.ModelType;
import io.fabric8.kubernetes.api.model.ProbeBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.utils.Serialization;
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

        String yaml = createDeploymentYaml(
                deployName,
                "gbdt-service",
                image,
                List.of("bash", "-c", serveShell),
                null,
                params
        );
        Deployment deployment = Serialization.unmarshal(yaml, Deployment.class);
        var container = deployment.getSpec().getTemplate().getSpec().getContainers().get(0);
        container.setStartupProbe(new ProbeBuilder()
                .withNewHttpGet().withPath("/health").withNewPort(80).endHttpGet()
                .withPeriodSeconds(5).withTimeoutSeconds(5).withFailureThreshold(120).build());
        container.setReadinessProbe(new ProbeBuilder()
                .withNewHttpGet().withPath("/health").withNewPort(80).endHttpGet()
                .withPeriodSeconds(5).withTimeoutSeconds(5).build());
        return Serialization.asYaml(deployment);
    }

    public static String genJobYaml(String pipelineConfig, String shell, String id, Map<String, String> params) {
        String configMapName = id + "-cm";
        String jobName = id + "-job";

        String configMapYaml = createPipelineConfigMapYaml(configMapName, pipelineConfig, shell);

        String jobYaml = createJobYaml(jobName, configMapName, params);

        return mergeK8sYamls(configMapYaml, jobYaml);
    }

    public static String getServiceK8sYaml(ModelType modelType, ServiceConf serviceConf) {
        return getServiceK8sYaml(modelType, serviceConf, serviceConf.getParams());
    }

    public static String getServiceK8sYaml(ModelType modelType, ServiceConf serviceConf, Map<String, String> params) {
        String deploymentYaml = createDeploymentYaml(
                serviceConf.getId(), serviceConf.getModelCheckpointDir(), modelType, params
        );
        return createServingResourcesYaml(serviceConf.getId(), deploymentYaml);
    }
}
