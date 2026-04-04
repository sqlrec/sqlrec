package com.sqlrec.model.tzrec;

import com.sqlrec.common.config.ModelConfigs;
import com.sqlrec.common.model.*;
import com.sqlrec.common.schema.FieldSchema;

import java.util.*;

public class WideAndDeepModel implements ModelController {
    @Override
    public String getModelName() {
        return "tzrec.wide_and_deep";
    }

    @Override
    public List<FieldSchema> getOutputFields(ModelConfig model) {
        return Collections.singletonList(new FieldSchema("probs", "FLOAT"));
    }

    @Override
    public String checkModel(ModelConfig model) {
        return null;
    }

    @Override
    public String genModelTrainK8sYaml(ModelConfig model, ModelTrainConf trainConf) {
        String pipelineConfig = PipelineConfigUtils.generateWideAndDeepTrainConfig(model, trainConf);
        String shell = ShellUtils.genTrainModelShell(model, trainConf);
        return genJobYaml(model, pipelineConfig, shell, trainConf.getId(), trainConf.getParams());
    }

    @Override
    public List<String> getExportCheckpoints(ModelExportConf exportConf) {
        List<String> exportCheckpointNames = new ArrayList<>();
        String exportBaseName = exportConf.getCheckpointName() + "_export";
        exportCheckpointNames.add(exportBaseName);
        return exportCheckpointNames;
    }

    @Override
    public String genModelExportK8sYaml(ModelConfig model, ModelExportConf exportConf) {
        String exportDir = exportConf.getBaseModelDir() + "_export";
        String pipelineConfig = PipelineConfigUtils.generateWideAndDeepExportConfig(model, exportConf);
        String shell = ShellUtils.genExportModelShell(model, exportConf, exportDir);
        return genJobYaml(model, pipelineConfig, shell, exportConf.getId(), exportConf.getParams());
    }

    @Override
    public String getServiceUrl(ModelConfig model, ServiceConfig serviceConf) {
        String namespace = ModelConfigs.NAMESPACE.getValue(serviceConf.getParams());
        return "http://" + serviceConf.getId() + "." + namespace + ".svc.cluster.local:80/predict";
    }

    @Override
    public String getServiceK8sYaml(ModelConfig model, ServiceConfig serviceConf) {
        String deploymentName = serviceConf.getId();
        String serviceName = serviceConf.getId();

        String serviceYaml = K8sYamlUtils.createServiceYaml(serviceName, 80, "app", deploymentName);
        String deploymentYaml = K8sYamlUtils.createDeploymentYaml(
                deploymentName, serviceConf.getModelCheckpointDir(), serviceConf.getParams()
        );

        return K8sYamlUtils.mergeK8sYamls(deploymentYaml, serviceYaml);
    }

    private String genJobYaml(ModelConfig model, String pipelineConfig, String shell, String id, Map<String, String> params) {
        String configMapName = id + "-cm";
        String jobName = id + "-job";
        String serviceName = jobName + "-headless";
        int nnodes = Config.NNODES.getValue(params);
        int nprocPerNode = Config.NPROC_PER_NODE.getValue(params);
        int masterPort = Config.MASTER_PORT.getValue(params);

        String configMapYaml = K8sYamlUtils.createConfigMapYaml(
                configMapName,
                new HashMap<String, String>() {{
                    put(Config.PIPELINE_CONFIG_NAME, pipelineConfig);
                    put(Config.START_SHELL_NAME, shell);
                }}
        );

        String serviceYaml = K8sYamlUtils.createHeadlessServiceYaml(jobName, serviceName, masterPort);

        String jobYaml = K8sYamlUtils.createJobYaml(
                jobName, configMapName, serviceName, nnodes, nprocPerNode, masterPort, params
        );

        return K8sYamlUtils.mergeK8sYamls(configMapYaml, serviceYaml, jobYaml);
    }
}
