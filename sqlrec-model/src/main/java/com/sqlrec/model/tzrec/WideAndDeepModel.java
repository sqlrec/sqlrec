package com.sqlrec.model.tzrec;

import com.sqlrec.common.config.ModelConfigs;
import com.sqlrec.common.model.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WideAndDeepModel implements ModelController {
    @Override
    public String getModelName() {
        return "torch_easy_rec.wide_and_deep";
    }

    @Override
    public String checkModel(ModelConfig model) {
        return null;
    }

    @Override
    public String genModelTrainK8sYaml(ModelConfig model, ModelTrainConf trainConf) {
        String pipelineConfig = PipelineConfigUtils.generateWideAndDeepTrainConfig(model, trainConf);
        String shell = ShellUtils.genTrainModelShell(model, trainConf);
        return genJobYaml(model, pipelineConfig, shell, trainConf.id, trainConf.params);
    }

    @Override
    public List<String> getExportCheckpoints(ModelExportConf exportConf) {
        List<String> exportCheckpointNames = new ArrayList<>();
        String exportBaseName = exportConf.checkpointName + "_export";
        exportCheckpointNames.add(exportBaseName);
        return exportCheckpointNames;
    }

    @Override
    public String genModelExportK8sYaml(ModelConfig model, ModelExportConf exportConf) {
        String exportDir = exportConf.baseModelDir + "_export";
        String pipelineConfig = PipelineConfigUtils.generateWideAndDeepExportConfig(model, exportConf);
        String shell = ShellUtils.genExportModelShell(model, exportConf, exportDir);
        return genJobYaml(model, pipelineConfig, shell, exportConf.id, exportConf.params);
    }

    @Override
    public String getServiceUrl(ModelConfig model, ServiceConfig serviceConf) {
        String namespace = ModelConfigs.NAMESPACE.getValue(serviceConf.params);
        return "http://" + serviceConf.serviceName + "." + namespace + ".svc.cluster.local:80/predict";
    }

    @Override
    public String getServiceK8sYaml(ModelConfig model, ServiceConfig serviceConf) {
        String deploymentName = serviceConf.id;
        String serviceName = serviceConf.id;

        String serviceYaml = K8sYamlUtils.createServiceYaml(serviceName, 80, "app", deploymentName);
        String deploymentYaml = K8sYamlUtils.createDeploymentYaml(
                deploymentName, serviceConf.modelCheckpointDir, serviceConf.params
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