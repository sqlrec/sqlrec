package com.sqlrec.model.tzrec;

import com.sqlrec.model.common.ModelConfig;
import com.sqlrec.model.common.ModelController;
import com.sqlrec.model.common.ModelTrainConf;

import java.util.HashMap;

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
        String pipelineConfig = PipelineConfigUtils.generateTrainConfig(model, trainConf);
        String shell = ShellUtils.genTrainModelShell(model, trainConf);

        String configMapName = trainConf.id + "-cm";
        String jobName = trainConf.id + "-job";
        String serviceName = jobName + "-headless";
        int nnodes = Config.NNODES.getValue(trainConf.params);
        int nprocPerNode = Config.NPROC_PER_NODE.getValue(trainConf.params);
        int masterPort = Config.MASTER_PORT.getValue(trainConf.params);

        String configMapYaml = K8sYamlUtils.createConfigMapYaml(
                configMapName,
                new HashMap<String, String>() {{
                    put(Config.PIPELINE_CONFIG_NAME, pipelineConfig);
                    put(Config.START_SHELL_NAME, shell);
                }}
        );
        
        String serviceYaml = K8sYamlUtils.createHeadlessServiceYaml(jobName, serviceName, masterPort);
        
        String jobYaml = K8sYamlUtils.createJobYaml(
                jobName, configMapName, serviceName, nnodes, nprocPerNode, masterPort
        );

        return K8sYamlUtils.mergeK8sYamls(configMapYaml, serviceYaml, jobYaml);
    }
}