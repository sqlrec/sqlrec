package com.sqlrec.model.tzrec;

import com.sqlrec.model.common.Model;
import com.sqlrec.model.common.ModelController;
import com.sqlrec.model.common.ModelTrainConf;

import java.util.HashMap;

public class WideAndDeepModel implements ModelController {
    @Override
    public String getModelName() {
        return "torch_easy_rec.wide_and_deep";
    }

    @Override
    public String checkModel(Model model) {
        return null;
    }

    @Override
    public String genModelTrainK8sYaml(Model model, ModelTrainConf trainConf) {
        String pipelineConfig = PipelineConfigUtils.generateTrainConfig(model, trainConf);
        String shell = ShellUtils.genTrainModelShell(model, trainConf);

        String configMapName = trainConf.name + "-cm";
        String jobName = trainConf.name + "-job";

        String configMapYaml = K8sYamlUtils.createConfigMapYaml(
                configMapName,
                new HashMap<String, String>() {{
                    put(Config.PIPELINE_CONFIG_NAME, pipelineConfig);
                    put(Config.START_SHELL_NAME, shell);
                }}
        );
        String jobYaml = K8sYamlUtils.createJobYaml(
                jobName, configMapName
        );

        return K8sYamlUtils.mergeK8sYamls(configMapYaml, jobYaml);
    }
}