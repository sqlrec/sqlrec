package com.sqlrec.model.tzrec;

import com.sqlrec.common.model.*;
import com.sqlrec.common.schema.FieldSchema;

import java.util.*;

public class WideAndDeepModel implements ModelController {
    @Override
    public String getModelName() {
        return "tzrec.wide_and_deep";
    }

    @Override
    public List<FieldSchema> getOutputFields(ModelConf model) {
        return Collections.singletonList(new FieldSchema("probs", "FLOAT"));
    }

    @Override
    public String checkModel(ModelConf model) {
        return null;
    }

    @Override
    public String genModelTrainK8sYaml(ModelConf model, ModelTrainConf trainConf) {
        String pipelineConfig = PipelineConfigUtils.generateWideAndDeepTrainConfig(model, trainConf);
        String shell = ShellUtils.genTrainModelShell(model, trainConf);
        return K8sYamlUtils.genJobYaml(pipelineConfig, shell, trainConf.getId(), trainConf.getParams());
    }

    @Override
    public List<String> getExportCheckpoints(ModelExportConf exportConf) {
        return Collections.singletonList(exportConf.getCheckpointName() + "_export");
    }

    @Override
    public String getExportCleanPath(ModelExportConf exportConf) {
        return null;
    }

    @Override
    public String genModelExportK8sYaml(ModelConf model, ModelExportConf exportConf) {
        String exportDir = exportConf.getBaseModelDir() + "_export";
        String pipelineConfig = PipelineConfigUtils.generateWideAndDeepExportConfig(model, exportConf);
        String shell = ShellUtils.genExportModelShell(model, exportConf, exportDir);
        return K8sYamlUtils.genJobYaml(pipelineConfig, shell, exportConf.getId(), exportConf.getParams());
    }

    @Override
    public String getServiceUrl(ModelConf model, ServiceConf serviceConf) {
        return K8sYamlUtils.getServiceUrl(serviceConf);
    }

    @Override
    public String getServiceK8sYaml(ModelConf model, ServiceConf serviceConf) {
        return K8sYamlUtils.getServiceK8sYaml(serviceConf);
    }
}
