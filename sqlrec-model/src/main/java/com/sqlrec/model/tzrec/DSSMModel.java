package com.sqlrec.model.tzrec;

import com.sqlrec.common.model.*;
import com.sqlrec.common.schema.FieldSchema;

import java.util.*;

public class DSSMModel implements ModelController {
    @Override
    public String getModelName() {
        return "tzrec.dssm";
    }

    @Override
    public List<FieldSchema> getOutputFields(ModelConfig model) {
        return Arrays.asList(
                new FieldSchema("user_tower_emb", "ARRAY<FLOAT>"),
                new FieldSchema("item_tower_emb", "ARRAY<FLOAT>")
        );
    }

    @Override
    public String checkModel(ModelConfig model) {
        Map<String, String> params = model.getParams();
        String userFeatures = params != null ? params.get(Config.USER_FEATURES.getKey()) : null;
        String itemFeatures = params != null ? params.get(Config.ITEM_FEATURES.getKey()) : null;

        if ((userFeatures == null || userFeatures.isEmpty()) && (itemFeatures == null || itemFeatures.isEmpty())) {
            return "At least one of user_features or item_features is required for DSSM model";
        }
        return null;
    }

    @Override
    public String genModelTrainK8sYaml(ModelConfig model, ModelTrainConf trainConf) {
        String pipelineConfig = PipelineConfigUtils.generateDSSMTrainConfig(model, trainConf);
        String shell = ShellUtils.genTrainModelShell(model, trainConf);
        return K8sYamlUtils.genJobYaml(pipelineConfig, shell, trainConf.getId(), trainConf.getParams());
    }

    @Override
    public List<String> getExportCheckpoints(ModelExportConf exportConf) {
        String exportBaseName = exportConf.getCheckpointName() + "_export";
        return Arrays.asList(exportBaseName + "/item", exportBaseName + "/user");
    }

    @Override
    public String getExportCleanPath(ModelExportConf exportConf) {
        return exportConf.getBaseModelDir() + "_export";
    }

    @Override
    public String genModelExportK8sYaml(ModelConfig model, ModelExportConf exportConf) {
        String exportDir = exportConf.getBaseModelDir() + "_export";
        String pipelineConfig = PipelineConfigUtils.generateDSSMExportConfig(model, exportConf);
        String shell = ShellUtils.genExportModelShell(model, exportConf, exportDir);
        return K8sYamlUtils.genJobYaml(pipelineConfig, shell, exportConf.getId(), exportConf.getParams());
    }

    @Override
    public String getServiceUrl(ModelConfig model, ServiceConfig serviceConf) {
        return K8sYamlUtils.getServiceUrl(serviceConf);
    }

    @Override
    public String getServiceK8sYaml(ModelConfig model, ServiceConfig serviceConf) {
        return K8sYamlUtils.getServiceK8sYaml(serviceConf);
    }
}
