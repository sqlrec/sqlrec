package com.sqlrec.model.tzrec;

import com.sqlrec.common.model.ModelConf;
import com.sqlrec.common.model.ModelExportConf;
import com.sqlrec.common.model.ModelTrainConf;
import com.sqlrec.common.schema.FieldSchema;

import java.util.List;
import java.util.Map;

/** Multi-target ranking with one service and an ordered output for every task. */
public class MMoEModel extends TzrecModelBase {
    @Override
    protected String getModelNameSuffix() {
        return "mmoe";
    }

    @Override
    public List<FieldSchema> getOutputFields(ModelConf model) {
        return MultiTaskOptions.parse(model).stream().map(task -> new FieldSchema(task.outputName(), "FLOAT")).toList();
    }

    @Override
    public String checkModel(ModelConf model) {
        return FeatureOptions.validate(model, false, false, true);
    }

    @Override
    protected void checkOverrides(ModelConf model, Map<String, String> overrides) {
        if (overrides == null || overrides.isEmpty()) return;
        if (overrides.containsKey("model") && !getModelName().equals(overrides.get("model"))) {
            throw new IllegalArgumentException("Operation cannot change the model type");
        }
        ModelConf effective = PipelineConfigUtils.effectiveModel(model, overrides);
        String error = checkModel(effective);
        if (error != null) throw new IllegalArgumentException(error);
        if (!MultiTaskPipelineConfigUtils.modelConfig(model).equals(MultiTaskPipelineConfigUtils.modelConfig(effective))
                || !PipelineConfigUtils.generateFeatureConfigs(model).equals(PipelineConfigUtils.generateFeatureConfigs(effective))) {
            throw new IllegalArgumentException("MMoE task, network and feature configuration cannot be changed by operation overrides; create a new model");
        }
    }

    @Override
    protected String generateTrainConfig(ModelConf model, ModelTrainConf train) {
        return MultiTaskPipelineConfigUtils.train(model, train);
    }

    @Override
    protected String generateExportConfig(ModelConf model, ModelExportConf export) {
        return MultiTaskPipelineConfigUtils.export(model, export);
    }

    @Override
    public List<String> getExportCheckpoints(ModelExportConf export) {
        return List.of(export.getCheckpointName() + "_export");
    }

    @Override
    public String getExportCleanPath(ModelExportConf export) {
        return null;
    }
}
