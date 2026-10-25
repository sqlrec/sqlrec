package com.sqlrec.model.tzrec;

import com.sqlrec.common.model.ModelConf;
import com.sqlrec.common.model.ModelExportConf;
import com.sqlrec.common.model.ModelTrainConf;
import com.sqlrec.common.schema.FieldSchema;

import java.util.List;

/** Native booster/light training with a single light prediction service. */
public class RocketLaunchingModel extends TzrecModelBase {
    @Override
    protected String getModelNameSuffix() { return "rocket_launching"; }

    @Override
    public List<FieldSchema> getOutputFields(ModelConf model) {
        return List.of(new FieldSchema("probs_light", "FLOAT"));
    }

    @Override
    public String checkModel(ModelConf model) {
        String error = FeatureOptions.validateDeepOnly(model);
        if (error != null) return error;
        try {
            RocketLaunchingOptions.validate(model);
            return null;
        } catch (IllegalArgumentException e) {
            return "Invalid RocketLaunching configuration: " + e.getMessage();
        }
    }

    @Override
    protected String generateTrainConfig(ModelConf model, ModelTrainConf train) {
        return RocketLaunchingPipelineConfigUtils.train(model, train);
    }

    @Override
    protected String generateExportConfig(ModelConf model, ModelExportConf export) {
        return RocketLaunchingPipelineConfigUtils.export(model, export);
    }

    @Override
    public List<String> getExportCheckpoints(ModelExportConf export) {
        return List.of(export.getCheckpointName() + "_export");
    }

    @Override
    public String getExportCleanPath(ModelExportConf export) { return null; }
}
