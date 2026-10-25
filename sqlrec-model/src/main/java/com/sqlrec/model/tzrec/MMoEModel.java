package com.sqlrec.model.tzrec;

import com.sqlrec.common.model.ModelConf;
import com.sqlrec.common.model.ModelExportConf;
import com.sqlrec.common.model.ModelTrainConf;
import com.sqlrec.common.schema.FieldSchema;

import java.util.List;

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
