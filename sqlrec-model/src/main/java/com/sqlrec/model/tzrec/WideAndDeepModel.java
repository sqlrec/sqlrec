package com.sqlrec.model.tzrec;

import com.sqlrec.common.model.ModelConf;
import com.sqlrec.common.model.ModelExportConf;
import com.sqlrec.common.model.ModelTrainConf;
import com.sqlrec.common.schema.FieldSchema;

import java.util.Collections;
import java.util.List;

/**
 * WideAndDeep model based on TZRec.
 *
 * <p>Model name: {@code tzrec.wide_and_deep}. Training/export/serving YAML generation is
 * delegated to {@link TzrecModelBase}; only the WideAndDeep-specific pipeline.config, output
 * schema and export checkpoints live here.
 */
public class WideAndDeepModel extends TzrecModelBase {

    @Override
    protected String getModelNameSuffix() {
        return "wide_and_deep";
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
    protected String generateTrainConfig(ModelConf model, ModelTrainConf trainConf) {
        return PipelineConfigUtils.generateWideAndDeepTrainConfig(model, trainConf);
    }

    @Override
    protected String generateExportConfig(ModelConf model, ModelExportConf exportConf) {
        return PipelineConfigUtils.generateWideAndDeepExportConfig(model, exportConf);
    }

    @Override
    public List<String> getExportCheckpoints(ModelExportConf exportConf) {
        return Collections.singletonList(exportConf.getCheckpointName() + "_export");
    }

    @Override
    public String getExportCleanPath(ModelExportConf exportConf) {
        return null;
    }
}
