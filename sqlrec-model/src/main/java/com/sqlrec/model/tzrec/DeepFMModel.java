package com.sqlrec.model.tzrec;

import com.sqlrec.common.model.ModelConf;
import com.sqlrec.common.model.ModelTrainConf;
import com.sqlrec.common.model.ModelExportConf;
import java.util.Map;

/** Explicit DeepFM backend; legacy WideAndDeep checkpoints retain their saved architecture. */
public class DeepFMModel extends WideAndDeepModel {
    @Override
    protected String getModelNameSuffix() {
        return "deepfm";
    }

    @Override
    protected String generateTrainConfig(ModelConf model, ModelTrainConf trainConf) {
        return super.generateTrainConfig(PipelineConfigUtils.effectiveModel(model, Map.of("model", "tzrec.deepfm")), trainConf);
    }

    @Override
    protected String generateExportConfig(ModelConf model, ModelExportConf exportConf) {
        return super.generateExportConfig(PipelineConfigUtils.effectiveModel(model, Map.of("model", "tzrec.deepfm")), exportConf);
    }
}
