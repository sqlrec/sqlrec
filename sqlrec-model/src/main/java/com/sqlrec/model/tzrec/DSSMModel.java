package com.sqlrec.model.tzrec;

import com.sqlrec.common.model.ModelConf;
import com.sqlrec.common.model.ModelExportConf;
import com.sqlrec.common.model.ModelTrainConf;
import com.sqlrec.common.schema.FieldSchema;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * DSSM two-tower model based on TZRec.
 *
 * <p>Model name: {@code tzrec.dssm}. Training/export/serving YAML generation is delegated to
 * {@link TzrecModelBase}; only the DSSM-specific pipeline.config, output schema and export
 * checkpoints live here.
 */
public class DSSMModel extends TzrecModelBase {

    @Override
    protected String getModelNameSuffix() {
        return "dssm";
    }

    @Override
    public List<FieldSchema> getOutputFields(ModelConf model) {
        return Arrays.asList(
                new FieldSchema("user_tower_emb", "ARRAY<FLOAT>"),
                new FieldSchema("item_tower_emb", "ARRAY<FLOAT>")
        );
    }

    @Override
    public String checkModel(ModelConf model) {
        String error = checkRecallLoss(model.getParams());
        if (error != null) return error;
        return FeatureOptions.validate(model, true, false);
    }

    @Override
    protected void checkOverrides(ModelConf model, Map<String, String> overrides) {
        String error = checkRecallLoss(model.getParams());
        if (error == null) error = checkRecallLoss(overrides);
        if (error != null) throw new IllegalArgumentException(error);
    }

    private static String checkRecallLoss(Map<String, String> params) {
        if (params != null && params.containsKey("loss")
                && !"softmax_cross_entropy".equals(params.get("loss"))) {
            return "DSSM only supports softmax_cross_entropy recall training; remove the loss option and prepare positive pairs";
        }
        return null;
    }

    @Override
    protected String generateTrainConfig(ModelConf model, ModelTrainConf trainConf) {
        return PipelineConfigUtils.generateDSSMTrainConfig(model, trainConf);
    }

    @Override
    protected String generateExportConfig(ModelConf model, ModelExportConf exportConf) {
        return PipelineConfigUtils.generateDSSMExportConfig(model, exportConf);
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
}
