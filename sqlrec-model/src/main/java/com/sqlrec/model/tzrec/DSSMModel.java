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
        Map<String, String> params = model.getParams();
        String userFeatures = params != null ? params.get(Config.USER_FEATURES.getKey()) : null;
        String itemFeatures = params != null ? params.get(Config.ITEM_FEATURES.getKey()) : null;

        if ((userFeatures == null || userFeatures.isEmpty()) && (itemFeatures == null || itemFeatures.isEmpty())) {
            return "At least one of user_features or item_features is required for DSSM model";
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
