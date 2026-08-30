package com.sqlrec.common.model;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.schema.FieldSchema;
import java.util.List;

public interface ModelController {
    String getModelName();

    List<FieldSchema> getOutputFields(ModelConf model);

    // return null when model is valid
    String checkModel(ModelConf model);

    // return model train k8s yaml
    String genModelTrainK8sYaml(ModelConf model, ModelTrainConf trainConf);

    // Most trainable backends consume an ON data source. Snapshot-based backends may opt out.
    default boolean requiresTrainingData() {
        return true;
    }

    // return export checkpoint names (one export command may generate multiple partitions)
    List<String> getExportCheckpoints(ModelExportConf exportConf);

    String getExportCleanPath(ModelExportConf exportConf);

    // return model export k8s yaml
    String genModelExportK8sYaml(ModelConf model, ModelExportConf exportConf);

    // return service url
    String getServiceUrl(ModelConf model, ServiceConf serviceConf);

    // return service k8s yaml
    String getServiceK8sYaml(ModelConf model, ServiceConf serviceConf);

    // Validate whether the given checkpoint type can be used to serve the model.
    // Return null when valid, or an error message otherwise.
    default String validateServiceCheckpointType(String checkpointType) {
        if (!Consts.CHECKPOINT_TYPE_EXPORT.equals(checkpointType)) {
            return "service only supports export checkpoint";
        }
        return null;
    }
}
