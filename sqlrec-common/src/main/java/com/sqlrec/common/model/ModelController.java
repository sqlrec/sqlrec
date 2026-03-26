package com.sqlrec.common.model;

import com.sqlrec.common.schema.FieldSchema;
import java.util.List;

public interface ModelController {
    String getModelName();

    List<FieldSchema> getOutputFields(ModelConfig model);

    // return null when model is valid
    String checkModel(ModelConfig model);

    // return model train k8s yaml
    String genModelTrainK8sYaml(ModelConfig model, ModelTrainConf trainConf);

    // return export checkpoint names (one export command may generate multiple partitions)
    List<String> getExportCheckpoints(ModelExportConf exportConf);

    // return model export k8s yaml
    String genModelExportK8sYaml(ModelConfig model, ModelExportConf exportConf);

    // return service url
    String getServiceUrl(ModelConfig model, ServiceConfig serviceConf);

    // return service k8s yaml
    String getServiceK8sYaml(ModelConfig model, ServiceConfig serviceConf);
}
