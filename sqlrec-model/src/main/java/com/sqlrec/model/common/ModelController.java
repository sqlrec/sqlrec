package com.sqlrec.model.common;

public interface ModelController {
    String getModelName();

    // return null when model is valid
    String checkModel(ModelConfig model);

    // return model train k8s yaml
    String genModelTrainK8sYaml(ModelConfig model, ModelTrainConf trainConf);
}
