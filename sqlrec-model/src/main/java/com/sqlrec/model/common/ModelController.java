package com.sqlrec.model.common;

public interface ModelController {
    String getModelName();

    // return null when model is valid
    String checkModel(Model model);

    // return model train k8s yaml
    String genModelTrainK8sYaml(Model model, ModelTrainConf trainConf);
}
