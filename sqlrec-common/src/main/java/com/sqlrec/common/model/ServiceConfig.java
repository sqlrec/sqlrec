package com.sqlrec.common.model;

import java.util.Map;

public class ServiceConfig {
    private String id;
    private String serviceName;
    private String modelName;
    private ModelConfig modelConfig;
    private String checkpointName;
    private String modelCheckpointDir;
    private Map<String, String> params;
    private String url;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public String getModelName() {
        return modelName;
    }

    public void setModelName(String modelName) {
        this.modelName = modelName;
    }

    public ModelConfig getModelConfig() {
        return modelConfig;
    }

    public void setModelConfig(ModelConfig modelConfig) {
        this.modelConfig = modelConfig;
    }

    public String getCheckpointName() {
        return checkpointName;
    }

    public void setCheckpointName(String checkpointName) {
        this.checkpointName = checkpointName;
    }

    public String getModelCheckpointDir() {
        return modelCheckpointDir;
    }

    public void setModelCheckpointDir(String modelCheckpointDir) {
        this.modelCheckpointDir = modelCheckpointDir;
    }

    public Map<String, String> getParams() {
        return params;
    }

    public void setParams(Map<String, String> params) {
        this.params = params;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }
}
