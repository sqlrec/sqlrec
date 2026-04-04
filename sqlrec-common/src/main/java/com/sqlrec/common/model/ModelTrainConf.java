package com.sqlrec.common.model;

import java.util.List;
import java.util.Map;

public class ModelTrainConf {
    private String id;
    private String modelName;
    private String checkpointName;
    private String modelDir;
    private String baseModelDir;
    private List<String> trainDataPaths;
    private Map<String, String> params;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getModelName() {
        return modelName;
    }

    public void setModelName(String modelName) {
        this.modelName = modelName;
    }

    public String getCheckpointName() {
        return checkpointName;
    }

    public void setCheckpointName(String checkpointName) {
        this.checkpointName = checkpointName;
    }

    public String getModelDir() {
        return modelDir;
    }

    public void setModelDir(String modelDir) {
        this.modelDir = modelDir;
    }

    public String getBaseModelDir() {
        return baseModelDir;
    }

    public void setBaseModelDir(String baseModelDir) {
        this.baseModelDir = baseModelDir;
    }

    public List<String> getTrainDataPaths() {
        return trainDataPaths;
    }

    public void setTrainDataPaths(List<String> trainDataPaths) {
        this.trainDataPaths = trainDataPaths;
    }

    public Map<String, String> getParams() {
        return params;
    }

    public void setParams(Map<String, String> params) {
        this.params = params;
    }
}
