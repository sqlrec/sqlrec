package com.sqlrec.common.model;

public class CheckpointInfo {
    private String modelName;
    private String checkpointName;

    public CheckpointInfo(String modelName, String checkpointName) {
        this.modelName = modelName;
        this.checkpointName = checkpointName;
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
}
