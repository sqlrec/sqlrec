package com.sqlrec.entity;

public class Checkpoint {
    private String modelName;
    private String checkpointName;
    private String modelDdl;
    private String ddl;
    private String yaml;
    private String checkpointType;
    private String status;
    private long createdAt;
    private long updatedAt;

    public String getModelDdl() {
        return modelDdl;
    }

    public void setModelDdl(String modelDdl) {
        this.modelDdl = modelDdl;
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

    public String getDdl() {
        return ddl;
    }

    public void setDdl(String ddl) {
        this.ddl = ddl;
    }

    public String getYaml() {
        return yaml;
    }

    public void setYaml(String yaml) {
        this.yaml = yaml;
    }

    public String getCheckpointType() {
        return checkpointType;
    }

    public void setCheckpointType(String checkpointType) {
        this.checkpointType = checkpointType;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(long createdAt) {
        this.createdAt = createdAt;
    }

    public long getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(long updatedAt) {
        this.updatedAt = updatedAt;
    }
}