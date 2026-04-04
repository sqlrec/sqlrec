package com.sqlrec.common.model;

import com.sqlrec.common.schema.FieldSchema;

import java.util.List;
import java.util.Map;

public class ModelConfig {
    private String modelName;
    private List<FieldSchema> inputFields;
    private Map<String, String> params;
    private String ddl;
    private String path;

    public String getModelName() {
        return modelName;
    }

    public void setModelName(String modelName) {
        this.modelName = modelName;
    }

    public List<FieldSchema> getInputFields() {
        return inputFields;
    }

    public void setInputFields(List<FieldSchema> inputFields) {
        this.inputFields = inputFields;
    }

    public Map<String, String> getParams() {
        return params;
    }

    public void setParams(Map<String, String> params) {
        this.params = params;
    }

    public String getDdl() {
        return ddl;
    }

    public void setDdl(String ddl) {
        this.ddl = ddl;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }
}
