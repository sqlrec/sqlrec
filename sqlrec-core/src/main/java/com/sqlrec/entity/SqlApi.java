package com.sqlrec.entity;

import com.sqlrec.common.utils.ResourceNames;

public class SqlApi {
    private String name;
    private String functionName;
    private long createdAt;
    private long updatedAt;

    public static SqlApi create(String name, String functionName) {
        SqlApi api = new SqlApi();
        api.name = ResourceNames.normalize(name);
        api.functionName = ResourceNames.normalize(functionName);
        api.createdAt = System.currentTimeMillis();
        api.updatedAt = api.createdAt;
        return api;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getFunctionName() {
        return functionName;
    }

    public void setFunctionName(String functionName) {
        this.functionName = functionName;
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
