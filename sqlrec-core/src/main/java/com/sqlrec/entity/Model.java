package com.sqlrec.entity;

public class Model {
    private String name;
    private String ddl;
    private long createdAt;
    private long updatedAt;

    public static Model create(String name, String ddl) {
        Model model = new Model();
        model.name = name;
        model.ddl = ddl;
        model.createdAt = System.currentTimeMillis();
        model.updatedAt = model.createdAt;
        return model;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDdl() {
        return ddl;
    }

    public void setDdl(String ddl) {
        this.ddl = ddl;
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
