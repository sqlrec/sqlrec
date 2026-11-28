package com.sqlrec.entity;

import com.sqlrec.common.utils.JsonUtils;

import java.util.List;

public class SqlFunction {
    private String name;
    private String sqlList;
    private long createdAt;
    private long updatedAt;

    public static SqlFunction create(String name, List<String> sqlList) {
        SqlFunction function = new SqlFunction();
        function.name = name;
        function.sqlList = JsonUtils.toJson(sqlList);
        function.createdAt = System.currentTimeMillis();
        function.updatedAt = function.createdAt;
        return function;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSqlList() {
        return sqlList;
    }

    public void setSqlList(String sqlList) {
        this.sqlList = sqlList;
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
