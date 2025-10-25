package com.sqlrec.entity;

import java.time.LocalDateTime;

/*
CREATE TABLE sql_function (
    name VARCHAR(255) NOT NULL PRIMARY KEY,
    sql_list text NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
);
 */
public class SqlFunction {
    private String name;
    private String sqlList;
    private long createdAt;
    private long updatedAt;

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
