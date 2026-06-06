package com.sqlrec.common.schema;

import org.apache.calcite.schema.impl.AbstractTable;

public abstract class SqlRecTable extends AbstractTable {
    protected String name = "";
    protected String createSql = "";

    public String getTableName() {
        return name;
    }

    public void setTableName(String name) {
        this.name = name;
    }

    public String getCreateSql() {
        return createSql;
    }

    public void setCreateSql(String createSql) {
        this.createSql = createSql;
    }
}
