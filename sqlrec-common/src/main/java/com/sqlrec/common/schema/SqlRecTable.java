package com.sqlrec.common.schema;

import org.apache.calcite.schema.impl.AbstractTable;

public abstract class SqlRecTable extends AbstractTable {
    protected String name = "";

    public String getTableName() {
        return name;
    }

    public void setTableName(String name) {
        this.name = name;
    }
}
