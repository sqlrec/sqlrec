package com.sqlrec.common.schema;

import org.apache.calcite.schema.impl.AbstractTable;

import java.util.List;

public abstract class SqlRecTable extends AbstractTable {
    public enum SqlRecTableType {
        BATCH,
        KV,
        MQ,
        VECTOR,
        MEMORY
    }

    public abstract SqlRecTableType getSqlRecTableType();

    public int getPrimaryKeyIndex() {
        throw new UnsupportedOperationException("getPrimaryKeyIndex not support");
    }

    public List<Object[]> getByPrimaryKey(Object key) {
        throw new UnsupportedOperationException("getByPrimaryKey not support");
    }
}
