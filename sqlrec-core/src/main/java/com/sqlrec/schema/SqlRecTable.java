package com.sqlrec.schema;

import org.apache.calcite.schema.impl.AbstractTable;

public abstract class SqlRecTable extends AbstractTable {
    public enum SqlRecTableType {
        BATCH,
        KV,
        MQ,
        VECTOR,
        MEMORY
    }

    public abstract SqlRecTableType getSqlRecTableType();
}
