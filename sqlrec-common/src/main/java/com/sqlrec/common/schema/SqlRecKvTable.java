package com.sqlrec.common.schema;

import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.ModifiableTable;

import java.util.List;

public abstract class SqlRecKvTable extends SqlRecTable implements ModifiableTable, FilterableTable {
    public int getPrimaryKeyIndex() {
        throw new UnsupportedOperationException("getPrimaryKeyIndex not support");
    }

    public List<Object[]> getByPrimaryKey(Object key) {
        throw new UnsupportedOperationException("getByPrimaryKey not support");
    }
}
