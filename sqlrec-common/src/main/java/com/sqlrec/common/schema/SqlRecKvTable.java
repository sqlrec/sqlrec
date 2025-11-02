package com.sqlrec.common.schema;

import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.ModifiableTable;

import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class SqlRecKvTable extends SqlRecTable implements ModifiableTable, FilterableTable {
    public int getPrimaryKeyIndex() {
        throw new UnsupportedOperationException("getPrimaryKeyIndex not support");
    }

    public Map<Object, List<Object[]>> getByPrimaryKey(Set<Object> keySet) {
        throw new UnsupportedOperationException("getByPrimaryKey not support");
    }

    public boolean onlyFilterByPrimaryKey() {
        return true;
    }
}
