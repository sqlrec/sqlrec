package com.sqlrec.common.schema;

import java.util.List;

public abstract class SqlRecVectorTable extends SqlRecKvTable {
    public List<Object[]> searchByEmbedding(String fieldName, List<Float> embedding, int limit, List<Integer> projectColumns) {
        throw new UnsupportedOperationException("searchByEmbedding not support");
    }
}
