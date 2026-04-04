package com.sqlrec.common.schema;

import org.apache.calcite.rex.RexNode;

import java.util.List;

public abstract class SqlRecVectorTable extends SqlRecKvTable {
    
    public abstract List<String> getFieldNames();

    public List<Object[]> searchByEmbeddingWithScore(
            Object[] leftValue,
            List<Float> embedding,
            String annFieldName,
            RexNode filterCondition,
            int limit,
            List<Integer> projectColumns) {
        throw new UnsupportedOperationException("searchByEmbeddingWithScore not support");
    }
}
