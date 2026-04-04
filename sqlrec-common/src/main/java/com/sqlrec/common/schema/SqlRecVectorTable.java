package com.sqlrec.common.schema;

import java.util.List;

public abstract class SqlRecVectorTable extends SqlRecKvTable {
    public List<Object[]> searchByEmbeddingWithScore(
            String fieldName,
            List<Float> embedding,
            String filterExpression,
            int limit,
            List<Integer> projectColumns) {
        throw new UnsupportedOperationException("searchByEmbeddingWithScore not support");
    }
}
