package com.sqlrec.common.schema;

import org.apache.calcite.rex.RexNode;

import java.util.List;
import java.util.Objects;

/** Describes one vector lookup for one row from the left input. */
public final class VectorSearchRequest {
    private final Object[] leftRow;
    private final List<Float> embedding;
    private final String vectorField;
    private final RexNode filterCondition;
    private final int topK;

    public VectorSearchRequest(
            Object[] leftRow,
            List<Float> embedding,
            String vectorField,
            RexNode filterCondition,
            int topK) {
        this.leftRow = Objects.requireNonNull(leftRow, "leftRow");
        this.embedding = Objects.requireNonNull(embedding, "embedding");
        this.vectorField = Objects.requireNonNull(vectorField, "vectorField");
        this.filterCondition = filterCondition;
        if (topK <= 0) {
            throw new IllegalArgumentException("topK must be greater than zero");
        }
        this.topK = topK;
    }

    public Object[] getLeftRow() {
        return leftRow;
    }

    public List<Float> getEmbedding() {
        return embedding;
    }

    public String getVectorField() {
        return vectorField;
    }

    public RexNode getFilterCondition() {
        return filterCondition;
    }

    public int getTopK() {
        return topK;
    }
}
