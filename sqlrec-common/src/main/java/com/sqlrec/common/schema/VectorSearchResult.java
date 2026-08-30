package com.sqlrec.common.schema;

import java.util.Objects;

/** One matched table row and its vector similarity score. */
public final class VectorSearchResult {
    private final Object[] row;
    private final double score;

    public VectorSearchResult(Object[] row, double score) {
        this.row = Objects.requireNonNull(row, "row");
        this.score = score;
    }

    public Object[] getRow() {
        return row;
    }

    public double getScore() {
        return score;
    }
}
