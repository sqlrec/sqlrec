package com.sqlrec.frontend.cli;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

/**
 * Minimal test-only {@link RelDataTypeField} implementation.
 * Only {@link #getName()} and {@link #getIndex()} return meaningful values;
 * {@link #getType()} returns {@code null} and {@link #isDynamicStar()} returns
 * {@code false}, which is sufficient for the {@link SqlOutputFormatter} /
 * {@code DataTransformUtils} code paths exercised by the unit tests.
 */
final class FakeRelDataTypeField implements RelDataTypeField {

    private final String name;
    private final int index;

    FakeRelDataTypeField(String name, int index) {
        this.name = name;
        this.index = index;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public RelDataType getType() {
        return null;
    }

    @Override
    public int getIndex() {
        return index;
    }

    @Override
    public boolean isDynamicStar() {
        return false;
    }

    // ---- Map.Entry<String, RelDataType> ----

    @Override
    public String getKey() {
        return name;
    }

    @Override
    public RelDataType getValue() {
        return null;
    }

    @Override
    public RelDataType setValue(RelDataType value) {
        throw new UnsupportedOperationException();
    }
}
