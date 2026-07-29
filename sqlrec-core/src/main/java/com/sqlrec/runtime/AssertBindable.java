package com.sqlrec.runtime;

import com.sqlrec.common.runtime.ExecuteContext;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Executes a SELECT query and asserts that every returned field is a boolean
 * value equal to {@code true}. If the query returns no rows, returns a non-boolean
 * value, or returns any {@code false}/{@code null} value, a RuntimeException is thrown.
 *
 * <p>The assert operator is not parallelizable: it must execute in isolation so that
 * its assertion result is observed before any dependent statement runs.
 */
public class AssertBindable extends BindableInterface {
    private final BindableInterface selectBindable;

    public AssertBindable(BindableInterface selectBindable) {
        this.selectBindable = selectBindable;

        List<RelDataTypeField> fields = selectBindable.getReturnDataFields();
        if (fields == null || fields.isEmpty()) {
            throw new RuntimeException("assert select must return at least one boolean column");
        }
        for (RelDataTypeField field : fields) {
            if (field.getType().getSqlTypeName() != SqlTypeName.BOOLEAN) {
                throw new RuntimeException("assert select must return boolean columns, but column '"
                        + field.getName() + "' is of type " + field.getType().getSqlTypeName());
            }
        }
    }

    @Override
    public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
        Enumerable<Object[]> result = selectBindable.bind(schema, context);
        List<Object[]> rows = new ArrayList<>();
        for (Object[] row : result) {
            rows.add(row);
        }

        if (rows.isEmpty()) {
            throw new RuntimeException("assert failed: select returned no rows");
        }

        for (int i = 0; i < rows.size(); i++) {
            Object[] row = rows.get(i);
            for (int j = 0; j < row.length; j++) {
                Object value = row[j];
                if (value == null) {
                    throw new RuntimeException("assert failed: row " + i + " column " + j
                            + " returned null");
                }
                if (!(value instanceof Boolean)) {
                    throw new RuntimeException("assert failed: row " + i + " column " + j
                            + " returned non-boolean value: " + value);
                }
                if (!(Boolean) value) {
                    throw new RuntimeException("assert failed: row " + i + " column " + j
                            + " returned false");
                }
            }
        }

        return null;
    }

    @Override
    public List<RelDataTypeField> getReturnDataFields() {
        return null;
    }

    @Override
    public boolean isParallelizable() {
        return false;
    }

    @Override
    public Set<String> getReadTables() {
        return selectBindable.getReadTables();
    }

    @Override
    public Set<String> getWriteTables() {
        return Set.of();
    }

    public BindableInterface getSelectBindable() {
        return selectBindable;
    }
}
