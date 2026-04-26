package com.sqlrec.udf.table;

import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.schema.CacheTable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

public class SetVariablesFunction {
    public CacheTable evaluate(ExecuteContext context, CacheTable input) {
        if (context == null) {
            throw new IllegalArgumentException("context cannot be null");
        }
        if (input == null) {
            throw new IllegalArgumentException("input table cannot be null");
        }

        List<RelDataTypeField> dataFields = input.getDataFields();
        if (dataFields.size() != 2) {
            throw new IllegalArgumentException("input table must have exactly 2 columns");
        }

        RelDataTypeField firstField = dataFields.get(0);
        RelDataTypeField secondField = dataFields.get(1);

        if (firstField.getType().getSqlTypeName() != SqlTypeName.VARCHAR &&
            firstField.getType().getSqlTypeName() != SqlTypeName.CHAR) {
            throw new IllegalArgumentException("first column must be string type");
        }

        if (secondField.getType().getSqlTypeName() != SqlTypeName.VARCHAR &&
            secondField.getType().getSqlTypeName() != SqlTypeName.CHAR) {
            throw new IllegalArgumentException("second column must be string type");
        }

        Enumerable<Object[]> enumerable = input.scan(null);
        if (enumerable != null) {
            for (Object[] row : enumerable) {
                String key = row[0] == null ? null : row[0].toString();
                String value = row[1] == null ? null : row[1].toString();
                context.setVariable(key, value);
            }
        }

        return input;
    }
}
