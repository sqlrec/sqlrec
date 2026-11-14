package com.sqlrec.udf.table;

import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AddColFunction {
    public CacheTable evaluate(CacheTable input, String colName, String value) {
        return addColumn(input, colName, value, SqlTypeName.VARCHAR);
    }

    public CacheTable evaluate(CacheTable input, String colName, Integer value) {
        return addColumn(input, colName, value, SqlTypeName.INTEGER);
    }

    public CacheTable evaluate(CacheTable input, String colName, Long value) {
        return addColumn(input, colName, value, SqlTypeName.BIGINT);
    }

    public CacheTable evaluate(CacheTable input, String colName, Double value) {
        return addColumn(input, colName, value, SqlTypeName.DOUBLE);
    }

    public CacheTable evaluate(CacheTable input, String colName, Boolean value) {
        return addColumn(input, colName, value, SqlTypeName.BOOLEAN);
    }

    private CacheTable addColumn(CacheTable input, String colName, Object value, SqlTypeName type) {
        if (colName == null || colName.isEmpty()) {
            throw new IllegalArgumentException("col name is empty");
        }

        List<RelDataTypeField> dataFields = input.getDataFields();
        if (DataTypeUtils.findFieldIndex(dataFields, colName) != -1) {
            throw new IllegalArgumentException("col name already exists");
        }

        List<RelDataTypeField> newDataFields = new ArrayList<>(dataFields);
        newDataFields.add(DataTypeUtils.getRelDataTypeField(colName, dataFields.size(), type));

        Enumerable<Object[]> enumerable = input.scan(null);
        List<Object[]> newData = new ArrayList<>();
        if (enumerable != null) {
            for (Object[] data : enumerable) {
                Object[] newDataRow = Arrays.copyOf(data, data.length + 1, Object[].class);
                newDataRow[data.length] = value;
                newData.add(newDataRow);
            }
        }

        return new CacheTable("output", Linq4j.asEnumerable(newData), newDataFields);
    }
}
