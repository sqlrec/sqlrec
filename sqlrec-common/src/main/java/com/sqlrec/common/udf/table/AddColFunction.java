package com.sqlrec.common.udf.table;

import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;

public class AddColFunction {
    public CacheTable eval(CacheTable input, String colName, String value) {
        if (colName == null || colName.isEmpty()) {
            throw new IllegalArgumentException("col name is empty");
        }

        List<RelDataTypeField> dataFields = input.getDataFields();
        for (RelDataTypeField field : dataFields) {
            if (field.getName().equalsIgnoreCase(colName)) {
                throw new IllegalArgumentException("col name already exists");
            }
        }

        List<RelDataTypeField> newDataFields = new ArrayList<>(dataFields);
        newDataFields.add(DataTypeUtils.getRelDataTypeField(colName, dataFields.size(), SqlTypeName.VARCHAR));

        Enumerable<Object[]> enumerable = input.scan(null);
        List<Object[]> newData = new ArrayList<>();
        if (enumerable != null) {
            for (Object[] data : enumerable) {
                Object[] newDataRow = new Object[data.length + 1];
                System.arraycopy(data, 0, newDataRow, 0, data.length);
                newDataRow[data.length] = value;
                newData.add(newDataRow);
            }
        }

        return new CacheTable("output", Linq4j.asEnumerable(newData), newDataFields);
    }
}
