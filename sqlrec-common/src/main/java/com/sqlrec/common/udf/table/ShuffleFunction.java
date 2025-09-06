package com.sqlrec.common.udf.table;

import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.schema.TableFunction;
import com.sqlrec.common.utils.DataTypeUtils;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ShuffleFunction implements TableFunction {
    @Override
    public CacheTable eval(CacheTable input) {
        List<RelDataTypeField> dataFields = input.getDataFields();
        Enumerable<Object[]> enumerable = input.scan(null);

        List<RelDataTypeField> newDataFields = new ArrayList<>(dataFields);
        newDataFields.add(DataTypeUtils.getRelDataTypeField("origin_index", dataFields.size(), SqlTypeName.INTEGER));

        List<Object[]> newData = new ArrayList<>();
        if (enumerable != null) {
            int index = 0;
            for (Object[] data : enumerable) {
                Object[] newDataRow = new Object[data.length + 1];
                System.arraycopy(data, 0, newDataRow, 0, data.length);
                newDataRow[data.length] = index++;
                newData.add(newDataRow);
            }
        }
        Collections.shuffle(newData);

        return new CacheTable("output", Linq4j.asEnumerable(newData), newDataFields);
    }
}
