package com.sqlrec.udf.table;

import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.RowTransformUtils;
import org.apache.calcite.linq4j.Linq4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ShuffleFunction {
    public CacheTable evaluate(CacheTable input) {
        List<Object[]> newData = new ArrayList<>(RowTransformUtils.materializeRows(input));
        Collections.shuffle(newData);

        return new CacheTable("output", Linq4j.asEnumerable(newData), input.getDataFields());
    }
}
