package com.sqlrec.udf.table;

import com.sqlrec.common.schema.CacheTable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ShuffleFunction {
    public CacheTable eval(CacheTable input) {
        Enumerable<Object[]> enumerable = input.scan(null);
        List<Object[]> newData = new ArrayList<>();
        if (enumerable != null) {
            for (Object[] data : enumerable) {
                newData.add(data);
            }
        }
        Collections.shuffle(newData);

        return new CacheTable("output", Linq4j.asEnumerable(newData), input.getDataFields());
    }
}
