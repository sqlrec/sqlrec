package com.sqlrec.udf.table;

import com.sqlrec.common.runtime.ReadonlyContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import org.apache.calcite.linq4j.Linq4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class GetVariablesFunction {
    public CacheTable evaluate(ReadonlyContext context) {
        if (context == null) {
            throw new IllegalArgumentException("context cannot be null");
        }

        Map<String, String> variables = context.getVariables();
        List<Object[]> data = new ArrayList<>();

        for (Map.Entry<String, String> entry : variables.entrySet()) {
            data.add(new Object[]{entry.getKey(), entry.getValue()});
        }

        return new CacheTable("variables", Linq4j.asEnumerable(data),
                DataTypeUtils.getStringTypeFieldList(List.of("key", "value")));
    }
}
