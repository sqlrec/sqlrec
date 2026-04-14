package com.sqlrec.udf.table;

import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class GetVariablesFunction {
    public CacheTable eval(ExecuteContext context) {
        if (context == null) {
            throw new IllegalArgumentException("context cannot be null");
        }

        Map<String, String> variables = context.getVariables();
        List<Object[]> data = new ArrayList<>();

        for (Map.Entry<String, String> entry : variables.entrySet()) {
            data.add(new Object[]{entry.getKey(), entry.getValue()});
        }

        List<RelDataTypeField> dataFields = new ArrayList<>();
        dataFields.add(DataTypeUtils.getRelDataTypeField("key", 0, SqlTypeName.VARCHAR));
        dataFields.add(DataTypeUtils.getRelDataTypeField("value", 1, SqlTypeName.VARCHAR));

        return new CacheTable("variables", Linq4j.asEnumerable(data), dataFields);
    }
}
