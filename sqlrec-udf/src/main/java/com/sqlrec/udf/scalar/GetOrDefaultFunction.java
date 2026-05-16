package com.sqlrec.udf.scalar;

import com.sqlrec.common.runtime.SqlRecDataContext;
import org.apache.calcite.DataContext;

public class GetOrDefaultFunction {
    public static String evaluate(DataContext context, String key, String defaultValue) {
        if (!(context instanceof SqlRecDataContext)) {
            throw new IllegalArgumentException("context must be SqlRecDataContext");
        }
        SqlRecDataContext sqlRecDataContext = (SqlRecDataContext) context;
        String value = sqlRecDataContext.getVariable(key);
        return value != null ? value : defaultValue;
    }
}
