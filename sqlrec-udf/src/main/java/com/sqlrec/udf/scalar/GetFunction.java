package com.sqlrec.udf.scalar;

import com.sqlrec.common.runtime.SqlRecDataContext;
import org.apache.calcite.DataContext;

public class GetFunction {
    public static String evaluate(DataContext context, String key) {
        if (!(context instanceof SqlRecDataContext)) {
            throw new IllegalArgumentException("context must be SqlRecDataContext");
        }
        SqlRecDataContext sqlRecDataContext = (SqlRecDataContext) context;
        return sqlRecDataContext.getVariable(key);
    }
}
