package com.sqlrec.udf.scalar;

import java.util.List;

public class ArrayContainsFunction {
    public static Boolean evaluate(List<?> list, Object element) {
        if (list == null || element == null) {
            return null;
        }
        return list.contains(element);
    }
}
