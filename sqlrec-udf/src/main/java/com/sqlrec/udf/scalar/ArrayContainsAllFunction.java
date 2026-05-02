package com.sqlrec.udf.scalar;

import java.util.List;

public class ArrayContainsAllFunction {
    public static Boolean evaluate(List<?> list, List<?> elements) {
        if (list == null || elements == null) {
            return null;
        }
        return list.containsAll(elements);
    }
}
