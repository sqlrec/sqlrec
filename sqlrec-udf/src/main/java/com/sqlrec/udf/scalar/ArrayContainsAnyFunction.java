package com.sqlrec.udf.scalar;

import java.util.List;

public class ArrayContainsAnyFunction {
    public static Boolean evaluate(List<?> list, List<?> elements) {
        if (list == null || elements == null) {
            return null;
        }
        for (Object element : elements) {
            if (list.contains(element)) {
                return true;
            }
        }
        return false;
    }
}
