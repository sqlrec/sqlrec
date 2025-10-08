package com.sqlrec.common.utils;

import java.util.ArrayList;
import java.util.List;

public class JoinUtils {
    public static List<Float> convertToFloat(Object obj) {
        if (obj instanceof List) {
            List<?> list = (List<?>) obj;
            List<Float> floatList = new ArrayList<>(list.size());
            for (Object o : list) {
                if (o instanceof Number) {
                    floatList.add(((Number) o).floatValue());
                } else {
                    throw new IllegalArgumentException("list contains non-number element");
                }
            }
            return floatList;
        }
        throw new IllegalArgumentException("obj is not list");
    }
}
