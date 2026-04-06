package com.sqlrec.common.utils;

import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DataTransformUtils {
    public static List<Float> convertToFloatVec(Object obj) {
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

    public static List<Map<String, Object>> convertToMapList(List<Object[]> results, List<RelDataTypeField> fields) {
        List<Map<String, Object>> result = new ArrayList<>();
        for (Object[] row : results) {
            Map<String, Object> map = new LinkedHashMap<>(fields.size());
            for (int i = 0; i < fields.size(); i++) {
                RelDataTypeField field = fields.get(i);
                if (row.length > i && row[i] != null) {
                    map.put(field.getName(), row[i]);
                }
            }
            result.add(map);
        }
        return result;
    }

    public static Enumerable<Object[]> convertDataToEnumerable(
            List<Map<String, Object>> data,
            List<RelDataTypeField> dataFields
    ) {
        List<Object[]> list = new ArrayList<>();
        for (Map<String, Object> map : data) {
            Object[] row = new Object[dataFields.size()];
            for (int i = 0; i < dataFields.size(); i++) {
                RelDataTypeField field = dataFields.get(i);
                row[i] = map.get(field.getName());
            }
            list.add(row);
        }

        return Linq4j.asEnumerable(list);
    }
}
