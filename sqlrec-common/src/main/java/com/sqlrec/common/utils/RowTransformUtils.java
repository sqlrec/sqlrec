package com.sqlrec.common.utils;

import com.sqlrec.common.schema.CacheTable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Conversions between row arrays, named values and Calcite enumerables. */
public final class RowTransformUtils {
    private RowTransformUtils() {
    }

    /** Materialize a cache table exactly once, treating a null enumerable as no rows. */
    public static List<Object[]> materializeRows(CacheTable table) {
        Enumerable<Object[]> enumerable = table.scan(null);
        return enumerable == null ? new ArrayList<>() : enumerable.toList();
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
            list.add(projectRow(dataFields, field -> map.get(field.getName())));
        }

        return Linq4j.asEnumerable(list);
    }

    public static Enumerable<Object[]> getJsonValueEnumerable(Enumerable<Object[]> enumerable, int index) {
        if (enumerable == null) {
            return null;
        }

        List<Object[]> list = new ArrayList<>();
        for (Object[] row : enumerable) {
            Object value = row[index];
            list.add(new Object[]{value == null ? null : JsonUtils.toJson(value)});
        }
        return Linq4j.asEnumerable(list);
    }

    public static Enumerable<Object[]> getMsgEnumerable(String msg) {
        if (msg == null) {
            return null;
        }
        return Linq4j.asEnumerable(Collections.singletonList(new String[]{msg}));
    }

    public static Enumerable<Object[]> convertListToEnumerable(List<String> list) {
        if (list == null) {
            return null;
        }
        return Linq4j.asEnumerable(list.stream().map(o -> new String[]{o}).collect(Collectors.toList()));
    }

    public static <T> Enumerable<Object[]> convertListToArrayToEnumerable(List<List<T>> list) {
        if (list == null) {
            return null;
        }
        return Linq4j.asEnumerable(list.stream().map(List::toArray).collect(Collectors.toList()));
    }

    /** Projects named values in the supplied schema order, preserving missing values as null. */
    public static <F> Object[] projectRow(List<F> fields, Function<F, Object> valueReader) {
        Object[] row = new Object[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            row[i] = valueReader.apply(fields.get(i));
        }
        return row;
    }

    public static Set<Object> convertKeySet(Set<Object> keys, SqlTypeName target) {
        Set<Object> result = new HashSet<>(keys.size());
        keys.forEach(key -> result.add(ScalarConversions.convert(key, target)));
        return result;
    }

    public static <V> Map<Object, V> convertMapKeys(Map<Object, V> source, SqlTypeName target) {
        Map<Object, V> result = new HashMap<>(source.size());
        source.forEach((key, value) -> result.put(ScalarConversions.convert(key, target), value));
        return result;
    }

    /** Converts scalar cells in place, retaining null rows and complex values. */
    public static void convertRowTypes(List<Object[]> rows, List<RelDataTypeField> fields) {
        if (rows == null || fields == null) {
            return;
        }
        for (Object[] row : rows) {
            if (row == null) {
                continue;
            }
            if (fields.size() > row.length) {
                throw new RuntimeException("convertRowTypes failed, row length is " + row.length
                        + ", fields size is " + fields.size());
            }
            for (int i = 0; i < fields.size(); i++) {
                row[i] = ScalarConversions.convert(row[i], fields.get(i).getType().getSqlTypeName());
            }
        }
    }

    /** Reorders and copies rows, allowing numeric conversions and conversion to string fields. */
    public static List<Object[]> adaptRowsToSchema(
            List<Object[]> rows,
            List<RelDataTypeField> desired,
            List<RelDataTypeField> given
    ) {
        if (rows == null || desired == null || given == null) {
            throw new RuntimeException(
                    "adaptRowsToSchema failed, rows/desiredFields/givenFields must not be null");
        }
        int[] mapping = mapping(desired, given);
        List<Object[]> result = new ArrayList<>(rows.size());
        for (Object[] row : rows) {
            result.add(row == null ? null : adaptRow(row, desired, mapping));
        }
        return result;
    }

    private static int[] mapping(
            List<RelDataTypeField> desired,
            List<RelDataTypeField> given
    ) {
        Map<String, Integer> indexes = new HashMap<>();
        for (int i = 0; i < given.size(); i++) {
            indexes.put(given.get(i).getName().toLowerCase(Locale.ROOT), i);
        }
        int[] mapping = new int[desired.size()];
        for (int i = 0; i < desired.size(); i++) {
            RelDataTypeField desiredField = desired.get(i);
            Integer givenIndex = indexes.get(desiredField.getName().toLowerCase(Locale.ROOT));
            if (givenIndex == null) {
                throw new RuntimeException(
                        "adaptRowsToSchema failed, desired field not found in given fields: "
                                + desiredField.getName());
            }
            checkCompatible(desiredField, given.get(givenIndex));
            mapping[i] = givenIndex;
        }
        return mapping;
    }

    private static Object[] adaptRow(
            Object[] row,
            List<RelDataTypeField> desired,
            int[] mapping
    ) {
        Object[] result = new Object[desired.size()];
        for (int i = 0; i < desired.size(); i++) {
            int sourceIndex = mapping[i];
            if (sourceIndex < row.length) {
                result[i] = ScalarConversions.convert(row[sourceIndex], desired.get(i).getType().getSqlTypeName());
            }
        }
        return result;
    }

    private static void checkCompatible(RelDataTypeField desired, RelDataTypeField given) {
        SqlTypeName desiredType = desired.getType().getSqlTypeName();
        SqlTypeName givenType = given.getType().getSqlTypeName();
        if (desiredType.equals(givenType)
                || SqlTypeName.STRING_TYPES.contains(desiredType)
                || SqlTypeName.NUMERIC_TYPES.contains(desiredType)
                && SqlTypeName.NUMERIC_TYPES.contains(givenType)) {
            return;
        }
        throw new RuntimeException("adaptRowsToSchema failed, incompatible field type for '"
                + desired.getName() + "': desired " + desiredType + ", given " + givenType);
    }
}
