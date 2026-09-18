package com.sqlrec.common.utils;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Internal implementation grouped behind the public {@link DataTypeUtils} facade. */
final class DataTypeSupport {
    private DataTypeSupport() {
    }

    static final class Parser {
        private static final Pattern DECIMAL = Pattern.compile(
                "^DECIMAL\\s*\\(\\s*(\\d+)\\s*(?:,\\s*(\\d+)\\s*)?\\)$");
        private static final Pattern CHARACTER = Pattern.compile(
                "^(CHAR|VARCHAR)\\s*\\(\\s*(\\d+)\\s*\\)$");
    
        private Parser() {
        }
    
        static RelDataType parse(RelDataTypeFactory factory, String text) {
            String type = normalize(text);
            if (type.startsWith("ARRAY<") && type.endsWith(">")) {
                return factory.createArrayType(
                        parse(factory, type.substring("ARRAY<".length(), type.length() - 1)), -1);
            }
    
            Matcher decimal = DECIMAL.matcher(type);
            if (decimal.matches()) {
                int precision = Integer.parseInt(decimal.group(1));
                int scale = decimal.group(2) == null ? 0 : Integer.parseInt(decimal.group(2));
                return factory.createSqlType(SqlTypeName.DECIMAL, precision, scale);
            }
            Matcher character = CHARACTER.matcher(type);
            if (character.matches()) {
                return factory.createSqlType(
                        Objects.requireNonNull(SqlTypeName.get(character.group(1))),
                        Integer.parseInt(character.group(2)));
            }
            SqlTypeName typeName = SqlTypeName.get(type);
            if (typeName == null) {
                throw new RuntimeException("sql type name not found: " + type);
            }
            return factory.createSqlType(typeName);
        }
    
        private static String normalize(String text) {
            String type = text.trim().toUpperCase();
            if (type.endsWith(" NOT NULL")) {
                type = type.substring(0, type.length() - " NOT NULL".length()).trim();
            }
            if (type.equals("INT")) {
                return "INTEGER";
            }
            if (type.equals("STRING")) {
                return "VARCHAR";
            }
            return type;
        }
    }

    static final class Schemas {
        private Schemas() {
        }
    
        static void checkCompatible(
                List<RelDataTypeField> desired,
                List<RelDataTypeField> given
        ) {
            if (desired.size() > given.size()) {
                throw new RuntimeException("desired fields size greater than given fields size");
            }
            for (int i = 0; i < desired.size(); i++) {
                RelDataTypeField desiredField = desired.get(i);
                RelDataTypeField givenField = given.get(i);
                if (!desiredField.getName().equalsIgnoreCase(givenField.getName())) {
                    throw new RuntimeException("desired field name not equal to given field name: "
                            + desiredField.getName() + " != " + givenField.getName());
                }
                if (!compatibleTypes(desiredField, givenField)) {
                    throw new RuntimeException("desired field type not equal to given field type: "
                            + desiredField.getType().getSqlTypeName() + " != "
                            + givenField.getType().getSqlTypeName());
                }
            }
        }
    
        static void checkSame(List<RelDataTypeField> first, List<RelDataTypeField> second) {
            if (first.size() != second.size()) {
                throw new RuntimeException(
                        "field count not equal: " + first.size() + " != " + second.size());
            }
            for (int i = 0; i < first.size(); i++) {
                RelDataTypeField firstField = first.get(i);
                RelDataTypeField secondField = second.get(i);
                if (!firstField.getName().equalsIgnoreCase(secondField.getName())) {
                    throw new RuntimeException("field name not equal: "
                            + firstField.getName() + " != " + secondField.getName());
                }
                if (!compatibleTypes(firstField, secondField)) {
                    throw new RuntimeException("field type not equal: "
                            + firstField.getType().getSqlTypeName() + " != "
                            + secondField.getType().getSqlTypeName());
                }
            }
        }
    
        static void checkIdentical(
                List<RelDataTypeField> reference,
                List<RelDataTypeField> fields,
                int tableIndex
        ) {
            if (reference.size() != fields.size()) {
                throw new IllegalArgumentException(
                        "Table " + tableIndex + " has different column count than table 0");
            }
            for (int i = 0; i < reference.size(); i++) {
                if (!reference.get(i).getType().equals(fields.get(i).getType())) {
                    throw new IllegalArgumentException("Column type mismatch at index " + i
                            + ": " + reference.get(i).getType() + " vs " + fields.get(i).getType());
                }
            }
        }
    
        private static boolean compatibleTypes(RelDataTypeField first, RelDataTypeField second) {
            SqlTypeName firstType = first.getType().getSqlTypeName();
            SqlTypeName secondType = second.getType().getSqlTypeName();
            return firstType.equals(secondType)
                    || SqlTypeName.STRING_TYPES.contains(firstType)
                    && SqlTypeName.STRING_TYPES.contains(secondType);
        }
    }

    static final class Rows {
        private Rows() {
        }
    
        static Object convert(Object value, SqlTypeName target) {
            if (value == null || target == null) {
                return value;
            }
            switch (target) {
                case TINYINT:
                    return value instanceof Byte ? value : number(value, target).byteValue();
                case SMALLINT:
                    return value instanceof Short ? value : number(value, target).shortValue();
                case INTEGER:
                    return value instanceof Integer ? value : number(value, target).intValue();
                case BIGINT:
                    return value instanceof Long ? value : number(value, target).longValue();
                case FLOAT:
                case REAL:
                    return value instanceof Float ? value : number(value, target).floatValue();
                case DOUBLE:
                    return value instanceof Double ? value : number(value, target).doubleValue();
                case DECIMAL:
                    return value instanceof BigDecimal ? value : new BigDecimal(value.toString());
                case BOOLEAN:
                    return value instanceof Boolean ? value : Boolean.valueOf(value.toString());
                case VARCHAR:
                case CHAR:
                    return value instanceof String ? value : value.toString();
                default:
                    return value;
            }
        }
    
        static Set<Object> convertKeys(Set<Object> keys, SqlTypeName target) {
            Set<Object> result = new HashSet<>(keys.size());
            keys.forEach(key -> result.add(convert(key, target)));
            return result;
        }
    
        static <V> Map<Object, V> convertKeys(Map<Object, V> source, SqlTypeName target) {
            Map<Object, V> result = new HashMap<>(source.size());
            source.forEach((key, value) -> result.put(convert(key, target), value));
            return result;
        }
    
        static void convertRows(List<Object[]> rows, List<RelDataTypeField> fields) {
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
                    row[i] = convert(row[i], fields.get(i).getType().getSqlTypeName());
                }
            }
        }
    
        static List<Object[]> adapt(
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
                    result[i] = convert(row[sourceIndex], desired.get(i).getType().getSqlTypeName());
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
    
        private static Number number(Object value, SqlTypeName target) {
            if (value instanceof Number) {
                return (Number) value;
            }
            if (!(value instanceof String)) {
                throw new IllegalArgumentException(
                        "Cannot convert " + value.getClass().getName() + " to numeric type " + target);
            }
            String text = (String) value;
            try {
                switch (target) {
                    case TINYINT: return Byte.valueOf(text);
                    case SMALLINT: return Short.valueOf(text);
                    case INTEGER: return Integer.valueOf(text);
                    case BIGINT: return Long.valueOf(text);
                    case FLOAT:
                    case REAL: return Float.valueOf(text);
                    case DOUBLE: return Double.valueOf(text);
                    case DECIMAL: return new BigDecimal(text);
                    default: return Double.valueOf(text);
                }
            } catch (NumberFormatException exception) {
                throw new IllegalArgumentException(
                        "Cannot parse '" + text + "' as " + target + " value", exception);
            }
        }
    }

    static final class Inference {
        private Inference() {
        }
    
        static String inferColumnTypeName(List<Map<String, Object>> rows, String columnName) {
            for (Map<String, Object> row : rows) {
                Object value = row.get(columnName);
                if (value != null) {
                    return inferTypeName(value);
                }
            }
            return "VARCHAR";
        }
    
        static String inferTypeName(Object value) {
            if (value == null) {
                return "VARCHAR";
            }
            if (value instanceof Long || value instanceof Integer) {
                return "BIGINT";
            }
            if (value instanceof Number) {
                return "DOUBLE";
            }
            if (value instanceof Boolean) {
                return "BOOLEAN";
            }
            if (value instanceof List) {
                String elementType = inferListElementType((List<?>) value);
                return elementType == null ? "VARCHAR" : "ARRAY<" + elementType + ">";
            }
            return "VARCHAR";
        }
    
        static String inferListElementType(List<?> list) {
            for (Object element : list) {
                if (element != null) {
                    if (element instanceof Map || element instanceof List) {
                        return null;
                    }
                    return inferTypeName(element);
                }
            }
            return null;
        }
    }
}
