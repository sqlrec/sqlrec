package com.sqlrec.common.utils;

import com.sqlrec.common.schema.FieldSchema;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;

import java.math.BigDecimal;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DataTypeUtils {
    private static final Pattern DECIMAL_TYPE_PATTERN = Pattern.compile(
            "^DECIMAL\\s*\\(\\s*(\\d+)\\s*(?:,\\s*(\\d+)\\s*)?\\)$"
    );
    private static final Pattern CHARACTER_TYPE_PATTERN = Pattern.compile(
            "^(CHAR|VARCHAR)\\s*\\(\\s*(\\d+)\\s*\\)$"
    );

    public static RelDataType getRelDataType(RelDataTypeFactory typeFactory, List<FieldSchema> fieldSchemas) {
        RelDataTypeFactory.FieldInfoBuilder builder = typeFactory.builder();
        for (FieldSchema fieldSchema : fieldSchemas) {
            builder.add(fieldSchema.getName(), getRelDataType(typeFactory, fieldSchema.getType()));
        }
        return builder.build();
    }

    public static RelDataType getRelDataType(String type) {
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        return getRelDataType(typeFactory, type);
    }

    public static RelDataType getRelDataType(RelDataTypeFactory typeFactory, String type) {
        type = type.trim().toUpperCase();
        if (type.endsWith(" NOT NULL")) {
            type = type.substring(0, type.length() - " NOT NULL".length()).trim();
        }
        if (type.equals("INT")) {
            type = "INTEGER";
        }
        if (type.equals("STRING")) {
            type = "VARCHAR";
        }

        if (type.startsWith("ARRAY<") && type.endsWith(">")) {
            String elementType = type.substring("ARRAY<".length(), type.length() - 1);
            RelDataType elementTypeName = getRelDataType(typeFactory, elementType);
            return typeFactory.createArrayType(elementTypeName, -1);
        }

        Matcher decimalMatcher = DECIMAL_TYPE_PATTERN.matcher(type);
        if (decimalMatcher.matches()) {
            int precision = Integer.parseInt(decimalMatcher.group(1));
            int scale = decimalMatcher.group(2) == null
                    ? 0
                    : Integer.parseInt(decimalMatcher.group(2));
            return typeFactory.createSqlType(SqlTypeName.DECIMAL, precision, scale);
        }

        Matcher characterMatcher = CHARACTER_TYPE_PATTERN.matcher(type);
        if (characterMatcher.matches()) {
            SqlTypeName typeName = SqlTypeName.get(characterMatcher.group(1));
            int precision = Integer.parseInt(characterMatcher.group(2));
            return typeFactory.createSqlType(Objects.requireNonNull(typeName), precision);
        }

        SqlTypeName sqlTypeName = SqlTypeName.get(type);
        if (sqlTypeName == null) {
            throw new RuntimeException("sql type name not found: " + type);
        }
        return typeFactory.createSqlType(sqlTypeName);
    }

    public static RelDataTypeField getRelDataTypeField(String name, int index, SqlTypeName typeName) {
        return new RelDataTypeFieldImpl(
                name,
                index,
                new BasicSqlType(RelDataTypeSystem.DEFAULT, typeName)
        );
    }

    public static RelDataTypeField getRelDataTypeField(String name, int index, String typeName) {
        RelDataType fieldType = getRelDataType(typeName);
        return new RelDataTypeFieldImpl(name, index, fieldType);
    }

    public static List<RelDataTypeField> addTypeFields(List<RelDataTypeField> origin, List<FieldSchema> fieldsToAdd) {
        List<RelDataTypeField> newFields = new ArrayList<>(origin);
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        for (FieldSchema fieldSchema : fieldsToAdd) {
            RelDataType fieldType = getRelDataType(typeFactory, fieldSchema.getType());
            newFields.add(
                    new RelDataTypeFieldImpl(
                            fieldSchema.getName(),
                            newFields.size(),
                            fieldType
                    )
            );
        }
        return newFields;
    }

    public static List<RelDataTypeField> getRelDataTypeFields(
            List<SqlIdentifier> columnList,
            List<SqlTypeNameSpec> columnTypeList,
            SqlValidator validator
    ) {
        if (columnList.size() != columnTypeList.size()) {
            throw new RuntimeException("column list size not equal to column type list size");
        }

        List<RelDataTypeField> relDataTypeFields = new ArrayList<>();
        for (int i = 0; i < columnList.size(); i++) {
            relDataTypeFields.add(
                    new RelDataTypeFieldImpl(
                            columnList.get(i).getSimple(),
                            i,
                            columnTypeList.get(i).deriveType(validator)
                    )
            );
        }
        return relDataTypeFields;
    }

    public static List<RelDataTypeField> getStringTypeField(String fieldName) {
        return getStringTypeFieldList(Collections.singletonList(fieldName));
    }

    public static List<RelDataTypeField> getStringTypeFieldList(List<String> fieldName) {
        List<RelDataTypeField> fields = new ArrayList<>();
        int index = 0;
        for (String name : fieldName) {
            fields.add(getRelDataTypeField(name, index++, SqlTypeName.VARCHAR));
        }
        return fields;
    }

    public static void checkTableSchemaCompatible(
            List<RelDataTypeField> desiredFields,
            List<RelDataTypeField> givenFields
    ) {
        if (desiredFields.size() > givenFields.size()) {
            throw new RuntimeException("desired fields size greater than given fields size");
        }

        for (int i = 0; i < desiredFields.size(); i++) {
            RelDataTypeField desiredField = desiredFields.get(i);
            RelDataTypeField givenField = givenFields.get(i);
            if (!desiredField.getName().equalsIgnoreCase(givenField.getName())) {
                throw new RuntimeException(
                        "desired field name not equal to given field name: "
                                + desiredField.getName() + " != " + givenField.getName());
            }
            if (SqlTypeName.STRING_TYPES.contains(desiredField.getType().getSqlTypeName()) &&
                    SqlTypeName.STRING_TYPES.contains(givenField.getType().getSqlTypeName())) {
                continue;
            }
            if (!desiredField.getType().getSqlTypeName().equals(givenField.getType().getSqlTypeName())) {
                throw new RuntimeException(
                        "desired field type not equal to given field type: "
                                + desiredField.getType().getSqlTypeName() + " != "
                                + givenField.getType().getSqlTypeName());
            }
        }
    }

    /**
     * Checks that two field lists describe the same schema: same field count,
     * same field names and same types. String types (CHAR/VARCHAR/...) are
     * interchangeable with each other, everything else must match exactly.
     */
    public static void checkTableSchemaSame(
            List<RelDataTypeField> fields1,
            List<RelDataTypeField> fields2
    ) {
        if (fields1.size() != fields2.size()) {
            throw new RuntimeException(
                    "field count not equal: " + fields1.size() + " != " + fields2.size());
        }

        for (int i = 0; i < fields1.size(); i++) {
            RelDataTypeField field1 = fields1.get(i);
            RelDataTypeField field2 = fields2.get(i);
            if (!field1.getName().equalsIgnoreCase(field2.getName())) {
                throw new RuntimeException(
                        "field name not equal: " + field1.getName() + " != " + field2.getName());
            }
            if (SqlTypeName.STRING_TYPES.contains(field1.getType().getSqlTypeName()) &&
                    SqlTypeName.STRING_TYPES.contains(field2.getType().getSqlTypeName())) {
                continue;
            }
            if (!field1.getType().getSqlTypeName().equals(field2.getType().getSqlTypeName())) {
                throw new RuntimeException(
                        "field type not equal: "
                                + field1.getType().getSqlTypeName() + " != "
                                + field2.getType().getSqlTypeName());
            }
        }
    }

    public static void checkTableSchemaIdentical(List<RelDataTypeField> referenceFields, List<RelDataTypeField> fields, int tableIndex) {
        if (referenceFields.size() != fields.size()) {
            throw new IllegalArgumentException("Table " + tableIndex + " has different column count than table 0");
        }
        for (int j = 0; j < referenceFields.size(); j++) {
            if (!referenceFields.get(j).getType().equals(fields.get(j).getType())) {
                throw new IllegalArgumentException("Column type mismatch at index " + j
                        + ": " + referenceFields.get(j).getType() + " vs " + fields.get(j).getType());
            }
        }
    }

    public static int findFieldIndex(List<RelDataTypeField> fields, String fieldName) {
        for (RelDataTypeField field : fields) {
            if (field.getName().equalsIgnoreCase(fieldName)) {
                return field.getIndex();
            }
        }
        return -1;
    }

    public static List<String> getTableFieldNames(Table calciteTable) {
        return calciteTable.getRowType(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT)).getFieldNames();
    }

    public static Object parseStringAsType(String value, String type) {
        if (value == null) {
            return null;
        }
        switch (type.toUpperCase()) {
            case "TINYINT":
                return Byte.parseByte(value);
            case "SMALLINT":
                return Short.parseShort(value);
            case "INTEGER":
            case "INT":
                return Integer.parseInt(value);
            case "BIGINT":
                return Long.parseLong(value);
            case "FLOAT":
            case "REAL":
                return Float.parseFloat(value);
            case "DOUBLE":
                return Double.parseDouble(value);
            case "DECIMAL":
            case "NUMERIC":
                return new BigDecimal(value);
            case "BOOLEAN":
                return Boolean.valueOf(value);
            case "VARCHAR":
            case "CHAR":
            case "TEXT":
            case "STRING":
                return value;
            case "DATE":
            case "TIME":
            case "TIMESTAMP":
                return value;
            default:
                return value;
        }
    }

    public static Object convertType(Object value, SqlTypeName sqlTypeName) {
        if (value == null) {
            return null;
        }
        if (sqlTypeName == null) {
            return value;
        }
        switch (sqlTypeName) {
            case TINYINT:
                if (value instanceof Byte) {
                    return value;
                }
                return toNumber(value, sqlTypeName).byteValue();
            case SMALLINT:
                if (value instanceof Short) {
                    return value;
                }
                return toNumber(value, sqlTypeName).shortValue();
            case INTEGER:
                if (value instanceof Integer) {
                    return value;
                }
                return toNumber(value, sqlTypeName).intValue();
            case BIGINT:
                if (value instanceof Long) {
                    return value;
                }
                return toNumber(value, sqlTypeName).longValue();
            case FLOAT:
            case REAL:
                if (value instanceof Float) {
                    return value;
                }
                return toNumber(value, sqlTypeName).floatValue();
            case DOUBLE:
                if (value instanceof Double) {
                    return value;
                }
                return toNumber(value, sqlTypeName).doubleValue();
            case DECIMAL:
                if (value instanceof BigDecimal) {
                    return value;
                }
                return toNumber(value, sqlTypeName);
            case BOOLEAN:
                if (value instanceof Boolean) {
                    return value;
                }
                return Boolean.valueOf(value.toString());
            case VARCHAR:
            case CHAR:
                if (value instanceof String) {
                    return value;
                }
                return value.toString();
            case DATE:
            case TIME:
            case TIMESTAMP:
                return value;
            default:
                return value;
        }
    }

    private static Number toNumber(Object value, SqlTypeName target) {
        if (value instanceof Number) {
            return (Number) value;
        }
        if (value instanceof String) {
            String s = (String) value;
            try {
                switch (target) {
                    case TINYINT:
                        return Byte.valueOf(s);
                    case SMALLINT:
                        return Short.valueOf(s);
                    case INTEGER:
                        return Integer.valueOf(s);
                    case BIGINT:
                        return Long.valueOf(s);
                    case FLOAT:
                    case REAL:
                        return Float.valueOf(s);
                    case DOUBLE:
                        return Double.valueOf(s);
                    case DECIMAL:
                        return new BigDecimal(s);
                    default:
                        return Double.valueOf(s);
                }
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        "Cannot parse '" + s + "' as " + target + " value", e);
            }
        }
        throw new IllegalArgumentException(
                "Cannot convert " + value.getClass().getName() + " to numeric type " + target);
    }

    public static Set<Object> convertKeySet(Set<Object> keySet, SqlTypeName sqlTypeName) {
        Set<Object> result = new HashSet<>(keySet.size());
        for (Object key : keySet) {
            result.add(convertType(key, sqlTypeName));
        }
        return result;
    }

    public static <V> Map<Object, V> convertMapKeys(Map<Object, V> map, SqlTypeName sqlTypeName) {
        Map<Object, V> result = new HashMap<>(map.size());
        for (Map.Entry<Object, V> entry : map.entrySet()) {
            result.put(convertType(entry.getKey(), sqlTypeName), entry.getValue());
        }
        return result;
    }

    public static void convertRowTypes(List<Object[]> rows, List<RelDataTypeField> fields) {
        if (rows == null || fields == null) {
            return;
        }
        for (Object[] row : rows) {
            if (row == null) {
                continue;
            }
            if (fields.size() > row.length) {
                throw new RuntimeException("convertRowTypes failed, row length is " + row.length + ", fields size is " + fields.size());
            }
            for (int i = 0; i < fields.size(); i++) {
                RelDataTypeField field = fields.get(i);
                SqlTypeName targetType = field.getType().getSqlTypeName();
                if (row[i] != null) {
                    row[i] = convertType(row[i], targetType);
                }
            }
        }
    }

    /**
     * Adapt rows from the givenFields schema to the desiredFields schema with relaxed
     * compatibility, supporting the following cases:
     * 1. Fields may be in any order: as long as each desired field exists in given fields,
     *    the result is reordered to match the desired order.
     * 2. Any type can be converted to a string type (when the desired type is a string type).
     * 3. Numeric types can be converted between each other.
     * Any other incompatible case throws an exception.
     *
     * @return a new list of rows reordered and converted to the desired field order and types.
     */
    public static List<Object[]> adaptRowsToSchema(
            List<Object[]> rows,
            List<RelDataTypeField> desiredFields,
            List<RelDataTypeField> givenFields
    ) {
        if (rows == null || desiredFields == null || givenFields == null) {
            throw new RuntimeException("adaptRowsToSchema failed, rows/desiredFields/givenFields must not be null");
        }

        Map<String, Integer> givenNameToIndex = new HashMap<>();
        for (int i = 0; i < givenFields.size(); i++) {
            givenNameToIndex.put(givenFields.get(i).getName().toLowerCase(Locale.ROOT), i);
        }

        int[] indexMapping = new int[desiredFields.size()];
        for (int i = 0; i < desiredFields.size(); i++) {
            RelDataTypeField desiredField = desiredFields.get(i);
            Integer givenIdx = givenNameToIndex.get(desiredField.getName().toLowerCase(Locale.ROOT));
            if (givenIdx == null) {
                throw new RuntimeException(
                        "adaptRowsToSchema failed, desired field not found in given fields: "
                                + desiredField.getName());
            }
            checkFieldTypeCompatible(desiredField, givenFields.get(givenIdx));
            indexMapping[i] = givenIdx;
        }

        List<Object[]> result = new ArrayList<>(rows.size());
        for (Object[] row : rows) {
            if (row == null) {
                result.add(null);
                continue;
            }
            Object[] newRow = new Object[desiredFields.size()];
            for (int i = 0; i < desiredFields.size(); i++) {
                int givenIdx = indexMapping[i];
                if (givenIdx < row.length && row[givenIdx] != null) {
                    SqlTypeName targetType = desiredFields.get(i).getType().getSqlTypeName();
                    newRow[i] = convertType(row[givenIdx], targetType);
                }
            }
            result.add(newRow);
        }
        return result;
    }

    private static void checkFieldTypeCompatible(RelDataTypeField desiredField, RelDataTypeField givenField) {
        SqlTypeName desiredType = desiredField.getType().getSqlTypeName();
        SqlTypeName givenType = givenField.getType().getSqlTypeName();
        // Rule 2: any type can be converted to a string type
        if (SqlTypeName.STRING_TYPES.contains(desiredType)) {
            return;
        }
        // Rule 3: numeric types can be converted between each other
        if (SqlTypeName.NUMERIC_TYPES.contains(desiredType)
                && SqlTypeName.NUMERIC_TYPES.contains(givenType)) {
            return;
        }
        // Otherwise an exact type match is required
        if (desiredType.equals(givenType)) {
            return;
        }
        throw new RuntimeException(
                "adaptRowsToSchema failed, incompatible field type for '"
                        + desiredField.getName() + "': desired " + desiredType
                        + ", given " + givenType);
    }

    public static List<RelDataTypeField> inferFields(List<Map<String, Object>> rows) {
        List<RelDataTypeField> fields = new ArrayList<>();
        if (rows == null || rows.isEmpty()) {
            return fields;
        }
        Map<String, Object> firstRow = rows.get(0);
        int index = 0;
        for (String name : firstRow.keySet()) {
            fields.add(getRelDataTypeField(name, index, inferColumnTypeName(rows, name)));
            index++;
        }
        return fields;
    }

    public static String inferColumnTypeName(List<Map<String, Object>> rows, String columnName) {
        for (Map<String, Object> row : rows) {
            Object value = row.get(columnName);
            if (value != null) {
                return inferTypeName(value);
            }
        }
        return "VARCHAR";
    }

    public static String inferTypeName(Object value) {
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
            if (elementType != null) {
                return "ARRAY<" + elementType + ">";
            }
            return "VARCHAR";
        }
        // String, Map -> VARCHAR
        return "VARCHAR";
    }

    public static String inferListElementType(List<?> list) {
        for (Object elem : list) {
            if (elem != null) {
                // nested complex type -> fall back to VARCHAR
                if (elem instanceof Map || elem instanceof List) {
                    return null;
                }
                return inferTypeName(elem);
            }
        }
        return null;
    }
}
