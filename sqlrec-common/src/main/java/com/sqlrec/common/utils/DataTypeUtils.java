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

public class DataTypeUtils {
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
        if (type.equals("INT")) {
            type = "INTEGER";
        }
        if (type.startsWith("VARCHAR")) {
            type = "VARCHAR";
        }
        if (type.equals("STRING")) {
            type = "VARCHAR";
        }

        if (type.startsWith("ARRAY<") && type.endsWith(">")) {
            String elementType = type.substring("ARRAY<".length(), type.length() - 1);
            RelDataType elementTypeName = getRelDataType(typeFactory, elementType);
            return typeFactory.createArrayType(elementTypeName, -1);
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
            if (field.getName().equals(fieldName)) {
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
                return ((Number) value).byteValue();
            case SMALLINT:
                if (value instanceof Short) {
                    return value;
                }
                return ((Number) value).shortValue();
            case INTEGER:
                if (value instanceof Integer) {
                    return value;
                }
                return ((Number) value).intValue();
            case BIGINT:
                if (value instanceof Long) {
                    return value;
                }
                return ((Number) value).longValue();
            case FLOAT:
            case REAL:
                if (value instanceof Float) {
                    return value;
                }
                return ((Number) value).floatValue();
            case DOUBLE:
                if (value instanceof Double) {
                    return value;
                }
                return ((Number) value).doubleValue();
            case DECIMAL:
                if (value instanceof BigDecimal) {
                    return value;
                }
                return BigDecimal.valueOf(((Number) value).doubleValue());
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
}
