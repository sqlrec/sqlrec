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

    public static List<String> getTableFieldNames(Table calciteTable) {
        return calciteTable.getRowType(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT)).getFieldNames();
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
                return ((Number) value).byteValue();
            case SMALLINT:
                return ((Number) value).shortValue();
            case INTEGER:
                return ((Number) value).intValue();
            case BIGINT:
                return ((Number) value).longValue();
            case FLOAT:
            case REAL:
                return ((Number) value).floatValue();
            case DOUBLE:
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
}
