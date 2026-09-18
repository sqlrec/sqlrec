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
        return DataTypeSupport.Parser.parse(typeFactory, type);
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
        DataTypeSupport.Schemas.checkCompatible(desiredFields, givenFields);
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
        DataTypeSupport.Schemas.checkSame(fields1, fields2);
    }

    public static void checkTableSchemaIdentical(List<RelDataTypeField> referenceFields, List<RelDataTypeField> fields, int tableIndex) {
        DataTypeSupport.Schemas.checkIdentical(referenceFields, fields, tableIndex);
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
        return DataTypeSupport.Rows.convert(value, sqlTypeName);
    }

    public static Set<Object> convertKeySet(Set<Object> keySet, SqlTypeName sqlTypeName) {
        return DataTypeSupport.Rows.convertKeys(keySet, sqlTypeName);
    }

    public static <V> Map<Object, V> convertMapKeys(Map<Object, V> map, SqlTypeName sqlTypeName) {
        return DataTypeSupport.Rows.convertKeys(map, sqlTypeName);
    }

    public static void convertRowTypes(List<Object[]> rows, List<RelDataTypeField> fields) {
        DataTypeSupport.Rows.convertRows(rows, fields);
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
        return DataTypeSupport.Rows.adapt(rows, desiredFields, givenFields);
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
        return DataTypeSupport.Inference.inferColumnTypeName(rows, columnName);
    }

    public static String inferTypeName(Object value) {
        return DataTypeSupport.Inference.inferTypeName(value);
    }

    public static String inferListElementType(List<?> list) {
        return DataTypeSupport.Inference.inferListElementType(list);
    }
}
