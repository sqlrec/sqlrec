package com.sqlrec.common.utils;

import com.sqlrec.common.schema.FieldSchema;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;

import java.util.ArrayList;
import java.util.List;

public class DataTypeUtils {
    public static RelDataType getRelDataType(RelDataTypeFactory typeFactory, List<FieldSchema> fieldSchemas) {
        RelDataTypeFactory.FieldInfoBuilder builder = typeFactory.builder();
        for (FieldSchema fieldSchema : fieldSchemas) {
            builder.add(fieldSchema.name, getSqlTypeName(typeFactory, fieldSchema.type));
        }
        return builder.build();
    }

    public static RelDataType getSqlTypeName(RelDataTypeFactory typeFactory, String type) {
        type = type.toUpperCase();
        if (type.equals("INT")) {
            type = "INTEGER";
        }

        if (type.startsWith("ARRAY<") && type.endsWith(">")) {
            String elementType = type.substring("ARRAY<".length(), type.length() - 1);
            RelDataType elementTypeName = getSqlTypeName(typeFactory, elementType);
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
            if (!desiredField.getType().getSqlTypeName().equals(givenField.getType().getSqlTypeName())) {
                throw new RuntimeException(
                        "desired field type not equal to given field type: "
                                + desiredField.getType().getSqlTypeName() + " != "
                                + givenField.getType().getSqlTypeName());
            }
        }
    }
}
