package com.sqlrec.common.utils;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;
import java.util.Locale;
import java.util.Objects;
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
            String type = text.trim().toUpperCase(Locale.ROOT);
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
}
