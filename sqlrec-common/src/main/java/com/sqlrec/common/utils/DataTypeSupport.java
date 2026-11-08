package com.sqlrec.common.utils;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlAlienSystemTypeNameSpec;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.calcite.sql.SqlUserDefinedTypeNameSpec;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImplConstants;
import org.apache.flink.sql.parser.impl.ParseException;
import org.apache.flink.sql.parser.impl.TokenMgrError;
import org.apache.flink.sql.parser.type.ExtendedSqlCollectionTypeNameSpec;
import org.apache.flink.sql.parser.type.ExtendedSqlRowTypeNameSpec;
import org.apache.flink.sql.parser.type.SqlMapTypeNameSpec;

import java.io.StringReader;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Internal implementation grouped behind the public {@link DataTypeUtils} facade. */
final class DataTypeSupport {
    private DataTypeSupport() {
    }

    static final class Parser {
        // Accept both identifier quoting styles without interpreting the type grammar ourselves.
        private static final Pattern QUOTED_NAME = Pattern.compile(
                "`(?:[^`]|``)*`|\"((?:[^\"]|\"\")*)\"");

        private Parser() {
        }

        static RelDataType parse(RelDataTypeFactory factory, String text) {
            // Calcite exposes internal scalar names that are not SQL declaration syntax.
            String name = text.trim().toUpperCase(Locale.ROOT);
            boolean nullable = !name.endsWith(" NOT NULL");
            if (!nullable) name = name.substring(0, name.length() - " NOT NULL".length()).trim();
            if (name.equals("NULL") || name.equals("UNKNOWN")) {
                return factory.createTypeWithNullability(factory.createSqlType(SqlTypeName.get(name)), nullable);
            }
            String source = QUOTED_NAME.matcher(text).replaceAll(match -> Matcher.quoteReplacement(
                    match.group(1) == null ? match.group() : "`"
                            + match.group(1).replace("\"\"", "\"").replace("`", "``") + "`"));
            FlinkSqlParserImpl parser = new FlinkSqlParserImpl(new StringReader(source));
            parser.switchTo(SqlAbstractParserImpl.LexicalState.BTID);
            parser.setQuotedCasing(Casing.UNCHANGED);
            parser.setUnquotedCasing(Casing.UNCHANGED);
            parser.setIdentifierMaxLength(Integer.MAX_VALUE);
            try {
                SqlDataTypeSpec type = normalize(parser.ExtendedDataType());
                if (parser.getNextToken().kind != FlinkSqlParserImplConstants.EOF) {
                    throw new IllegalArgumentException("Unexpected content after type: " + text);
                }
                CalciteCatalogReader catalog = new CalciteCatalogReader(CalciteSchema.createRootSchema(false),
                        List.of(), factory, new CalciteConnectionConfigImpl(new Properties()));
                SqlValidator validator = SqlValidatorUtil.newValidator(
                        SqlStdOperatorTable.instance(), catalog, factory, SqlValidator.Config.DEFAULT);
                return type.deriveType(validator, true);
            } catch (ParseException | TokenMgrError invalidType) {
                throw new IllegalArgumentException("Invalid SQL type: " + text, invalidType);
            }
        }

        private static SqlDataTypeSpec normalize(SqlDataTypeSpec type) {
            return new SqlDataTypeSpec(normalize(type.getTypeNameSpec()), type.getParserPosition())
                    .withNullable(type.getNullable());
        }

        /** Preserve SQLRec's aliases while leaving parsing and type derivation to the libraries. */
        private static SqlTypeNameSpec normalize(SqlTypeNameSpec type) {
            if (type instanceof SqlAlienSystemTypeNameSpec
                    && type.getTypeName().getSimple().equals("VARCHAR")) {
                // Flink STRING has a length bound; SQLRec STRING uses Calcite's unbounded VARCHAR.
                return new SqlBasicTypeNameSpec(SqlTypeName.VARCHAR, type.getParserPos());
            }
            if (type instanceof SqlUserDefinedTypeNameSpec) {
                String name = type.getTypeName().getSimple().toUpperCase(Locale.ROOT);
                SqlTypeName scalar = name.equals("TEXT") ? SqlTypeName.VARCHAR : SqlTypeName.get(name);
                if (scalar == null) throw new IllegalArgumentException("Unknown SQL type: " + name);
                return new SqlBasicTypeNameSpec(scalar, type.getParserPos());
            }
            if (type instanceof SqlMapTypeNameSpec) {
                SqlMapTypeNameSpec map = (SqlMapTypeNameSpec) type;
                return new SqlMapTypeNameSpec(normalize(map.getKeyType()), normalize(map.getValType()), type.getParserPos());
            }
            if (type instanceof ExtendedSqlRowTypeNameSpec) {
                ExtendedSqlRowTypeNameSpec row = (ExtendedSqlRowTypeNameSpec) type;
                return new ExtendedSqlRowTypeNameSpec(type.getParserPos(), row.getFieldNames(),
                        row.getFieldTypes().stream().map(Parser::normalize).collect(Collectors.toList()),
                        row.getComments(), row.unparseAsStandard());
            }
            if (type instanceof ExtendedSqlCollectionTypeNameSpec) {
                ExtendedSqlCollectionTypeNameSpec collection = (ExtendedSqlCollectionTypeNameSpec) type;
                return new ExtendedSqlCollectionTypeNameSpec(normalize(collection.getElementTypeName()),
                        collection.elementNullable(), collection.getCollectionTypeName(),
                        collection.unparseAsStandard(), type.getParserPos());
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
                if (!compatibleTypes(desiredField.getType(), givenField.getType())) {
                    throw new RuntimeException("desired field type not equal to given field type: "
                            + desiredField.getType() + " != " + givenField.getType());
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
                if (!compatibleTypes(firstField.getType(), secondField.getType())) {
                    throw new RuntimeException("field type not equal: "
                            + firstField.getType() + " != " + secondField.getType());
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

        private static boolean compatibleTypes(RelDataType first, RelDataType second) {
            SqlTypeName firstType = first.getSqlTypeName();
            SqlTypeName secondType = second.getSqlTypeName();
            if (firstType != secondType) {
                return SqlTypeName.STRING_TYPES.contains(firstType)
                        && SqlTypeName.STRING_TYPES.contains(secondType);
            }
            // Composite types must also agree internally: schema merging does not convert row values.
            switch (firstType) {
                case ARRAY:
                case MULTISET:
                    return compatibleTypes(first.getComponentType(), second.getComponentType());
                case MAP:
                    return compatibleTypes(first.getKeyType(), second.getKeyType())
                            && compatibleTypes(first.getValueType(), second.getValueType());
                case ROW:
                    if (first.getFieldCount() != second.getFieldCount()) {
                        return false;
                    }
                    for (int i = 0; i < first.getFieldCount(); i++) {
                        RelDataTypeField firstField = first.getFieldList().get(i);
                        RelDataTypeField secondField = second.getFieldList().get(i);
                        if (!firstField.getName().equalsIgnoreCase(secondField.getName())
                                || !compatibleTypes(firstField.getType(), secondField.getType())) {
                            return false;
                        }
                    }
                    return true;
                default:
                    return true;
            }
        }
    }
}
