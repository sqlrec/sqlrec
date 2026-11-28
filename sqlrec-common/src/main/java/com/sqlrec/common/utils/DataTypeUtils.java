package com.sqlrec.common.utils;

import com.sqlrec.common.schema.FieldSchema;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlAlienSystemTypeNameSpec;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.calcite.sql.SqlUserDefinedTypeNameSpec;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImplConstants;
import org.apache.flink.sql.parser.impl.ParseException;
import org.apache.flink.sql.parser.impl.TokenMgrError;
import org.apache.flink.sql.parser.type.ExtendedSqlCollectionTypeNameSpec;
import org.apache.flink.sql.parser.type.SqlMapTypeNameSpec;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

/** Parses SQL types and builds and compares Calcite schemas. */
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
        // Calcite exposes internal scalar names that are not SQL declaration syntax.
        String name = type.trim().toUpperCase(Locale.ROOT);
        String notNull = " NOT NULL";
        boolean nullable = !name.endsWith(notNull);
        if (!nullable) {
            name = name.substring(0, name.length() - notNull.length()).trim();
        }
        if (name.equals("NULL") || name.equals("UNKNOWN")) {
            return typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.get(name)), nullable);
        }

        FlinkSqlParserImpl parser = new FlinkSqlParserImpl(new StringReader(type));
        parser.switchTo(SqlAbstractParserImpl.LexicalState.BTID);
        parser.setQuotedCasing(Casing.UNCHANGED);
        parser.setUnquotedCasing(Casing.UNCHANGED);
        parser.setIdentifierMaxLength(Integer.MAX_VALUE);
        try {
            SqlDataTypeSpec spec = normalizeType(parser.ExtendedDataType());
            if (parser.getNextToken().kind != FlinkSqlParserImplConstants.EOF) {
                throw new IllegalArgumentException("Unexpected content after type: " + type);
            }
            CalciteCatalogReader catalog = new CalciteCatalogReader(CalciteSchema.createRootSchema(false),
                    List.of(), typeFactory, new CalciteConnectionConfigImpl(new Properties()));
            SqlValidator validator = SqlValidatorUtil.newValidator(
                    SqlStdOperatorTable.instance(), catalog, typeFactory, SqlValidator.Config.DEFAULT);
            return requireSupportedColumnType(spec.deriveType(validator, true));
        } catch (ParseException | TokenMgrError invalidType) {
            throw new IllegalArgumentException("Invalid SQL type: " + type, invalidType);
        }
    }

    private static SqlDataTypeSpec normalizeType(SqlDataTypeSpec type) {
        return new SqlDataTypeSpec(normalizeType(type.getTypeNameSpec()), type.getParserPosition())
                .withNullable(type.getNullable());
    }

    /** Applies SQLRec's aliases to the parsed type. */
    private static SqlTypeNameSpec normalizeType(SqlTypeNameSpec type) {
        if (type instanceof SqlAlienSystemTypeNameSpec && type.getTypeName().getSimple().equals("VARCHAR")) {
            // Flink STRING has a length bound; SQLRec STRING uses Calcite's unbounded VARCHAR.
            return new SqlBasicTypeNameSpec(SqlTypeName.VARCHAR, type.getParserPos());
        }
        if (type instanceof SqlUserDefinedTypeNameSpec) {
            String name = type.getTypeName().getSimple().toUpperCase(Locale.ROOT);
            SqlTypeName scalar = name.equals("TEXT") ? SqlTypeName.VARCHAR : SqlTypeName.get(name);
            if (scalar == null) {
                throw new IllegalArgumentException("Unknown SQL type: " + name);
            }
            if (scalar == SqlTypeName.MAP) {
                throw new IllegalArgumentException("MAP requires key and value types");
            }
            return new SqlBasicTypeNameSpec(scalar, type.getParserPos());
        }
        if (type instanceof SqlMapTypeNameSpec) {
            SqlMapTypeNameSpec map = (SqlMapTypeNameSpec) type;
            return new SqlMapTypeNameSpec(normalizeType(map.getKeyType()), normalizeType(map.getValType()),
                    type.getParserPos());
        }
        if (type instanceof ExtendedSqlCollectionTypeNameSpec) {
            ExtendedSqlCollectionTypeNameSpec collection = (ExtendedSqlCollectionTypeNameSpec) type;
            return new ExtendedSqlCollectionTypeNameSpec(normalizeType(collection.getElementTypeName()),
                    collection.elementNullable(), collection.getCollectionTypeName(),
                    collection.unparseAsStandard(), type.getParserPos());
        }
        return type;
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

    public static RelDataType requireSupportedColumnType(RelDataType type) {
        switch (type.getSqlTypeName()) {
            case ROW:
                throw new IllegalArgumentException("ROW column types are not supported");
            case ARRAY:
            case MULTISET:
                requireSupportedColumnType(type.getComponentType());
                break;
            case MAP:
                requireSupportedColumnType(type.getKeyType());
                requireSupportedColumnType(type.getValueType());
                break;
            default:
                break;
        }
        return type;
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
        checkMatchingFields(desiredFields, givenFields,
                "desired field name not equal to given field name: ",
                "desired field type not equal to given field type: ");
    }

    /**
     * Checks that two field lists describe the same schema: same field count,
     * same field names and type names, including nested element and key/value types.
     * Nullability and type bounds are ignored; string types (CHAR/VARCHAR/...) are interchangeable.
     */
    public static void checkTableSchemaSame(
            List<RelDataTypeField> first,
            List<RelDataTypeField> second
    ) {
        if (first.size() != second.size()) {
            throw new RuntimeException("field count not equal: " + first.size() + " != " + second.size());
        }
        checkMatchingFields(first, second, "field name not equal: ", "field type not equal: ");
    }

    private static void checkMatchingFields(
            List<RelDataTypeField> first, List<RelDataTypeField> second, String nameMismatch, String typeMismatch) {
        for (int i = 0; i < first.size(); i++) {
            RelDataTypeField expected = first.get(i);
            RelDataTypeField actual = second.get(i);
            if (!expected.getName().equalsIgnoreCase(actual.getName())) {
                throw new RuntimeException(nameMismatch + expected.getName() + " != " + actual.getName());
            }
            if (!compatibleTypes(expected.getType(), actual.getType(), false)) {
                throw new RuntimeException(typeMismatch + expected.getType() + " != " + actual.getType());
            }
        }
    }

    static boolean compatibleTypes(RelDataType first, RelDataType second, boolean allowRowConversion) {
        SqlTypeName firstType = first.getSqlTypeName();
        SqlTypeName secondType = second.getSqlTypeName();
        if (firstType != secondType) {
            if (allowRowConversion) {
                return SqlTypeName.STRING_TYPES.contains(firstType)
                        || (SqlTypeName.NUMERIC_TYPES.contains(firstType) && SqlTypeName.NUMERIC_TYPES.contains(secondType));
            }
            return SqlTypeName.STRING_TYPES.contains(firstType) && SqlTypeName.STRING_TYPES.contains(secondType);
        }
        switch (firstType) {
            case ARRAY:
                return compatibleTypes(first.getComponentType(), second.getComponentType(), allowRowConversion);
            case MULTISET:
                // Row adaptation passes MULTISET values through.
                return allowRowConversion || compatibleTypes(first.getComponentType(), second.getComponentType(), false);
            case MAP:
                return compatibleTypes(first.getKeyType(), second.getKeyType(), allowRowConversion)
                        && compatibleTypes(first.getValueType(), second.getValueType(), allowRowConversion);
            default:
                return true;
        }
    }

    /** Common schema for alternative results, preserving nullability and type bounds from both. */
    public static List<RelDataTypeField> mergeTableSchemas(
            List<RelDataTypeField> first, List<RelDataTypeField> second) {
        checkTableSchemaSame(first, second);
        RelDataTypeFactory factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        List<RelDataTypeField> merged = new ArrayList<>();
        for (int i = 0; i < first.size(); i++) {
            RelDataType type = factory.leastRestrictive(List.of(first.get(i).getType(), second.get(i).getType()));
            if (type == null) {
                throw new IllegalArgumentException("cannot merge field types: " + first.get(i) + " and " + second.get(i));
            }
            merged.add(new RelDataTypeFieldImpl(first.get(i).getName(), i, type));
        }
        return merged;
    }

    public static void checkTableSchemaIdentical(
            List<RelDataTypeField> referenceFields, List<RelDataTypeField> fields, int tableIndex) {
        if (referenceFields.size() != fields.size()) {
            throw new IllegalArgumentException("Table " + tableIndex + " has different column count than table 0");
        }
        for (int i = 0; i < referenceFields.size(); i++) {
            RelDataTypeField expected = referenceFields.get(i);
            RelDataTypeField actual = fields.get(i);
            if (!expected.getName().equalsIgnoreCase(actual.getName())) {
                throw new IllegalArgumentException("Column name mismatch at index " + i
                        + ": " + expected.getName() + " vs " + actual.getName());
            }
            if (!expected.getType().equals(actual.getType())) {
                throw new IllegalArgumentException("Column type mismatch at index " + i
                        + ": " + expected.getType() + " vs " + actual.getType());
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
}
