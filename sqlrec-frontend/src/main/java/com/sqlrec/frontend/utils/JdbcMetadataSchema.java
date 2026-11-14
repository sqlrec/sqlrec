package com.sqlrec.frontend.utils;

import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.executor.SqlProcessResult;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.sql.type.SqlTypeName.BOOLEAN;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.calcite.sql.type.SqlTypeName.SMALLINT;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;

/** Ordered JDBC result columns and their Calcite types, defined together. */
public final class JdbcMetadataSchema {
    public static final JdbcMetadataSchema CATALOGS = new JdbcMetadataSchema(
            column("TABLE_CAT", VARCHAR));

    public static final JdbcMetadataSchema SCHEMAS = new JdbcMetadataSchema(
            column("TABLE_SCHEM", VARCHAR),
            column("TABLE_CATALOG", VARCHAR));

    public static final JdbcMetadataSchema TABLE_TYPES = new JdbcMetadataSchema(
            column("TABLE_TYPE", VARCHAR));

    public static final JdbcMetadataSchema TABLES = new JdbcMetadataSchema(
            column("TABLE_CAT", VARCHAR),
            column("TABLE_SCHEM", VARCHAR),
            column("TABLE_NAME", VARCHAR),
            column("TABLE_TYPE", VARCHAR),
            column("REMARKS", VARCHAR),
            column("TYPE_CAT", VARCHAR),
            column("TYPE_SCHEM", VARCHAR),
            column("TYPE_NAME", VARCHAR),
            column("SELF_REFERENCING_COL_NAME", VARCHAR),
            column("REF_GENERATION", VARCHAR));

    public static final JdbcMetadataSchema COLUMNS = new JdbcMetadataSchema(
            column("TABLE_CAT", VARCHAR),
            column("TABLE_SCHEM", VARCHAR),
            column("TABLE_NAME", VARCHAR),
            column("COLUMN_NAME", VARCHAR),
            column("DATA_TYPE", INTEGER),
            column("TYPE_NAME", VARCHAR),
            column("COLUMN_SIZE", INTEGER),
            column("BUFFER_LENGTH", INTEGER),
            column("DECIMAL_DIGITS", INTEGER),
            column("NUM_PREC_RADIX", INTEGER),
            column("NULLABLE", INTEGER),
            column("REMARKS", VARCHAR),
            column("COLUMN_DEF", VARCHAR),
            column("SQL_DATA_TYPE", INTEGER),
            column("SQL_DATETIME_SUB", INTEGER),
            column("CHAR_OCTET_LENGTH", INTEGER),
            column("ORDINAL_POSITION", INTEGER),
            column("IS_NULLABLE", VARCHAR),
            column("SCOPE_CATALOG", VARCHAR),
            column("SCOPE_SCHEMA", VARCHAR),
            column("SCOPE_TABLE", VARCHAR),
            column("SOURCE_DATA_TYPE", SMALLINT),
            column("IS_AUTOINCREMENT", VARCHAR),
            column("IS_GENERATEDCOLUMN", VARCHAR));

    public static final JdbcMetadataSchema FUNCTIONS = new JdbcMetadataSchema(
            column("FUNCTION_CAT", VARCHAR),
            column("FUNCTION_SCHEM", VARCHAR),
            column("FUNCTION_NAME", VARCHAR),
            column("REMARKS", VARCHAR),
            column("FUNCTION_TYPE", SMALLINT),
            column("SPECIFIC_NAME", VARCHAR));

    public static final JdbcMetadataSchema PRIMARY_KEYS = new JdbcMetadataSchema(
            column("TABLE_CAT", VARCHAR),
            column("TABLE_SCHEM", VARCHAR),
            column("TABLE_NAME", VARCHAR),
            column("COLUMN_NAME", VARCHAR),
            column("KEY_SEQ", SMALLINT),
            column("PK_NAME", VARCHAR));

    public static final JdbcMetadataSchema TYPE_INFO = new JdbcMetadataSchema(
            column("TYPE_NAME", VARCHAR),
            column("DATA_TYPE", INTEGER),
            column("PRECISION", INTEGER),
            column("LITERAL_PREFIX", VARCHAR),
            column("LITERAL_SUFFIX", VARCHAR),
            column("CREATE_PARAMS", VARCHAR),
            column("NULLABLE", SMALLINT),
            column("CASE_SENSITIVE", BOOLEAN),
            column("SEARCHABLE", SMALLINT),
            column("UNSIGNED_ATTRIBUTE", BOOLEAN),
            column("FIXED_PREC_SCALE", BOOLEAN),
            column("AUTO_INCREMENT", BOOLEAN),
            column("LOCAL_TYPE_NAME", VARCHAR),
            column("MINIMUM_SCALE", SMALLINT),
            column("MAXIMUM_SCALE", SMALLINT),
            column("SQL_DATA_TYPE", INTEGER),
            column("SQL_DATETIME_SUB", INTEGER),
            column("NUM_PREC_RADIX", INTEGER));

    private record Column(String name, SqlTypeName type) {}

    private final List<Column> columns;

    private JdbcMetadataSchema(Column... columns) {
        this.columns = List.of(columns);
    }

    private static Column column(String name, SqlTypeName type) {
        return new Column(name, type);
    }

    public SqlProcessResult rows(List<Object[]> values) {
        List<RelDataTypeField> fields = new ArrayList<>(columns.size());
        for (Column column : columns) {
            fields.add(DataTypeUtils.getRelDataTypeField(column.name(), fields.size(), column.type()));
        }
        return SqlProcessResult.of(Linq4j.asEnumerable(values), fields);
    }
}
