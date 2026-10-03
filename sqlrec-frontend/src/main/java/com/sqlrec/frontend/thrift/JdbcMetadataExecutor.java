package com.sqlrec.frontend.thrift;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.common.utils.HiveTableUtils;
import com.sqlrec.db.MetadataAccess;
import com.sqlrec.executor.SqlProcessResult;
import com.sqlrec.udf.config.FunctionConfigs;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;

import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/** Hive JDBC metadata for SQLRec-visible persistent objects. No Gateway calls or UDF class loading. */
public final class JdbcMetadataExecutor {
    private record PrimaryKey(String column, int sequence, String name) {}
    private static final Pattern TYPE_PRECISION = Pattern.compile("(?i)^[A-Z_]+\\s*\\(\\s*(\\d+)");
    private static final Pattern DECIMAL_SCALE = Pattern.compile("(?i)^DECIMAL\\s*\\(\\s*\\d+\\s*,\\s*(\\d+)");
    private static final String CATALOG_COLUMNS = "TABLE_CAT";
    private static final String SCHEMA_COLUMNS = "TABLE_SCHEM,TABLE_CATALOG";
    private static final String TABLE_TYPE_COLUMNS = "TABLE_TYPE";
    private static final String TABLE_COLUMNS =
            "TABLE_CAT,TABLE_SCHEM,TABLE_NAME,TABLE_TYPE,REMARKS,TYPE_CAT,TYPE_SCHEM,TYPE_NAME,"
            + "SELF_REFERENCING_COL_NAME,REF_GENERATION";
    private static final String COLUMN_RESULT_COLUMNS =
            "TABLE_CAT,TABLE_SCHEM,TABLE_NAME,COLUMN_NAME,DATA_TYPE,TYPE_NAME,COLUMN_SIZE,"
            + "BUFFER_LENGTH,DECIMAL_DIGITS,NUM_PREC_RADIX,NULLABLE,REMARKS,COLUMN_DEF,SQL_DATA_TYPE,"
            + "SQL_DATETIME_SUB,CHAR_OCTET_LENGTH,ORDINAL_POSITION,IS_NULLABLE,SCOPE_CATALOG,"
            + "SCOPE_SCHEMA,SCOPE_TABLE,SOURCE_DATA_TYPE,IS_AUTOINCREMENT,IS_GENERATEDCOLUMN";
    private static final String FUNCTION_COLUMNS = "FUNCTION_CAT,FUNCTION_SCHEM,FUNCTION_NAME,REMARKS,FUNCTION_TYPE,SPECIFIC_NAME";
    private static final String PRIMARY_KEY_COLUMNS = "TABLE_CAT,TABLE_SCHEM,TABLE_NAME,COLUMN_NAME,KEY_SEQ,PK_NAME";
    private static final String TYPE_INFO_COLUMNS =
            "TYPE_NAME,DATA_TYPE,PRECISION,LITERAL_PREFIX,LITERAL_SUFFIX,CREATE_PARAMS,NULLABLE,"
            + "CASE_SENSITIVE,SEARCHABLE,UNSIGNED_ATTRIBUTE,FIXED_PREC_SCALE,AUTO_INCREMENT,"
            + "LOCAL_TYPE_NAME,MINIMUM_SCALE,MAXIMUM_SCALE,SQL_DATA_TYPE,SQL_DATETIME_SUB,NUM_PREC_RADIX";
    private static final Map<String, SqlTypeName> COLUMN_TYPES = columnTypes();
    private static final Map<String, SqlTypeName> TYPE_INFO_TYPES = typeInfoTypes();
    private final MetadataAccess metadata;

    public JdbcMetadataExecutor(MetadataAccess metadata) {
        this.metadata = metadata;
    }

    private static String catalog() {
        return Consts.HIVE_CATALOG_NAME;
    }

    public SqlProcessResult catalogs() {
        return rows(CATALOG_COLUMNS, List.<Object[]>of(new Object[]{catalog()}));
    }

    public SqlProcessResult schemas(String requestedCatalog, String schemaPattern) throws Exception {
        List<Object[]> result = new ArrayList<>();
        if (acceptsCatalog(requestedCatalog)) {
            for (String database : databases(schemaPattern)) {
                result.add(new Object[]{database, catalog()});
            }
        }
        return rows(SCHEMA_COLUMNS, result);
    }

    public SqlProcessResult tableTypes() {
        return rows(TABLE_TYPE_COLUMNS, List.of(new Object[]{"TABLE"}, new Object[]{"VIEW"}));
    }

    public SqlProcessResult tables(String requestedCatalog, String schemaPattern,
            String tablePattern, List<String> types) throws Exception {
        Predicate<String> tableFilter = nameFilter(tablePattern);
        List<Object[]> result = new ArrayList<>();
        if (acceptsCatalog(requestedCatalog)) {
            for (String database : databases(schemaPattern)) {
                for (Table table : metadata.getTables(database)) {
                    String type = "VIRTUAL_VIEW".equals(table.getTableType()) ? "VIEW" : "TABLE";
                    if (tableFilter.test(table.getTableName())
                            && (types == null || types.stream().anyMatch(type::equalsIgnoreCase))) {
                        result.add(new Object[]{catalog(), database, table.getTableName(), type,
                                parameters(table).getOrDefault("flink.comment", parameters(table).get("comment")),
                                null, null, null, null, null});
                    }
                }
            }
        }
        return rows(TABLE_COLUMNS, result);
    }

    public SqlProcessResult columns(String requestedCatalog, String schemaPattern,
            String tablePattern, String columnPattern) throws Exception {
        Predicate<String> tableFilter = nameFilter(tablePattern);
        Predicate<String> columnFilter = nameFilter(columnPattern);
        List<Object[]> result = new ArrayList<>();
        if (acceptsCatalog(requestedCatalog)) {
            for (String database : databases(schemaPattern)) {
                for (Table table : metadata.getTables(database)) {
                    if (!tableFilter.test(table.getTableName())) {
                        continue;
                    }
                    result.addAll(columnRows(database, table, columnFilter));
                }
            }
        }
        return rows(COLUMN_RESULT_COLUMNS, COLUMN_TYPES, result);
    }

    private static List<Object[]> columnRows(String database, Table table, Predicate<String> filter) throws Exception {
        List<FieldSchema> columns = HiveTableUtils.getMetadataFields(table);
        List<String> primaryKeys = loadPrimaryKeys(table).stream().map(PrimaryKey::column).toList();
        List<Object[]> result = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            FieldSchema column = columns.get(i);
            if (filter.test(column.getName())) {
                result.add(columnRow(database, table, column, i, primaryKeys));
            }
        }
        return result;
    }

    private static Object[] columnRow(String database, Table table, FieldSchema column,
            int index, List<String> primaryKeys) {
        String rawType = column.getType();
        int type = jdbcType(rawType);
        int size = precision(rawType);
        boolean nullable = !primaryKeys.contains(column.getName())
                && !rawType.toUpperCase(Locale.ROOT).endsWith("NOT NULL");
        String prefix = "flink.schema." + index + ".";
        String typeName = rawType.replaceAll("(?i)\\s+NOT\\s+NULL$", "");
        Integer characterLength = type == Types.VARCHAR || type == Types.CHAR ? size : null;
        boolean generated = parameters(table).containsKey(prefix + "expr");
        return new Object[]{catalog(), database, table.getTableName(), column.getName(),
                type, typeName, size, null, scale(rawType), numericRadix(type),
                nullable ? DatabaseMetaData.columnNullable : DatabaseMetaData.columnNoNulls,
                columnComment(table, column.getName(), prefix), null, null, null, characterLength,
                index + 1, nullable ? "YES" : "NO", null, null, null, null, "NO", generated ? "YES" : "NO"};
    }

    private static Map<String, SqlTypeName> columnTypes() {
        Map<String, SqlTypeName> types = integerColumns("DATA_TYPE,COLUMN_SIZE,BUFFER_LENGTH,DECIMAL_DIGITS,NUM_PREC_RADIX,NULLABLE,SQL_DATA_TYPE,SQL_DATETIME_SUB,CHAR_OCTET_LENGTH,ORDINAL_POSITION");
        types.put("SOURCE_DATA_TYPE", SqlTypeName.SMALLINT);
        return Map.copyOf(types);
    }

    public SqlProcessResult functions(String requestedCatalog, String schemaPattern,
            String functionPattern) throws Exception {
        Predicate<String> functionFilter = nameFilter(functionPattern);
        List<Object[]> result = new ArrayList<>();
        if (acceptsCatalog(requestedCatalog)) {
            for (String database : databases(schemaPattern)) {
                result.addAll(functionRows(database, functionFilter));
            }
        }
        return rows(FUNCTION_COLUMNS,
                Map.of("FUNCTION_TYPE", SqlTypeName.SMALLINT), result);
    }

    private List<Object[]> functionRows(String database, Predicate<String> filter) throws Exception {
        Map<String, Function> functions = new LinkedHashMap<>();
        for (Function function : metadata.getFunctions(database)) {
            functions.put(function.getFunctionName(), function);
        }
        List<Object[]> result = new ArrayList<>();
        for (Function function : functions.values()) {
            if (filter.test(function.getFunctionName())) {
                result.add(functionRow(database, function.getFunctionName(), function.getClassName(),
                        DatabaseMetaData.functionResultUnknown));
            }
        }
        result.addAll(builtinFunctionRows(database, filter, functions,
                FunctionConfigs.DEFAULT_JAVA_FUNCTION_CONFIGS, DatabaseMetaData.functionReturnsTable));
        // SQLRec's procedural SQL functions are not JDBC scalar functions.
        result.addAll(builtinFunctionRows(database, filter, functions,
                FunctionConfigs.DEFAULT_SCALAR_FUNCTION_CONFIGS, DatabaseMetaData.functionNoTable));
        return result;
    }

    private static List<Object[]> builtinFunctionRows(
            String database, Predicate<String> filter, Map<String, Function> persistentFunctions,
            Map<String, String> builtins, int type) {
        List<Object[]> result = new ArrayList<>();
        for (var entry : builtins.entrySet()) {
            if (!persistentFunctions.containsKey(entry.getKey()) && filter.test(entry.getKey())) {
                result.add(functionRow(database, entry.getKey(), entry.getValue(), type));
            }
        }
        return result;
    }

    private static Object[] functionRow(String database, String name, String className, int type) {
        return new Object[]{catalog(), database, name, className, (short) type, database + "." + name};
    }

    public SqlProcessResult primaryKeys(String requestedCatalog, String database, String tableName) throws Exception {
        List<Object[]> result = new ArrayList<>();
        if (acceptsCatalog(requestedCatalog)) {
            if (tableName == null || tableName.isEmpty()) {
                throw new IllegalArgumentException("A table name is required for GetPrimaryKeys");
            }
            List<String> databases = database == null ? metadata.getDatabases() : List.of(database);
            for (String db : databases) {
                Table table;
                try {
                    table = metadata.getTable(db, tableName);
                } catch (NoSuchObjectException e) {
                    continue;
                }
                if (table != null) {
                    for (PrimaryKey key : loadPrimaryKeys(table)) {
                        result.add(new Object[]{catalog(), db, tableName, key.column(), (short) key.sequence(), key.name()});
                    }
                }
            }
        }
        return rows(PRIMARY_KEY_COLUMNS,
                Map.of("KEY_SEQ", SqlTypeName.SMALLINT), result);
    }

    public SqlProcessResult typeInfo() {
        List<Object[]> result = new ArrayList<>();
        for (String type : List.of("BOOLEAN", "TINYINT", "SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE",
                "DECIMAL", "STRING", "CHAR", "VARCHAR", "BINARY", "DATE", "TIMESTAMP", "ARRAY", "MAP", "STRUCT")) {
            result.add(typeInfoRow(type));
        }
        return rows(TYPE_INFO_COLUMNS, TYPE_INFO_TYPES, result);
    }

    private static Object[] typeInfoRow(String type) {
        int code = jdbcType(type);
        boolean character = code == Types.CHAR || code == Types.VARCHAR;
        String quote = character ? "'" : null;
        String createParams = type.equals("DECIMAL") ? "precision,scale" : character ? "length" : null;
        short maxScale = (short) (type.equals("DECIMAL") ? 38 : type.equals("TIMESTAMP") ? 9 : 0);
        return new Object[]{type, code, precision(type), quote, quote, createParams,
                (short) DatabaseMetaData.typeNullable, character, (short) DatabaseMetaData.typeSearchable,
                false, false, false, type, (short) 0, maxScale, null, null, numericRadix(code)};
    }

    private static Map<String, SqlTypeName> typeInfoTypes() {
        Map<String, SqlTypeName> types = integerColumns("DATA_TYPE,PRECISION,SQL_DATA_TYPE,SQL_DATETIME_SUB,NUM_PREC_RADIX");
        for (String name : List.of("NULLABLE", "SEARCHABLE", "MINIMUM_SCALE", "MAXIMUM_SCALE")) {
            types.put(name, SqlTypeName.SMALLINT);
        }
        for (String name : List.of("CASE_SENSITIVE", "UNSIGNED_ATTRIBUTE", "FIXED_PREC_SCALE", "AUTO_INCREMENT")) {
            types.put(name, SqlTypeName.BOOLEAN);
        }
        return Map.copyOf(types);
    }

    private List<String> databases(String pattern) throws Exception {
        return metadata.getDatabases().stream().filter(nameFilter(pattern)).sorted().toList();
    }

    private static boolean acceptsCatalog(String requested) {
        return requested == null || requested.isEmpty() || requested.equalsIgnoreCase(catalog());
    }

    /** JDBC LIKE patterns, including escaped wildcard characters; input is never interpreted as regex. */
    static boolean matches(String value, String pattern) {
        return nameFilter(pattern).test(value);
    }

    private static Predicate<String> nameFilter(String pattern) {
        if (pattern == null) {
            return value -> true;
        }
        StringBuilder expression = new StringBuilder("^");
        boolean escaped = false;
        for (char character : pattern.toCharArray()) {
            if (!escaped && character == '\\') {
                escaped = true;
            } else {
                expression.append(!escaped && character == '%' ? ".*"
                        : !escaped && character == '_' ? "." : Pattern.quote(String.valueOf(character)));
                escaped = false;
            }
        }
        if (escaped) {
            expression.append(Pattern.quote("\\"));
        }
        Pattern compiled = Pattern.compile(expression.append('$').toString(), Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        return value -> compiled.matcher(value).matches();
    }

    private static Map<String, String> parameters(Table table) {
        return table.getParameters() == null ? Map.of() : table.getParameters();
    }

    private static String columnComment(Table table, String name, String prefix) {
        if (parameters(table).containsKey(prefix + "name")) {
            return parameters(table).get(prefix + "comment");
        }
        List<org.apache.hadoop.hive.metastore.api.FieldSchema> fields = new ArrayList<>();
        if (table.getSd() != null && table.getSd().getCols() != null) {
            fields.addAll(table.getSd().getCols());
        }
        if (table.getPartitionKeys() != null) {
            fields.addAll(table.getPartitionKeys());
        }
        return fields.stream().filter(field -> field.getName().equals(name))
                .map(org.apache.hadoop.hive.metastore.api.FieldSchema::getComment)
                .filter(java.util.Objects::nonNull).findFirst().orElse(null);
    }

    private static List<PrimaryKey> loadPrimaryKeys(Table table) throws Exception {
        String keys = parameters(table).get("flink.schema.primary-key.columns");
        List<PrimaryKey> result = new ArrayList<>();
        if (keys != null && !keys.isBlank()) {
            String[] columns = keys.split(",");
            String name = parameters(table).get("flink.schema.primary-key.name");
            for (int i = 0; i < columns.length; i++) {
                result.add(new PrimaryKey(columns[i].trim(), i + 1, name));
            }
        } else if (!SqlRecConfigs.isFileSystemMetadata() && ("hive".equalsIgnoreCase(
                HiveTableUtils.getTableConnector(table)) || !parameters(table).containsKey("flink.connector"))) {
            for (var key : com.sqlrec.db.remote.HmsClient.getPrimaryKeys(table.getDbName(), table.getTableName())) {
                result.add(new PrimaryKey(key.getColumn_name(), key.getKey_seq(), key.getPk_name()));
            }
        }
        result.sort(java.util.Comparator.comparingInt(PrimaryKey::sequence));
        return result;
    }

    private static String baseType(String type) {
        return type.strip().toUpperCase(Locale.ROOT).split("[<(\\s]", 2)[0];
    }

    static int jdbcType(String type) {
        return switch (baseType(type)) {
            case "BOOLEAN" -> Types.BOOLEAN;
            case "TINYINT" -> Types.TINYINT;
            case "SMALLINT" -> Types.SMALLINT;
            case "INT", "INTEGER" -> Types.INTEGER;
            case "BIGINT" -> Types.BIGINT;
            case "FLOAT" -> Types.FLOAT;
            case "DOUBLE" -> Types.DOUBLE;
            case "DECIMAL" -> Types.DECIMAL;
            case "CHAR" -> Types.CHAR;
            case "VARCHAR", "STRING" -> Types.VARCHAR;
            case "BINARY", "VARBINARY", "BYTES" -> Types.BINARY;
            case "DATE" -> Types.DATE;
            case "TIME" -> Types.TIME;
            case "TIMESTAMP", "TIMESTAMP_LTZ" -> Types.TIMESTAMP;
            case "ARRAY" -> Types.ARRAY;
            case "ROW", "STRUCT" -> Types.STRUCT;
            default -> Types.JAVA_OBJECT;
        };
    }

    private static int precision(String type) {
        String base = baseType(type);
        if (base.equals("TIME") || base.equals("TIMESTAMP") || base.equals("TIMESTAMP_LTZ")) {
            int fraction = fractionalPrecision(type);
            return (base.equals("TIME") ? 8 : 19) + (fraction == 0 ? 0 : fraction + 1);
        }
        var match = TYPE_PRECISION.matcher(type);
        if (match.find()) {
            return Integer.parseInt(match.group(1));
        }
        return switch (baseType(type)) {
            case "TINYINT" -> 3;
            case "SMALLINT" -> 5;
            case "INT", "INTEGER" -> 10;
            case "BIGINT" -> 19;
            case "FLOAT" -> 7;
            case "DOUBLE" -> 15;
            case "DECIMAL" -> 38;
            case "DATE" -> 10;
            case "BOOLEAN" -> 1;
            default -> Integer.MAX_VALUE;
        };
    }

    private static Integer scale(String type) {
        return switch (baseType(type)) {
            case "TIME", "TIMESTAMP", "TIMESTAMP_LTZ" -> fractionalPrecision(type);
            case "DECIMAL" -> {
                var match = DECIMAL_SCALE.matcher(type);
                yield match.find() ? Integer.valueOf(match.group(1)) : 0;
            }
            case "TINYINT", "SMALLINT", "INT", "INTEGER", "BIGINT" -> 0;
            default -> null;
        };
    }

    private static int fractionalPrecision(String type) {
        var match = TYPE_PRECISION.matcher(type);
        return match.find() ? Integer.parseInt(match.group(1)) : baseType(type).equals("TIME") ? 0 : 9;
    }

    private static Integer numericRadix(int type) {
        return switch (type) {
            case Types.TINYINT, Types.SMALLINT, Types.INTEGER, Types.BIGINT,
                    Types.FLOAT, Types.DOUBLE, Types.DECIMAL -> 10;
            default -> null;
        };
    }

    private static Map<String, SqlTypeName> integerColumns(String columns) {
        Map<String, SqlTypeName> types = new LinkedHashMap<>();
        for (String name : columns.split(",")) {
            types.put(name, SqlTypeName.INTEGER);
        }
        return types;
    }

    private static SqlProcessResult rows(String columns, List<Object[]> values) {
        return rows(columns, Map.of(), values);
    }

    private static SqlProcessResult rows(String columns, Map<String, SqlTypeName> types, List<Object[]> values) {
        List<RelDataTypeField> fields = new ArrayList<>();
        for (String name : columns.split(",")) {
            fields.add(DataTypeUtils.getRelDataTypeField(name, fields.size(), types.getOrDefault(name, SqlTypeName.VARCHAR)));
        }
        return SqlProcessResult.of(Linq4j.asEnumerable(values), fields);
    }
}
