package com.sqlrec.frontend.thrift;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.common.utils.HiveTableUtils;
import com.sqlrec.db.MetadataAccess;
import com.sqlrec.executor.SqlProcessResult;
import com.sqlrec.frontend.utils.JdbcMetadataSchema;
import com.sqlrec.frontend.utils.JdbcTypeUtils;
import com.sqlrec.udf.config.FunctionConfigs;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;

import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/** Hive JDBC metadata for SQLRec-visible persistent objects. No Gateway calls or UDF class loading. */
public final class JdbcMetadataExecutor {
    private record PrimaryKey(String column, int sequence, String name) {}
    private final MetadataAccess metadata;

    public JdbcMetadataExecutor(MetadataAccess metadata) {
        this.metadata = metadata;
    }

    private static String catalog() {
        return Consts.HIVE_CATALOG_NAME;
    }

    public SqlProcessResult catalogs() {
        return JdbcMetadataSchema.CATALOGS.rows(List.<Object[]>of(new Object[]{catalog()}));
    }

    public SqlProcessResult schemas(String requestedCatalog, String schemaPattern) throws Exception {
        List<Object[]> result = new ArrayList<>();
        if (acceptsCatalog(requestedCatalog)) {
            for (String database : databases(schemaPattern)) {
                result.add(new Object[]{database, catalog()});
            }
        }
        return JdbcMetadataSchema.SCHEMAS.rows(result);
    }

    public SqlProcessResult tableTypes() {
        return JdbcMetadataSchema.TABLE_TYPES.rows(List.of(new Object[]{"TABLE"}, new Object[]{"VIEW"}));
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
        return JdbcMetadataSchema.TABLES.rows(result);
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
        return JdbcMetadataSchema.COLUMNS.rows(result);
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
        int type = JdbcTypeUtils.jdbcType(rawType);
        int size = JdbcTypeUtils.precision(rawType);
        boolean nullable = !primaryKeys.contains(column.getName())
                && JdbcTypeUtils.isNullable(rawType);
        String prefix = "flink.schema." + index + ".";
        String typeName = JdbcTypeUtils.typeName(rawType);
        Integer characterLength = type == Types.VARCHAR || type == Types.CHAR ? size : null;
        boolean generated = parameters(table).containsKey(prefix + "expr");
        return new Object[]{catalog(), database, table.getTableName(), column.getName(),
                type, typeName, size, null, JdbcTypeUtils.scale(rawType), JdbcTypeUtils.numericRadix(type),
                nullable ? DatabaseMetaData.columnNullable : DatabaseMetaData.columnNoNulls,
                columnComment(table, column.getName(), prefix), null, null, null, characterLength,
                index + 1, nullable ? "YES" : "NO", null, null, null, null, "NO", generated ? "YES" : "NO"};
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
        return JdbcMetadataSchema.FUNCTIONS.rows(result);
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
        return JdbcMetadataSchema.PRIMARY_KEYS.rows(result);
    }

    public SqlProcessResult typeInfo() {
        List<Object[]> result = new ArrayList<>();
        for (String type : List.of("BOOLEAN", "TINYINT", "SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE",
                "DECIMAL", "STRING", "CHAR", "VARCHAR", "BINARY", "DATE", "TIMESTAMP", "ARRAY", "MAP", "STRUCT")) {
            result.add(typeInfoRow(type));
        }
        return JdbcMetadataSchema.TYPE_INFO.rows(result);
    }

    private static Object[] typeInfoRow(String type) {
        int code = JdbcTypeUtils.jdbcType(type);
        boolean character = code == Types.CHAR || code == Types.VARCHAR;
        String quote = character ? "'" : null;
        String createParams = type.equals("DECIMAL") ? "precision,scale" : character ? "length" : null;
        short maxScale = (short) (type.equals("DECIMAL") ? 38 : type.equals("TIMESTAMP") ? 9 : 0);
        return new Object[]{type, code, JdbcTypeUtils.precision(type), quote, quote, createParams,
                (short) DatabaseMetaData.typeNullable, character, (short) DatabaseMetaData.typeSearchable,
                false, false, false, type, (short) 0, maxScale, null, null, JdbcTypeUtils.numericRadix(code)};
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

}
