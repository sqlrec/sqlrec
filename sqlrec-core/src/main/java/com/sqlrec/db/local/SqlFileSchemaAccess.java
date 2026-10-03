package com.sqlrec.db.local;

import com.sqlrec.common.config.Consts;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.db.FlinkTableDdl;
import com.sqlrec.db.MetadataQueryUtils;
import com.sqlrec.db.SchemaAccess;
import com.sqlrec.executor.SqlProcessResult;
import com.sqlrec.utils.SchemaUtils;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.sql.parser.ddl.SqlCreateFunction;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlCreateTableAs;
import org.apache.flink.sql.parser.ddl.SqlCreateTableLike;
import org.apache.flink.sql.parser.dql.SqlRichDescribeTable;
import org.apache.flink.sql.parser.dql.SqlShowCreateTable;
import org.apache.flink.sql.parser.dql.SqlShowFunctions;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;

import java.util.*;

public class SqlFileSchemaAccess implements SchemaAccess {
    private final Set<String> databases;
    private final Map<String, List<Table>> databaseTables;
    private final Map<String, List<Function>> databaseFunctions;
    private final Map<String, Map<String, FileTable>> tableDefinitions = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    private final long loadTime = System.currentTimeMillis();

    // Keep the SQL text before validation, which can change primary-key column nullability in the AST.
    private record FileTable(SqlCreateTable definition, String createSql) {}

    public SqlFileSchemaAccess(List<SqlNode> tableNodes, List<SqlNode> udfFunctionNodes) {
        this.databases = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        this.databases.add(Consts.DEFAULT_SCHEMA_NAME);
        this.databaseTables = buildDatabaseTables(tableNodes, this.databases);
        this.databaseFunctions = buildDatabaseFunctions(udfFunctionNodes, this.databases);
    }

    @Override
    public synchronized SqlProcessResult executeMetadataQuery(SqlNode node, String database) throws Exception {
        if (node instanceof SqlShowCreateTable show) {
            FileTable table = tableDefinition(show.getTableName().names.toArray(String[]::new), database);
            return SqlProcessResult.msg(table.createSql(), "result");
        }
        if (node instanceof SqlRichDescribeTable describe) {
            SqlCreateTable table = tableDefinition(describe.fullTableName(), database).definition();
            if (table instanceof SqlCreateTableAs || table instanceof SqlCreateTableLike) {
                throw new UnsupportedOperationException("DESCRIBE requires explicit columns in SQL file metadata; CTAS and LIKE are not resolved");
            }
            return MetadataQueryUtils.describeTable(FlinkTableDdl.create(table).getResolvedSchema());
        }
        if (node instanceof SqlShowFunctions show) {
            String name = MetadataQueryUtils.databaseName(show.fullDatabaseName(), database);
            requireDatabase(name);
            return MetadataQueryUtils.showFunctions(show,
                    getFunctions(name).stream().map(Function::getFunctionName).toList());
        }
        return SchemaAccess.super.executeMetadataQuery(node, database);
    }

    private FileTable tableDefinition(String[] names, String database) throws NoSuchObjectException {
        ObjectPath path = MetadataQueryUtils.objectPath(names, database);
        requireDatabase(path.getDatabaseName());
        FileTable table = tableDefinitions.getOrDefault(path.getDatabaseName(), Map.of()).get(path.getObjectName());
        if (table == null) throw new NoSuchObjectException("Table does not exist: " + path.getFullName());
        return table;
    }

    private void requireDatabase(String database) throws NoSuchObjectException {
        if (!databases.contains(database)) throw new NoSuchObjectException("Database does not exist: " + database);
    }

    @Override
    public List<String> getDatabases() throws Exception {
        return new ArrayList<>(databases);
    }

    @Override
    public List<Table> getTables(String database) throws Exception {
        return new ArrayList<>(databaseTables.getOrDefault(database, Collections.emptyList()));
    }

    @Override
    public Table getTable(String database, String tableName) throws Exception {
        for (Table t : getTables(database)) {
            if (t.getTableName().equalsIgnoreCase(tableName)) {
                return t;
            }
        }
        throw new NoSuchObjectException("table not found " + tableName + " from db " + database);
    }

    @Override
    public List<Function> getFunctions(String database) throws Exception {
        return new ArrayList<>(databaseFunctions.getOrDefault(database, Collections.emptyList()));
    }

    @Override
    public Function getFunction(String database, String funName) throws Exception {
        for (Function fun : getFunctions(database)) {
            if (fun.getFunctionName().equalsIgnoreCase(funName)) {
                return fun;
            }
        }
        throw new NoSuchObjectException("function not found " + funName + " from db " + database);
    }

    @Override
    public long getTableUpdateTime(String database, String table) {
        return loadTime;
    }

    @Override
    public List<String> getPartitionPaths(String database, String table, String partitionFilter) throws Exception {
        throw new UnsupportedOperationException("getPartitionPaths is not supported in SqlFileSchemaAccess");
    }

    private Map<String, List<Table>> buildDatabaseTables(List<SqlNode> tableNodes, Set<String> databases) {
        Map<String, List<Table>> result = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (SqlNode node : tableNodes) {
            if (node instanceof SqlCreateTable createTable) {
                ObjectPath path = MetadataQueryUtils.objectPath(createTable.fullTableName(), Consts.DEFAULT_SCHEMA_NAME);
                databases.add(path.getDatabaseName());
                tableDefinitions.computeIfAbsent(path.getDatabaseName(), key -> new TreeMap<>(String.CASE_INSENSITIVE_ORDER))
                        .putIfAbsent(path.getObjectName(), new FileTable(createTable, CompileManager.getSqlStr(createTable)));
                Table hmsTable = SchemaUtils.parseCreateTableToHmsTable(createTable);
                if (hmsTable == null) {
                    // Definitions without physical columns can still be listed and shown.
                    hmsTable = new Table();
                    hmsTable.setTableName(path.getObjectName());
                    hmsTable.setSd(new StorageDescriptor());
                    hmsTable.getSd().setCols(List.of());
                    hmsTable.setParameters(Map.of());
                }
                hmsTable.setDbName(path.getDatabaseName());
                result.computeIfAbsent(path.getDatabaseName(), k -> new ArrayList<>()).add(hmsTable);
            }
        }
        return result;
    }

    private Map<String, List<Function>> buildDatabaseFunctions(List<SqlNode> udfFunctionNodes, Set<String> databases) {
        Map<String, List<Function>> result = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (SqlNode node : udfFunctionNodes) {
            if (node instanceof SqlCreateFunction) {
                SqlCreateFunction createFunc = (SqlCreateFunction) node;
                String[] identifiers = createFunc.getFunctionIdentifier();
                ObjectPath path = MetadataQueryUtils.objectPath(identifiers, Consts.DEFAULT_SCHEMA_NAME);
                String database = path.getDatabaseName();
                databases.add(database);
                String functionName = path.getObjectName();
                String className = SchemaUtils.getValueOfStringLiteral(createFunc.getFunctionClassName());
                Function func = new Function();
                func.setFunctionName(functionName);
                func.setClassName(className);
                func.setDbName(database);
                result.computeIfAbsent(database, k -> new ArrayList<>()).add(func);
            }
        }
        return result;
    }
}
