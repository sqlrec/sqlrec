package com.sqlrec.db.local;

import com.sqlrec.db.SchemaAccess;
import com.sqlrec.utils.SchemaUtils;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.sql.parser.ddl.SqlCreateFunction;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SqlFileSchemaAccess implements SchemaAccess {
    private static final Logger log = LoggerFactory.getLogger(SqlFileSchemaAccess.class);

    private final Set<String> databases;
    private final Map<String, List<Table>> databaseTables;
    private final Map<String, List<Function>> databaseFunctions;
    private final Map<String, Long> tableLoadTime = new HashMap<>();

    public SqlFileSchemaAccess(List<SqlNode> tableNodes, List<SqlNode> udfFunctionNodes) {
        this.databases = new LinkedHashSet<>();
        this.databaseTables = buildDatabaseTables(tableNodes, this.databases);
        this.databaseFunctions = buildDatabaseFunctions(udfFunctionNodes, this.databases);
        for (String database : this.databases) {
            if (databaseTables.containsKey(database) || databaseFunctions.containsKey(database)) {
                tableLoadTime.put(database, System.currentTimeMillis());
            }
        }
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
        throw new RuntimeException("function not found " + funName + " from db " + database);
    }

    @Override
    public long getTableUpdateTime(String database, String table) {
        return tableLoadTime.getOrDefault(database, 0L);
    }

    @Override
    public List<String> getPartitionPaths(String database, String table, String partitionFilter) throws Exception {
        throw new UnsupportedOperationException("getPartitionPaths is not supported in SqlFileSchemaAccess");
    }

    private Map<String, List<Table>> buildDatabaseTables(List<SqlNode> tableNodes, Set<String> databases) {
        Map<String, List<Table>> result = new HashMap<>();
        for (SqlNode node : tableNodes) {
            if (node instanceof SqlCreateTable) {
                SqlCreateTable createTable = (SqlCreateTable) node;
                String database = extractDatabase(createTable.getTableName());
                String tableName = extractTableName(createTable.getTableName());
                databases.add(database);
                List<FieldSchema> columns = extractColumnsFromCreateTable(createTable);
                if (!columns.isEmpty()) {
                    result.computeIfAbsent(database, k -> new ArrayList<>())
                            .add(buildHmsTable(database, tableName, columns));
                }
            }
        }
        return result;
    }

    private Map<String, List<Function>> buildDatabaseFunctions(List<SqlNode> udfFunctionNodes, Set<String> databases) {
        Map<String, List<Function>> result = new HashMap<>();
        for (SqlNode node : udfFunctionNodes) {
            if (node instanceof SqlCreateFunction) {
                SqlCreateFunction createFunc = (SqlCreateFunction) node;
                String[] identifiers = createFunc.getFunctionIdentifier();
                String database = identifiers.length > 1 ? identifiers[0] : "default";
                databases.add(database);
                String functionName = identifiers[identifiers.length - 1];
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

    private List<FieldSchema> extractColumnsFromCreateTable(SqlCreateTable createTable) {
        List<FieldSchema> columns = new ArrayList<>();
        org.apache.calcite.sql.SqlNodeList columnList = createTable.getColumnList();
        if (columnList != null) {
            for (SqlNode field : columnList) {
                if (field instanceof SqlTableColumn.SqlRegularColumn) {
                    SqlTableColumn.SqlRegularColumn col = (SqlTableColumn.SqlRegularColumn) field;
                    String colName = col.getName().getSimple();
                    String typeStr = col.getType().toString();
                    columns.add(new FieldSchema(colName, typeStr, null));
                }
            }
        }
        return columns;
    }

    private Table buildHmsTable(String database, String tableName, List<FieldSchema> columns) {
        Table table = new Table();
        table.setDbName(database);
        table.setTableName(tableName);
        StorageDescriptor sd = new StorageDescriptor();
        sd.setCols(columns);
        table.setSd(sd);
        return table;
    }

    private String extractDatabase(SqlIdentifier identifier) {
        if (identifier.names.size() > 1) {
            return identifier.names.get(0);
        }
        return "default";
    }

    private String extractTableName(SqlIdentifier identifier) {
        if (identifier.names.size() > 1) {
            return identifier.names.get(identifier.names.size() - 1);
        }
        return identifier.getSimple();
    }
}
