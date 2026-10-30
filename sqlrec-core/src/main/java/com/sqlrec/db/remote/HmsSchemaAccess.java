package com.sqlrec.db.remote;

import com.sqlrec.db.SchemaAccess;
import com.sqlrec.executor.SqlProcessResult;
import org.apache.calcite.sql.SqlNode;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class HmsSchemaAccess implements SchemaAccess, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(HmsSchemaAccess.class);

    private final FlinkHiveDdlAdapter ddlAdapter = new FlinkHiveDdlAdapter();

    @Override
    public void close() {
        ddlAdapter.close();
    }

    @Override
    public void executeMetadataDdl(SqlNode node, String database) throws Exception {
        ddlAdapter.executeDdl(node, database);
    }

    @Override
    public SqlProcessResult executeMetadataQuery(SqlNode node, String database) throws Exception {
        return ddlAdapter.executeQuery(node, database);
    }

    @Override
    public List<String> getDatabases() throws Exception {
        return HmsClient.getAllDatabases();
    }

    @Override
    public List<Table> getTables(String database) throws Exception {
        List<Table> tables = new ArrayList<>();
        try {
            List<String> tableNames = HmsClient.getAllTables(database);
            for (String tableName : tableNames) {
                Table tableObj;
                try {
                    tableObj = HmsClient.getTableObj(database, tableName);
                } catch (NoSuchObjectException e) {
                    continue; // The table was dropped after listing it.
                }
                if (tableObj == null) continue;
                tables.add(tableObj);
            }
        } catch (Exception e) {
            log.error("Error while getting table metas for schema {}", database, e);
            throw new RuntimeException(e);
        }
        return tables;
    }

    @Override
    public Table getTable(String database, String tableName) throws Exception {
        return HmsClient.getTableObj(database, tableName);
    }

    @Override
    public List<Function> getFunctions(String database) throws Exception {
        List<Function> functions = new ArrayList<>();
        try {
            List<String> functionNames = HmsClient.getAllFunctions(database);
            for (String functionName : functionNames) {
                Function functionObj;
                try {
                    functionObj = HmsClient.getFunctionObj(database, functionName);
                } catch (NoSuchObjectException e) {
                    continue;
                }
                if (functionObj != null) {
                    functions.add(functionObj);
                }
            }
        } catch (Exception e) {
            log.error("Error while getting function metas for schema {}", database, e);
            throw new RuntimeException("Failed to get function metas for schema: " + database, e);
        }
        return functions;
    }

    @Override
    public Function getFunction(String database, String funName) throws Exception {
        return HmsClient.getFunctionObj(database, funName);
    }

    @Override
    public List<String> getPartitionPaths(String database, String table, String partitionFilter) throws Exception {
        return HmsClient.getPartitionPaths(database, table, partitionFilter);
    }
}
