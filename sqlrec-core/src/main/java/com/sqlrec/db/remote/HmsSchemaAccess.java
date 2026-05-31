package com.sqlrec.db.remote;

import com.sqlrec.common.utils.HiveTableUtils;
import com.sqlrec.db.SchemaAccess;
import com.sqlrec.udf.config.FunctionConfigs;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HmsSchemaAccess implements SchemaAccess {
    private static final Logger log = LoggerFactory.getLogger(HmsSchemaAccess.class);

    private final Map<String, Map<String, Long>> tableUpdateTimesMap = new ConcurrentHashMap<>();

    @Override
    public List<String> getDatabases() throws Exception {
        return HmsClient.getAllDatabases();
    }

    @Override
    public List<Table> getTables(String database) throws Exception {
        List<Table> tables = new ArrayList<>();
        Map<String, Long> tableUpdateTimes = tableUpdateTimesMap.computeIfAbsent(database, k -> new ConcurrentHashMap<>());
        try {
            List<String> tableNames = HmsClient.getAllTables(database);
            for (String tableName : tableNames) {
                Table tableObj = HmsClient.getTableObj(database, tableName);
                if (tableObj == null) continue;
                long modTime = HiveTableUtils.getTableModificationTime(tableObj);
                tableUpdateTimes.put(tableName, modTime);
                tables.add(tableObj);
            }
        } catch (Exception e) {
            log.error("Error while getting table metas for schema {}", database, e);
            throw new RuntimeException(e);
        }
        return tables;
    }

    @Override
    public List<Function> getFunctions(String database) throws Exception {
        List<Function> functions = new ArrayList<>();
        try {
            List<String> functionNames = HmsClient.getAllFunctions(database);
            for (String functionName : functionNames) {
                Function functionObj = HmsClient.getFunctionObj(database, functionName);
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
    public long getTableUpdateTime(String database, String table) {
        Map<String, Long> tableUpdateTimes = tableUpdateTimesMap.get(database);
        if (tableUpdateTimes == null) {
            return 0L;
        }
        return tableUpdateTimes.getOrDefault(table, 0L);
    }

    @Override
    public List<String> getPartitionPaths(String database, String table, String partitionFilter) throws Exception {
        return HmsClient.getPartitionPaths(database, table, partitionFilter);
    }
}
