package com.sqlrec.schema;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.utils.HiveTableUtils;
import com.sqlrec.udf.config.FunctionConfigs;
import com.sqlrec.utils.ObjCache;
import com.sqlrec.utils.SchemaUtils;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HmsSchema extends AbstractSchema {
    private static final Logger log = LoggerFactory.getLogger(HmsSchema.class);
    private static CalciteSchema globalSchema;  // for test

    private static Map<String, HmsSchema> schemaMap = new ConcurrentHashMap<>();
    private static ObjCache<List<String>> databaseListCache = new ObjCache<>(
            SqlRecConfigs.SCHEMA_CACHE_EXPIRE.getValue() * 1000L,
            SqlRecConfigs.ASYNC_SCHEMA_UPDATE.getValue(),
            (oldDatabaseList) -> {
                try {
                    return HmsClient.getAllDatabases();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
    );

    private String databaseName;
    private ObjCache<Map<String, Table>> tableMapCache;
    private ObjCache<Multimap<String, Function>> functionMapCache;
    private Map<String, Long> tableUpdateTimes = new ConcurrentHashMap<>();

    public HmsSchema(String databaseName) {
        this.databaseName = databaseName;
        tableMapCache = new ObjCache<>(
                SqlRecConfigs.SCHEMA_CACHE_EXPIRE.getValue() * 1000L,
                SqlRecConfigs.ASYNC_SCHEMA_UPDATE.getValue(),
                this::computeTableMap
        );
        functionMapCache = new ObjCache<>(
                SqlRecConfigs.SCHEMA_CACHE_EXPIRE.getValue() * 1000L,
                SqlRecConfigs.ASYNC_SCHEMA_UPDATE.getValue(),
                this::computeFunctionMap
        );
    }

    public static void setGlobalSchema(CalciteSchema schema) {
        globalSchema = schema;
    }

    public static CalciteSchema getHmsCalciteSchema() {
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);

        if (globalSchema != null) {
            globalSchema.getSubSchemaMap().forEach((k, v) -> {
                rootSchema.add(k, v.schema);
            });
            return rootSchema;
        }

        try {
            for (String database : databaseListCache.getObj()) {
                if (!schemaMap.containsKey(database)) {
                    schemaMap.put(database, new HmsSchema(database));
                }
                rootSchema.add(database, schemaMap.get(database));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return rootSchema;
    }

    @Override
    protected Map<String, Table> getTableMap() {
        return tableMapCache.getObj();
    }

    public Map<String, Table> computeTableMap(Map<String, Table> oldTableMap) {
        Map<String, Table> tableMap = new ConcurrentHashMap<>();
        try {
            List<String> tables = HmsClient.getAllTables(databaseName);
            for (String table : tables) {
                org.apache.hadoop.hive.metastore.api.Table tableObj = HmsClient.getTableObj(databaseName, table);
                if (oldTableMap != null && oldTableMap.containsKey(table) && tableUpdateTimes.containsKey(table)) {
                    if (tableUpdateTimes.get(table) >= HiveTableUtils.getTableModificationTime(tableObj)) {
                        tableMap.put(table, oldTableMap.get(table));
                        continue;
                    }
                }
                Table tableFromHmsTable = TableFactoryUtils.getTableFromHmsTable(tableObj);
                if (tableFromHmsTable != null) {
                    tableMap.put(table, tableFromHmsTable);
                    tableUpdateTimes.put(table, HiveTableUtils.getTableModificationTime(tableObj));
                }
            }
        } catch (Exception e) {
            log.error("Error while computing table map for schema {}", databaseName, e);
            throw new RuntimeException(e);
        }
        return tableMap;
    }

    @Override
    protected Multimap<String, Function> getFunctionMultimap() {
        return functionMapCache.getObj();
    }

    private Multimap<String, Function> computeFunctionMap(Multimap<String, Function> oldFunctionMap) {
        Multimap<String, Function> functionMap = ArrayListMultimap.create();
        try {
            List<String> functions = HmsClient.getAllFunctions(databaseName);
            for (String function : functions) {
                org.apache.hadoop.hive.metastore.api.Function functionObj = HmsClient.getFunctionObj(databaseName, function);
                ScalarFunction scalarFunction = SchemaUtils.createScalarFunction(functionObj.getClassName());
                if (scalarFunction != null) {
                    functionMap.put(function, scalarFunction);
                }
            }
            for (Map.Entry<String, String> entry : FunctionConfigs.DEFAULT_SCALAR_FUNCTION_CONFIGS.entrySet()) {
                functionMap.put(entry.getKey(), SchemaUtils.createScalarFunction(entry.getValue()));
            }
        } catch (Exception e) {
            log.error("Error while computing function map for schema {}", databaseName, e);
            throw new RuntimeException("Failed to compute function map for schema: " + databaseName, e);
        }
        return functionMap;
    }

    public static long getTableUpdateTime(String db, String table) {
        HmsSchema schema = schemaMap.get(db);
        if (schema == null) {
            return 0L;
        }
        return schema.tableUpdateTimes.getOrDefault(table, 0L);
    }
}