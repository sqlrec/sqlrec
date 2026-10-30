package com.sqlrec.schema;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.sqlrec.common.config.Consts;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.utils.MetricsUtils;
import com.sqlrec.common.utils.ResourceNames;
import com.sqlrec.db.MetadataAccess;
import com.sqlrec.udf.UdfManager;
import com.sqlrec.udf.config.FunctionConfigs;
import com.sqlrec.utils.CacheUtils;
import com.sqlrec.utils.TableFactoryUtils;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HmsSchema extends AbstractSchema {
    private static final Logger log = LoggerFactory.getLogger(HmsSchema.class);

    private final String databaseName;
    private final MetadataAccess metadataAccess;
    private final CacheUtils.SingleValueCache<Map<String, Table>> tableMapCache;
    private final CacheUtils.SingleValueCache<Multimap<String, Function>> functionMapCache;
    private Map<String, TableEntry> tableEntries = new HashMap<>();

    private record TableEntry(org.apache.hadoop.hive.metastore.api.Table metadata, Table table) {}

    public HmsSchema(String databaseName, MetadataAccess metadataAccess) {
        this(
                databaseName,
                metadataAccess,
                Duration.ofSeconds(SqlRecConfigs.SCHEMA_CACHE_EXPIRE.getValue())
        );
    }

    HmsSchema(
            String databaseName,
            MetadataAccess metadataAccess,
            Duration refreshInterval
    ) {
        this.databaseName = databaseName;
        this.metadataAccess = metadataAccess;
        this.tableMapCache = CacheUtils.createSingleValueRefreshCache(
                refreshInterval,
                ignored -> computeTableMap()
        );
        this.functionMapCache = CacheUtils.createSingleValueRefreshCache(
                refreshInterval,
                this::computeFunctionMap
        );
    }

    public void invalidateCache() {
        synchronized (this) {
            tableEntries.keySet().forEach(this::unregisterTableMetrics);
            tableEntries.clear();
        }
        invalidateMetadata();
    }

    /** Reload definitions while retaining execution objects whose metadata has not changed. */
    public void invalidateMetadata() {
        tableMapCache.invalidate();
        functionMapCache.invalidate();
    }

    public void evictTable(String table) {
        synchronized (this) {
            tableEntries.keySet().removeIf(name -> {
                if (!name.equalsIgnoreCase(table)) return false;
                unregisterTableMetrics(name);
                return true;
            });
        }
        // Caffeine can call the loader while holding its own lock; never invalidate under this monitor.
        tableMapCache.invalidate();
    }

    @Override
    protected Map<String, Table> getTableMap() {
        return tableMapCache.get();
    }

    @Override
    protected Multimap<String, Function> getFunctionMultimap() {
        return functionMapCache.get();
    }

    private synchronized Map<String, Table> computeTableMap() {
        try {
            List<org.apache.hadoop.hive.metastore.api.Table> tableMetas = metadataAccess.getTables(databaseName);
            Map<String, Table> tableMap = new HashMap<>();
            Map<String, TableEntry> updated = new HashMap<>();
            for (org.apache.hadoop.hive.metastore.api.Table tableMeta : tableMetas) {
                String tableName = tableMeta.getTableName();
                TableEntry entry = tableEntries.get(tableName);
                if (entry == null || !entry.metadata().equals(tableMeta)) {
                    Table table = TableFactoryUtils.getTableFromHmsTable(tableMeta);
                    if (table == null) continue;
                    entry = new TableEntry(tableMeta.deepCopy(), table);
                }
                updated.put(tableName, entry);
                tableMap.put(tableName, entry.table());
            }
            for (String oldTableName : tableEntries.keySet()) {
                if (!updated.containsKey(oldTableName)) unregisterTableMetrics(oldTableName);
            }
            Map<String, Table> result = Map.copyOf(tableMap);
            tableEntries = updated;
            return result;
        } catch (Exception e) {
            log.error("Error while computing table map for schema {}", databaseName, e);
            throw new RuntimeException(e);
        }
    }

    private void unregisterTableMetrics(String table) {
        String fullName = Consts.DEFAULT_SCHEMA_NAME.equals(databaseName) ? table : databaseName + "." + table;
        MetricsUtils.unregisterTableCacheMetrics(fullName);
    }

    private Multimap<String, Function> computeFunctionMap(Multimap<String, Function> oldFunctionMap) {
        try {
            List<org.apache.hadoop.hive.metastore.api.Function> functionMetas = metadataAccess.getFunctions(databaseName);
            Multimap<String, Function> functionMap = ArrayListMultimap.create();
            for (org.apache.hadoop.hive.metastore.api.Function functionMeta : functionMetas) {
                registerScalarFunction(functionMap, functionMeta.getFunctionName(), functionMeta.getClassName());
            }
            // Successfully loaded HMS definitions take precedence. Registering both copies
            // makes Calcite resolve them as overloads, even when the implementations match.
            for (Map.Entry<String, String> entry : FunctionConfigs.DEFAULT_SCALAR_FUNCTION_CONFIGS.entrySet()) {
                registerScalarFunction(functionMap, entry.getKey(), entry.getValue());
            }
            return functionMap;
        } catch (Exception e) {
            log.error("Error while computing function map for schema {}", databaseName, e);
            throw new RuntimeException(e);
        }
    }

    private static void registerScalarFunction(
            Multimap<String, Function> functionMap, String name, String className) {
        String functionName = ResourceNames.normalize(name);
        if (functionMap.containsKey(functionName)) {
            return;
        }
        try {
            ScalarFunction scalarFunction = UdfManager.createScalarFunction(className);
            if (scalarFunction != null) {
                functionMap.put(functionName, scalarFunction);
            }
        } catch (Exception | LinkageError e) {
            log.error("Failed to create scalar function {} from class {}", name, className, e);
        }
    }
}
