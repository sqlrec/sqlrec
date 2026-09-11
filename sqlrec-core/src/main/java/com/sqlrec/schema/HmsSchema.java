package com.sqlrec.schema;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.utils.HiveTableUtils;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HmsSchema extends AbstractSchema {
    private static final Logger log = LoggerFactory.getLogger(HmsSchema.class);

    private final String databaseName;
    private final MetadataAccess metadataAccess;
    private final CacheUtils.SingleValueCache<Map<String, Table>> tableMapCache;
    private final CacheUtils.SingleValueCache<Multimap<String, Function>> functionMapCache;
    private volatile Map<String, Long> tableModificationTimes = Collections.emptyMap();

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
                this::computeTableMap
        );
        this.functionMapCache = CacheUtils.createSingleValueRefreshCache(
                refreshInterval,
                this::computeFunctionMap
        );
    }

    public void invalidateCache() {
        tableMapCache.invalidate();
        functionMapCache.invalidate();
    }

    @Override
    protected Map<String, Table> getTableMap() {
        return tableMapCache.get();
    }

    @Override
    protected Multimap<String, Function> getFunctionMultimap() {
        return functionMapCache.get();
    }

    private Map<String, Table> computeTableMap(Map<String, Table> oldTableMap) {
        try {
            List<org.apache.hadoop.hive.metastore.api.Table> tableMetas = metadataAccess.getTables(databaseName);
            Map<String, Table> tableMap = new ConcurrentHashMap<>();
            Map<String, Long> oldModificationTimes = tableModificationTimes;
            Map<String, Long> newModificationTimes = new HashMap<>();
            for (org.apache.hadoop.hive.metastore.api.Table tableMeta : tableMetas) {
                String tableName = tableMeta.getTableName();
                long newModificationTime = HiveTableUtils.getTableModificationTime(tableMeta);
                newModificationTimes.put(tableName, newModificationTime);

                Long oldModificationTime = oldModificationTimes.get(tableName);
                if (oldTableMap != null
                        && oldTableMap.containsKey(tableName)
                        && oldModificationTime != null
                        && oldModificationTime >= newModificationTime) {
                    tableMap.put(tableName, oldTableMap.get(tableName));
                    continue;
                }
                Table table = TableFactoryUtils.getTableFromHmsTable(tableMeta);
                if (table != null) {
                    tableMap.put(tableName, table);
                }
            }
            tableModificationTimes = Collections.unmodifiableMap(newModificationTimes);
            return tableMap;
        } catch (Exception e) {
            log.error("Error while computing table map for schema {}", databaseName, e);
            throw new RuntimeException(e);
        }
    }

    private Multimap<String, Function> computeFunctionMap(Multimap<String, Function> oldFunctionMap) {
        try {
            List<org.apache.hadoop.hive.metastore.api.Function> functionMetas = metadataAccess.getFunctions(databaseName);
            Multimap<String, Function> functionMap = ArrayListMultimap.create();
            for (org.apache.hadoop.hive.metastore.api.Function functionMeta : functionMetas) {
                try {
                    ScalarFunction scalarFunction = UdfManager.createScalarFunction(functionMeta.getClassName());
                    if (scalarFunction != null) {
                        functionMap.put(functionMeta.getFunctionName(), scalarFunction);
                    }
                } catch (Exception e) {
                    log.error("Failed to create scalar function {} from class {}",
                            functionMeta.getFunctionName(), functionMeta.getClassName(), e);
                }
            }
            for (Map.Entry<String, String> entry : FunctionConfigs.DEFAULT_SCALAR_FUNCTION_CONFIGS.entrySet()) {
                try {
                    ScalarFunction scalarFunction = UdfManager.createScalarFunction(entry.getValue());
                    if (scalarFunction != null) {
                        functionMap.put(entry.getKey(), scalarFunction);
                    }
                } catch (Exception e) {
                    log.error("Failed to create scalar function {} from class {}",
                            entry.getKey(), entry.getValue(), e);
                }
            }
            return functionMap;
        } catch (Exception e) {
            log.error("Error while computing function map for schema {}", databaseName, e);
            throw new RuntimeException(e);
        }
    }
}