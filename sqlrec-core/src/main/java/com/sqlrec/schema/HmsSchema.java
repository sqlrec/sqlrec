package com.sqlrec.schema;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.utils.HiveTableUtils;
import com.sqlrec.db.MetadataAccess;
import com.sqlrec.udf.UdfManager;
import com.sqlrec.utils.ObjCache;
import com.sqlrec.utils.TableFactoryUtils;
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

    private final String databaseName;
    private final MetadataAccess metadataAccess;
    private final ObjCache<Map<String, Table>> tableMapCache;
    private final ObjCache<Multimap<String, Function>> functionMapCache;

    public HmsSchema(String databaseName, MetadataAccess metadataAccess) {
        this.databaseName = databaseName;
        this.metadataAccess = metadataAccess;
        this.tableMapCache = new ObjCache<>(
                SqlRecConfigs.SCHEMA_CACHE_EXPIRE.getValue() * 1000L,
                SqlRecConfigs.ASYNC_SCHEMA_UPDATE.getValue(),
                this::computeTableMap
        );
        this.functionMapCache = new ObjCache<>(
                SqlRecConfigs.SCHEMA_CACHE_EXPIRE.getValue() * 1000L,
                SqlRecConfigs.ASYNC_SCHEMA_UPDATE.getValue(),
                this::computeFunctionMap
        );
    }

    @Override
    protected Map<String, Table> getTableMap() {
        return tableMapCache.getObj();
    }

    @Override
    protected Multimap<String, Function> getFunctionMultimap() {
        return functionMapCache.getObj();
    }

    private Map<String, Table> computeTableMap(Map<String, Table> oldTableMap) {
        try {
            List<org.apache.hadoop.hive.metastore.api.Table> tableMetas = metadataAccess.getTableMetas(databaseName);
            Map<String, Table> tableMap = new ConcurrentHashMap<>();
            for (org.apache.hadoop.hive.metastore.api.Table tableMeta : tableMetas) {
                String tableName = tableMeta.getTableName();
                if (oldTableMap != null && oldTableMap.containsKey(tableName)) {
                    long oldUpdateTime = metadataAccess.getTableUpdateTime(databaseName, tableName);
                    if (oldUpdateTime >= HiveTableUtils.getTableModificationTime(tableMeta)) {
                        tableMap.put(tableName, oldTableMap.get(tableName));
                        continue;
                    }
                }
                Table table = TableFactoryUtils.getTableFromHmsTable(tableMeta);
                if (table != null) {
                    tableMap.put(tableName, table);
                }
            }
            return tableMap;
        } catch (Exception e) {
            log.error("Error while computing table map for schema {}", databaseName, e);
            throw new RuntimeException(e);
        }
    }

    private Multimap<String, Function> computeFunctionMap(Multimap<String, Function> oldFunctionMap) {
        try {
            List<org.apache.hadoop.hive.metastore.api.Function> functionMetas = metadataAccess.getFunctionMetas(databaseName);
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
            return functionMap;
        } catch (Exception e) {
            log.error("Error while computing function map for schema {}", databaseName, e);
            throw new RuntimeException(e);
        }
    }
}