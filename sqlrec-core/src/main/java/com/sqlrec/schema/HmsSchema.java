package com.sqlrec.schema;

import com.sqlrec.utils.HiveTableUtils;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

public class HmsSchema extends AbstractSchema {
    private static Map<String, HmsTableFactory> tableFactories;
    private static Map<String, HmsSchema> schemaMap = new ConcurrentHashMap<>();

    private String databaseName;
    private Map<String, Table> tableMap = new ConcurrentHashMap<>();

    private static CalciteSchema globalSchema;  // for test

    public HmsSchema(String databaseName) {
        this.databaseName = databaseName;
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
            for (String database : HmsClient.getAllDatabases()) {
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
        try {
            List<String> tables = HmsClient.getAllTables(databaseName);
            for (String table : tables) {
                if (tableMap.containsKey(table)) {
                    continue;
                }
                org.apache.hadoop.hive.metastore.api.Table tableObj = HmsClient.getTableObj(databaseName, table);
                Table tableFromHmsTable = getTableFromHmsTable(tableObj);
                if (tableFromHmsTable != null) {
                    tableMap.put(table, tableFromHmsTable);
                }
            }
            tableMap.keySet().removeIf(table -> !tables.contains(table));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return tableMap;
    }

    private Table getTableFromHmsTable(org.apache.hadoop.hive.metastore.api.Table tableObj) {
        String connector = HiveTableUtils.getTableConnector(tableObj);
        if (connector == null) {
            return null;
        }
        HmsTableFactory tableFactory = getTableFactory(connector);
        if (tableFactory != null) {
            return tableFactory.getTableFromHmsTable(tableObj);
        }
        return null;
    }

    public static HmsTableFactory getTableFactory(String connector) {
        return getTableFactorieMap().getOrDefault(connector, null);
    }

    public static synchronized Map<String, HmsTableFactory> getTableFactorieMap() {
        if (tableFactories == null) {
            tableFactories = new ConcurrentHashMap<>();
            ServiceLoader<HmsTableFactory> serviceLoader = ServiceLoader.load(HmsTableFactory.class);
            for (HmsTableFactory tableFactory : serviceLoader) {
                tableFactories.put(tableFactory.getConnectorName(), tableFactory);
            }
        }
        return tableFactories;
    }
}
