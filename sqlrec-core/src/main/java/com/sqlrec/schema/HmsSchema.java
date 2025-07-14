package com.sqlrec.schema;

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

    public HmsSchema(String databaseName) {
        this.databaseName = databaseName;
    }

    public static CalciteSchema getHmsCalciteSchema() throws Exception {
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);
        for (String database : HmsClient.getAllDatabases()) {
            if (!schemaMap.containsKey(database)) {
                schemaMap.put(database, new HmsSchema(database));
            }
            rootSchema.add(database, schemaMap.get(database));
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
            for (String table : tableMap.keySet()) {
                if (!tables.contains(table)) {
                    tableMap.remove(table);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return tableMap;
    }

    private Table getTableFromHmsTable(org.apache.hadoop.hive.metastore.api.Table tableObj) {
        Map<String, String> tableProperties = tableObj.getParameters();
        // Get specific option such as connector
        String connector = tableProperties.get("connector");
        if (connector == null) {
            return null;
        }
        HmsTableFactory tableFactory = getTableFactory(connector);
        if (tableFactory != null) {
            return tableFactory.getTableFromHmsTable(tableObj);
        }
        return null;
    }

    private synchronized HmsTableFactory getTableFactory(String connector) {
        if (tableFactories == null) {
            tableFactories = new ConcurrentHashMap<>();
            ServiceLoader<HmsTableFactory> serviceLoader = ServiceLoader.load(HmsTableFactory.class);
            for (HmsTableFactory tableFactory : serviceLoader) {
                tableFactories.put(tableFactory.getConnectorName(), tableFactory);
            }
        }
        return tableFactories.get(connector);
    }
}
