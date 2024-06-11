package com.sqlrec.schema;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;

import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

public class HmsSchema extends AbstractSchema {
    private Map<String, HmsTableFactory> tableFactories;
    private String hiveMetastoreUri;
    private String databaseName;
    private HiveMetaStoreClient client;

    public HmsSchema(String hiveMetastoreUri, String databaseName) throws MetaException {
        this.hiveMetastoreUri = hiveMetastoreUri;
        this.databaseName = databaseName;

        HiveConf hiveConf = new HiveConf();
        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, hiveMetastoreUri);
        client = new HiveMetaStoreClient(hiveConf);
    }

    @Override
    protected Map<String, Table> getTableMap() {
        Map<String, Table> tableMap = new java.util.HashMap<>();
        try {
            List<String> tables = client.getAllTables(databaseName);
            for (String table : tables) {
                org.apache.hadoop.hive.metastore.api.Table tableObj = client.getTable(databaseName, table);
                Table tableFromHmsTable = getTableFromHmsTable(tableObj);
                if (tableFromHmsTable != null) {
                    tableMap.put(table, tableFromHmsTable);
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
            ServiceLoader<HmsTableFactory> serviceLoader = ServiceLoader.load(HmsTableFactory.class);
            for (HmsTableFactory tableFactory : serviceLoader) {
                tableFactories.put(tableFactory.getConnectorName(), tableFactory);
            }
        }
        return tableFactories.get(connector);
    }
}
