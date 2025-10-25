package com.sqlrec.schema;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.compiler.NormalSqlCompiler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.thrift.TException;

import java.util.List;

public class HmsClient {
    private static HiveMetaStoreClient client = HmsClientSingleton(null);

    private static HiveMetaStoreClient HmsClientSingleton(String hiveMetastoreUri) {
        if (hiveMetastoreUri == null || hiveMetastoreUri.isEmpty()) {
            hiveMetastoreUri = SqlRecConfigs.HIVE_METASTORE_URI.getValue();
        }

        Configuration hiveConf = new Configuration();
        hiveConf.set(HiveConf.ConfVars.METASTOREURIS.toString(), hiveMetastoreUri);
        hiveConf.set(MetastoreConf.ConfVars.EXECUTE_SET_UGI.toString(), SqlRecConfigs.EXECUTE_SET_UGI.getValue());
        try {
            return new HiveMetaStoreClient(hiveConf);
        } catch (MetaException e) {
            throw new RuntimeException(e);
        }
    }

    // get all database names
    public synchronized static List<String> getAllDatabases() throws Exception {
        return client.getAllDatabases();
    }

    // get all table of a dataase
    public synchronized static List<String> getAllTables(String database) throws Exception {
        return client.getAllTables(database);
    }

    // get table obj
    public synchronized static org.apache.hadoop.hive.metastore.api.Table getTableObj(
            String database,
            String table
    ) throws TException {
        return client.getTable(database, table);
    }

    // get table obj
    public synchronized static org.apache.hadoop.hive.metastore.api.Table getTableObj(
            String table
    ) throws TException {
        if (table.contains(".")) {
            String[] tableNameParts = table.split("\\.");
            if (tableNameParts.length != 2) {
                throw new RuntimeException("table name " + table + " is not in format schema.table");
            }
            String schemaName = tableNameParts[0];
            String shortTableName = tableNameParts[1];
            return getTableObj(schemaName, shortTableName);
        } else {
            return getTableObj(NormalSqlCompiler.DEFAULT_SCHEMA_NAME, table);
        }
    }

    // get all function of a database
    public synchronized static List<String> getAllFunctions(String database) throws Exception {
        return client.getFunctions(database, "*");
    }

    // get function obj
    public synchronized static org.apache.hadoop.hive.metastore.api.Function getFunctionObj(
            String database,
            String function
    ) throws TException {
        return client.getFunction(database, function);
    }
}
