package com.sqlrec.schema;

import com.sqlrec.common.config.SqlRecConfigs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.thrift.TException;

import java.util.List;
import java.util.stream.Collectors;

public class HmsClient {
    private static volatile HiveMetaStoreClient client;

    private static HiveMetaStoreClient getClient() {
        if (client == null) {
            synchronized (HmsClient.class) {
                if (client == null) {
                    String hiveMetastoreUri = SqlRecConfigs.HIVE_METASTORE_URI.getValue();
                    Configuration hiveConf = new Configuration();
                    hiveConf.set(HiveConf.ConfVars.METASTOREURIS.toString(), hiveMetastoreUri);
                    hiveConf.set(MetastoreConf.ConfVars.EXECUTE_SET_UGI.toString(), SqlRecConfigs.EXECUTE_SET_UGI.getValue());
                    try {
                        client = new HiveMetaStoreClient(hiveConf);
                    } catch (MetaException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        return client;
    }

    // get all database names
    public synchronized static List<String> getAllDatabases() throws Exception {
        return getClient().getAllDatabases();
    }

    // get all table of a dataase
    public synchronized static List<String> getAllTables(String database) throws Exception {
        return getClient().getAllTables(database);
    }

    // get table obj
    public synchronized static org.apache.hadoop.hive.metastore.api.Table getTableObj(
            String database,
            String table
    ) throws TException {
        return getClient().getTable(database, table);
    }

    // get all function of a database
    public synchronized static List<String> getAllFunctions(String database) throws Exception {
        return getClient().getFunctions(database, "*");
    }

    // get function obj
    public synchronized static org.apache.hadoop.hive.metastore.api.Function getFunctionObj(
            String database,
            String function
    ) throws TException {
        return getClient().getFunction(database, function);
    }

    // get all partition paths by database, table and partition filter
    public synchronized static List<String> getPartitionPaths(
            String database,
            String table,
            String partitionFilter
    ) throws Exception {
        List<org.apache.hadoop.hive.metastore.api.Partition> partitions = getClient().listPartitionsByFilter(
                database, table, partitionFilter, (short) -1
        );
        return partitions.stream()
                .map(partition -> partition.getSd().getLocation())
                .collect(Collectors.toList());
    }
}
