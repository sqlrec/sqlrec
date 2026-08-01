package com.sqlrec.db.remote;

import com.sqlrec.common.config.SqlRecConfigs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

public class HmsClient {
    private static final Logger log = LoggerFactory.getLogger(HmsClient.class);
    private static volatile HiveMetaStoreClient client;

    static {
        // Close the cached HMS client on JVM exit so the underlying Thrift transport to
        // the Hive Metastore is gracefully closed instead of leaving a half-open
        // connection that the server must wait out by TCP timeout.
        Runtime.getRuntime().addShutdownHook(new Thread(HmsClient::invalidateClient, "HmsClient-shutdown"));
    }

    private static synchronized HiveMetaStoreClient getClient() {
        if (client == null) {
            client = createClient();
        }
        return client;
    }

    private static HiveMetaStoreClient createClient() {
        String hiveMetastoreUri = SqlRecConfigs.HIVE_METASTORE_URI.getValue();
        Configuration hiveConf = new Configuration();
        hiveConf.set(HiveConf.ConfVars.METASTOREURIS.toString(), hiveMetastoreUri);
        hiveConf.set(MetastoreConf.ConfVars.EXECUTE_SET_UGI.toString(), SqlRecConfigs.EXECUTE_SET_UGI.getValue());
        try {
            return new HiveMetaStoreClient(hiveConf);
        } catch (MetaException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Close and discard the cached HMS client so the next call re-creates it.
     * Intended to be called after a transport-level failure (HMS restart, network blip)
     * so that subsequent calls recover instead of failing forever on the broken client.
     */
    public static synchronized void invalidateClient() {
        if (client != null) {
            try {
                client.close();
            } catch (Exception e) {
                log.warn("Failed to close stale HMS client during invalidation: {}", e.getMessage());
            }
            client = null;
        }
    }

    @FunctionalInterface
    private interface HmsCall<T> {
        T apply(HiveMetaStoreClient c) throws TException;
    }

    /**
     * Run an HMS call, and on a transport-level failure invalidate the broken client
     * and retry once with a fresh client. HMS read operations are idempotent, so a
     * single in-call retry is safe and lets a transient HMS blip heal immediately.
     */
    private static <T> T withRetry(HmsCall<T> call) throws TException {
        try {
            return call.apply(getClient());
        } catch (TTransportException e) {
            log.warn("HMS transport error, invalidating client and retrying once: {}", e.getMessage());
            invalidateClient();
            return call.apply(getClient());
        }
    }

    public synchronized static List<String> getAllDatabases() throws Exception {
        return withRetry(HiveMetaStoreClient::getAllDatabases);
    }

    public synchronized static List<String> getAllTables(String database) throws Exception {
        return withRetry(c -> c.getAllTables(database));
    }

    public synchronized static org.apache.hadoop.hive.metastore.api.Table getTableObj(
            String database,
            String table
    ) throws TException {
        return withRetry(c -> c.getTable(database, table));
    }

    public synchronized static List<String> getAllFunctions(String database) throws Exception {
        return withRetry(c -> c.getFunctions(database, "*"));
    }

    public synchronized static org.apache.hadoop.hive.metastore.api.Function getFunctionObj(
            String database,
            String function
    ) throws TException {
        return withRetry(c -> c.getFunction(database, function));
    }

    public synchronized static List<String> getPartitionPaths(
            String database,
            String table,
            String partitionFilter
    ) throws Exception {
        List<org.apache.hadoop.hive.metastore.api.Partition> partitions = withRetry(
                c -> c.listPartitionsByFilter(database, table, partitionFilter, (short) -1)
        );
        return partitions.stream()
                .map(partition -> partition.getSd().getLocation())
                .collect(Collectors.toList());
    }
}
