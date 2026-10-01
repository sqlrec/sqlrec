package com.sqlrec.db;

import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.List;

public interface SchemaAccess {

    List<String> getDatabases() throws Exception;

    List<Table> getTables(String database) throws Exception;

    Table getTable(String database, String tableName) throws Exception;

    List<Function> getFunctions(String database) throws Exception;

    Function getFunction(String database, String funName) throws Exception;

    long getTableUpdateTime(String database, String table);

    /** Execute one persistent metadata DDL through the Hive client, never through a SQL gateway. */
    default void executeMetadataDdl(String sql, String database) throws Exception {
        throw new UnsupportedOperationException(
                "Metadata DDL is not supported in local SQL file metadata mode; edit SQL_SCHEMA_DIR and restart");
    }

    /** Full Catalog descriptions, including definitions the local query engine cannot execute. */
    default com.sqlrec.executor.SqlProcessResult executeMetadataQuery(String sql, String database) throws Exception {
        return null;
    }

    List<String> getPartitionPaths(String database, String table, String partitionFilter) throws Exception;
}
