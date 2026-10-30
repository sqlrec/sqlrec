package com.sqlrec.db;

import com.sqlrec.executor.SqlProcessResult;
import org.apache.calcite.sql.SqlNode;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.List;

public interface SchemaAccess {

    List<String> getDatabases() throws Exception;

    List<Table> getTables(String database) throws Exception;

    Table getTable(String database, String tableName) throws Exception;

    List<Function> getFunctions(String database) throws Exception;

    Function getFunction(String database, String funName) throws Exception;

    /** Execute one metadata DDL in the configured storage, never through a SQL gateway. */
    default void executeMetadataDdl(SqlNode node, String database) throws Exception {
        throw new UnsupportedOperationException(
                "Metadata DDL is not supported by this storage: " + node.getClass().getSimpleName());
    }

    /** Describes stored definitions independently of whether the local query engine can execute them. */
    default SqlProcessResult executeMetadataQuery(SqlNode node, String database) throws Exception {
        throw new UnsupportedOperationException("Unsupported metadata query: " + node.getClass().getSimpleName());
    }

    List<String> getPartitionPaths(String database, String table, String partitionFilter) throws Exception;
}
