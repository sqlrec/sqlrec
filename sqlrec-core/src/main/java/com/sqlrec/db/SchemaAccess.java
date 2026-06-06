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

    List<String> getPartitionPaths(String database, String table, String partitionFilter) throws Exception;
}
