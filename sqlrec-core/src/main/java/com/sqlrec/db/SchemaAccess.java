package com.sqlrec.db;

import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.List;

public interface SchemaAccess {

    List<String> getDatabases() throws Exception;

    List<Table> getTableMetas(String database) throws Exception;

    List<Function> getFunctionMetas(String database) throws Exception;

    long getTableUpdateTime(String database, String table);
}
