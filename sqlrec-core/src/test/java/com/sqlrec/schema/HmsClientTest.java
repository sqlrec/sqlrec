package com.sqlrec.schema;

import java.util.List;

public class HmsClientTest {
    public static void main(String[] args) throws Exception {
        List<String> databases = HmsClient.getAllDatabases();
        for (String database : databases) {
            List<String> tables = HmsClient.getAllTables(database);
            for (String table : tables) {
                System.out.println(table);
                org.apache.hadoop.hive.metastore.api.Table tableObj = HmsClient.getTableObj(database, table);
                System.out.println(tableObj);
            }
        }
    }
}