package com.sqlrec.schema;

import java.util.List;

public class HmsClientTest {
    public static void main(String[] args) throws Exception {
        List<String> databases = HmsClient.getAllDatabases();
        for (String database : databases) {
            System.out.println(database);
        }
    }
}