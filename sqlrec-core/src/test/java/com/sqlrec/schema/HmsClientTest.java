package com.sqlrec.schema;

import com.sqlrec.common.utils.HiveTableUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

@Tag("integration")
public class HmsClientTest {
    @Test
    public void testHmsClient() throws Exception {
        List<String> databases = HmsClient.getAllDatabases();
        for (String database : databases) {
            List<String> tables = HmsClient.getAllTables(database);
            for (String table : tables) {
                System.out.println(table);
                org.apache.hadoop.hive.metastore.api.Table tableObj = HmsClient.getTableObj(database, table);
                System.out.println(tableObj);
                long modificationTime = HiveTableUtils.getTableModificationTime(tableObj);
                System.out.println("modificationTime: " + modificationTime);
                break;
            }
            List<String> functions = HmsClient.getAllFunctions(database);
            for (String function : functions) {
                System.out.println(function);
                org.apache.hadoop.hive.metastore.api.Function functionObj = HmsClient.getFunctionObj(database, function);
                System.out.println(functionObj);
                break;
            }
            break;
        }
    }
}