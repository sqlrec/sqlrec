package com.sqlrec.db.remote;

import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class HmsSchemaAccessTest {
    @Test
    void concurrentDropsDoNotBreakTheOtherTablesAndFunctions() throws Exception {
        try (var hms = mockStatic(HmsClient.class)) {
            Table table = new Table();
            table.setTableName("remaining");
            Function function = new Function();
            function.setFunctionName("remaining");
            hms.when(() -> HmsClient.getAllTables("db")).thenReturn(List.of("dropped", "remaining"));
            hms.when(() -> HmsClient.getTableObj("db", "dropped")).thenThrow(new NoSuchObjectException());
            hms.when(() -> HmsClient.getTableObj("db", "remaining")).thenReturn(table);
            hms.when(() -> HmsClient.getAllFunctions("db")).thenReturn(List.of("dropped", "remaining"));
            hms.when(() -> HmsClient.getFunctionObj("db", "dropped")).thenThrow(new NoSuchObjectException());
            hms.when(() -> HmsClient.getFunctionObj("db", "remaining")).thenReturn(function);
            HmsSchemaAccess access = new HmsSchemaAccess();
            assertEquals(List.of(table), access.getTables("db"));
            assertEquals(List.of(function), access.getFunctions("db"));
        }
    }

    @Test
    void newTableSnapshotRemovesDeletedModificationTimes() throws Exception {
        try (var hms = mockStatic(HmsClient.class)) {
            Table table = new Table();
            table.setTableName("old");
            table.putToParameters("transient_lastDdlTime", "100");
            hms.when(() -> HmsClient.getAllTables("db")).thenReturn(List.of("old"), List.of());
            hms.when(() -> HmsClient.getTableObj("db", "old")).thenReturn(table);
            HmsSchemaAccess access = new HmsSchemaAccess();
            access.getTables("db");
            assertTrue(access.getTableUpdateTime("db", "old") > 0);
            access.getTables("db");
            assertEquals(0, access.getTableUpdateTime("db", "old"));
        }
    }
}
