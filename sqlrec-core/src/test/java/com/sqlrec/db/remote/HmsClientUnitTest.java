package com.sqlrec.db.remote;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Mock unit tests for HmsClient.
 * Injects a mock HiveMetaStoreClient via setClientForTest to verify read methods
 * without a real Hive Metastore service.
 */
@ExtendWith(MockitoExtension.class)
public class HmsClientUnitTest {

    @Mock
    private HiveMetaStoreClient mockClient;

    @BeforeEach
    public void setUp() {
        // Inject mock client to avoid triggering real HMS connection creation
        HmsClient.setClientForTest(mockClient);
    }

    @AfterEach
    public void tearDown() {
        // Clean up cached client to avoid polluting other tests
        HmsClient.invalidateClient();
    }

    @Test
    public void testGetAllDatabases() throws Exception {
        // Mock returning a list of database names
        when(mockClient.getAllDatabases()).thenReturn(Arrays.asList("db1", "db2"));

        List<String> databases = HmsClient.getAllDatabases();

        assertEquals(Arrays.asList("db1", "db2"), databases);
    }

    @Test
    public void testGetAllTables() throws Exception {
        // Mock returning table names for the given database
        when(mockClient.getAllTables("mydb")).thenReturn(Arrays.asList("tbl1", "tbl2"));

        List<String> tables = HmsClient.getAllTables("mydb");

        assertEquals(Arrays.asList("tbl1", "tbl2"), tables);
    }

    @Test
    public void testGetTableObj() throws Exception {
        // Table has no 2-arg constructor; use no-arg constructor + setters
        Table table = new Table();
        table.setDbName("db");
        table.setTableName("tbl");
        when(mockClient.getTable("db", "tbl")).thenReturn(table);

        Table result = HmsClient.getTableObj("db", "tbl");

        assertSame(table, result);
        assertEquals("db", result.getDbName());
        assertEquals("tbl", result.getTableName());
    }

    @Test
    public void testGetAllFunctions() throws Exception {
        // Mock returning function names
        when(mockClient.getFunctions("db", "*")).thenReturn(Arrays.asList("fn1", "fn2"));

        List<String> functions = HmsClient.getAllFunctions("db");

        assertEquals(Arrays.asList("fn1", "fn2"), functions);
    }

    @Test
    public void testGetPartitionPaths() throws Exception {
        // Build two partitions with StorageDescriptor locations set
        Partition p1 = new Partition();
        p1.setSd(new StorageDescriptor());
        p1.getSd().setLocation("hdfs://namenode/warehouse/db/tbl/p1");

        Partition p2 = new Partition();
        p2.setSd(new StorageDescriptor());
        p2.getSd().setLocation("hdfs://namenode/warehouse/db/tbl/p2");

        // Mock listPartitionsByFilter to return the partition list; maxParts=-1 means all
        when(mockClient.listPartitionsByFilter(eq("db"), eq("tbl"), anyString(), eq((short) -1)))
                .thenReturn(Arrays.asList(p1, p2));

        List<String> paths = HmsClient.getPartitionPaths("db", "tbl", "dt='20240101'");

        assertEquals(Arrays.asList(
                "hdfs://namenode/warehouse/db/tbl/p1",
                "hdfs://namenode/warehouse/db/tbl/p2"), paths);
    }

    @Test
    public void testGetAllDatabasesThrowsOnException() throws Exception {
        // Mock throwing RuntimeException; withRetry only catches TTransportException,
        // so RuntimeException propagates directly
        when(mockClient.getAllDatabases()).thenThrow(new RuntimeException("HMS unavailable"));

        RuntimeException ex = assertThrows(RuntimeException.class, () -> HmsClient.getAllDatabases());
        assertEquals("HMS unavailable", ex.getMessage());
    }

    @Test
    public void testInvalidateClientIdempotent() {
        // First invalidation closes the mock client and sets client to null
        HmsClient.invalidateClient();
        // Second invalidation should be a no-op (client already null) and not throw
        assertDoesNotThrow(() -> HmsClient.invalidateClient());
    }

    @Test
    public void testInvalidateClientClosesClient() {
        // invalidateClient should close the underlying HMS client
        HmsClient.invalidateClient();
        verify(mockClient, times(1)).close();
    }

    @Test
    public void testInvalidateClientCatchesCloseException() {
        // close() throws; invalidateClient should catch it and still set client to null
        doThrow(new RuntimeException("close failed")).when(mockClient).close();
        assertDoesNotThrow(() -> HmsClient.invalidateClient());
        assertNull(HmsClient.client);
    }

    @Test
    public void testGetTableObjThrowsTException() throws Exception {
        // TException (not TTransportException) should propagate directly without retry
        when(mockClient.getTable("db", "tbl")).thenThrow(new TException("non-transport error"));
        assertThrows(TException.class, () -> HmsClient.getTableObj("db", "tbl"));
    }
}
