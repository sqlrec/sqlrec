package com.sqlrec.utils;

import com.sqlrec.compiler.CompileManager;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class NodeUtilsTest {

    @Test
    public void testGetTableFromSqlNodeWithNull() {
        List<String> result = NodeUtils.getTableFromSqlNode(null);
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testGetTableFromSqlNodeWithSelect() throws Exception {
        SqlNode sqlNode = CompileManager.parseFlinkSql("select * from my_table");
        List<String> result = NodeUtils.getTableFromSqlNode(sqlNode);
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("my_table", result.get(0));
    }

    @Test
    public void testGetTableFromSqlNodeWithJoin() throws Exception {
        SqlNode sqlNode = CompileManager.parseFlinkSql("select * from t1 join t2 on t1.id = t2.id");
        List<String> result = NodeUtils.getTableFromSqlNode(sqlNode);
        assertNotNull(result);
        assertEquals(2, result.size());
        assertTrue(result.contains("t1"));
        assertTrue(result.contains("t2"));
    }

    @Test
    public void testGetTableFromSqlNodeWithInsert() throws Exception {
        SqlNode sqlNode = CompileManager.parseFlinkSql("insert into target_table select * from source_table");
        List<String> result = NodeUtils.getTableFromSqlNode(sqlNode);
        assertNotNull(result);
        assertEquals(2, result.size());
        assertTrue(result.contains("target_table"));
        assertTrue(result.contains("source_table"));
    }

    @Test
    public void testGetModifyTablesFromSqlNodeWithNull() {
        List<String> result = NodeUtils.getModifyTablesFromSqlNode(null);
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testGetModifyTablesFromSqlNodeWithInsert() throws Exception {
        SqlNode sqlNode = CompileManager.parseFlinkSql("insert into target_table select * from source_table");
        List<String> result = NodeUtils.getModifyTablesFromSqlNode(sqlNode);
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("target_table", result.get(0));
    }

    @Test
    public void testGetModifyTablesFromSqlNodeWithSelect() throws Exception {
        SqlNode sqlNode = CompileManager.parseFlinkSql("select * from my_table");
        List<String> result = NodeUtils.getModifyTablesFromSqlNode(sqlNode);
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testGetModifyTablesFromSqlNodeWithUpdate() throws Exception {
        SqlNode sqlNode = CompileManager.parseFlinkSql("update target_table set col1 = 'value' where id = 1");
        List<String> result = NodeUtils.getModifyTablesFromSqlNode(sqlNode);
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("target_table", result.get(0));
    }

    @Test
    public void testGetModifyTablesFromSqlNodeWithDelete() throws Exception {
        SqlNode sqlNode = CompileManager.parseFlinkSql("delete from target_table where id = 1");
        List<String> result = NodeUtils.getModifyTablesFromSqlNode(sqlNode);
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("target_table", result.get(0));
    }

    @Test
    public void testIsKvTableWithNull() {
        boolean result = NodeUtils.isKvTable(null);
        assertFalse(result);
    }

    @Test
    public void testGetScanTableWithNull() {
        var result = NodeUtils.getScanTable(null);
        assertNull(result);
    }

    @Test
    public void testGetScanKVTableWithNull() {
        var result = NodeUtils.getScanKVTable(null);
        assertNull(result);
    }

    @Test
    public void testGetScanCacheTableWithNull() {
        var result = NodeUtils.getScanCacheTable(null);
        assertNull(result);
    }

    @Test
    public void testIsScanKVTableWithNull() {
        boolean result = NodeUtils.isScanKVTable(null);
        assertFalse(result);
    }
}
