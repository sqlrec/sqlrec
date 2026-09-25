package com.sqlrec.connectors.redis.calcite;

import com.sqlrec.connectors.redis.handler.RedisHandler;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class RedisCalciteTableTest {

    @Test
    void listUpdateUsesRowReplacement() {
        RedisCalciteTable table = mock(RedisCalciteTable.class);
        RedisHandler handler = mock(RedisHandler.class);
        when(table.getTableName()).thenReturn("test_table");
        when(handler.isListMode()).thenReturn(true);

        List<Object[]> oldRows = Collections.singletonList(new Object[]{"user1", "old"});
        List<Object[]> newRows = Collections.singletonList(new Object[]{"user1", "new"});
        when(handler.replaceListRows(oldRows, newRows)).thenReturn(1);

        RedisCalciteTable.RedisCollection collection = new RedisCalciteTable.RedisCollection(table, handler);
        assertTrue(collection.replaceAll(oldRows, newRows));
        assertEquals(1, collection.size());
        verify(handler).replaceListRows(oldRows, newRows);
        verify(handler, never()).batchInsert(any());
    }

    @Test
    void nonListUpdateKeepsUpsertBehavior() {
        RedisCalciteTable table = mock(RedisCalciteTable.class);
        RedisHandler handler = mock(RedisHandler.class);
        when(table.getTableName()).thenReturn("test_table");

        List<Object[]> oldRows = Collections.singletonList(new Object[]{"user1", "old"});
        List<Object[]> newRows = Collections.singletonList(new Object[]{"user1", "new"});
        RedisCalciteTable.RedisCollection collection = new RedisCalciteTable.RedisCollection(table, handler);

        assertTrue(collection.replaceAll(oldRows, newRows));
        assertEquals(1, collection.size());
        verify(handler).batchInsert(newRows);
        verify(handler, never()).replaceListRows(any(), any());
    }
}
