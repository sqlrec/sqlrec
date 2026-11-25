package com.sqlrec.connectors.redis.calcite;

import com.sqlrec.connectors.redis.handler.RedisHandler;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
        List<Object[]> newRows = Collections.singletonList(new Object[]{"user2", "new"});
        when(handler.replaceListRows(oldRows, newRows)).thenReturn(1);

        RedisCalciteTable.RedisCollection collection = new RedisCalciteTable.RedisCollection(table, handler);
        assertTrue(collection.replaceAll(oldRows, newRows));
        assertEquals(1, collection.size());
        verify(handler).replaceListRows(oldRows, newRows);
        verify(handler, never()).batchInsert(any());
        verify(table).invalidateCache(oldRows.get(0));
        verify(table).invalidateCache(newRows.get(0));
    }

    @Test
    void nonListUpdateUsesSingleValueReplacement() {
        RedisCalciteTable table = mock(RedisCalciteTable.class);
        RedisHandler handler = mock(RedisHandler.class);
        when(table.getTableName()).thenReturn("test_table");

        List<Object[]> oldRows = Collections.singletonList(new Object[]{"user1", "old"});
        List<Object[]> newRows = Collections.singletonList(new Object[]{"user2", "new"});
        when(handler.replaceSingleValueRows(oldRows, newRows)).thenReturn(1);
        RedisCalciteTable.RedisCollection collection = new RedisCalciteTable.RedisCollection(table, handler);

        assertTrue(collection.replaceAll(oldRows, newRows));
        assertEquals(1, collection.size());
        verify(handler).replaceSingleValueRows(oldRows, newRows);
        verify(handler, never()).replaceListRows(any(), any());
        verify(table).invalidateCache(oldRows.get(0));
        verify(table).invalidateCache(newRows.get(0));
    }

    @Test
    void failedSingleValueBatchInvalidatesEveryAffectedKey() {
        RedisCalciteTable table = mock(RedisCalciteTable.class);
        RedisHandler handler = mock(RedisHandler.class);
        when(table.getTableName()).thenReturn("test_table");
        var collection = new RedisCalciteTable.RedisCollection(table, handler);
        List<Object[]> oldRows = List.of(new Object[]{"user1", "old"}, new Object[]{"user2", "old"});
        List<Object[]> newRows = List.of(new Object[]{"user1", "new"}, new Object[]{"user3", "new"});
        var failure = new IllegalStateException("destination key already exists");
        when(handler.replaceSingleValueRows(oldRows, newRows)).thenThrow(failure);

        assertSame(failure, assertThrows(IllegalStateException.class, () -> collection.replaceAll(oldRows, newRows)));

        assertEquals(0, collection.size());
        oldRows.forEach(row -> verify(table).invalidateCache(row));
        newRows.forEach(row -> verify(table).invalidateCache(row));
    }
}
