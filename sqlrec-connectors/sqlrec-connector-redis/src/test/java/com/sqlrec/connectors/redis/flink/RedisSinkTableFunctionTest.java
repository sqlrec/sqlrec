package com.sqlrec.connectors.redis.flink;

import com.sqlrec.connectors.redis.config.RedisConfig;
import com.sqlrec.connectors.redis.handler.RedisHandler;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Mock unit tests for the buffering Redis Flink sink: verify buffering until
 * batch-size, flush on checkpoint / close, RowKind routing and error wrapping.
 */
@ExtendWith(MockitoExtension.class)
class RedisSinkTableFunctionTest {

    @Mock
    RedisHandler mockHandler;

    private RedisSinkTableFunction<RowData> sink;

    @BeforeEach
    void setUp() throws Exception {
        RedisConfig config = new RedisConfig();
        config.url = "redis://localhost:6379";
        config.database = "db";
        config.tableName = "t";
        config.batchSize = 2;
        // 1 hour: keeps the interval-based flush out of these tests
        config.flushInterval = 3600L;

        ResolvedSchema schema = ResolvedSchema.of(
                Column.physical("id", DataTypes.STRING()),
                Column.physical("name", DataTypes.STRING()));

        sink = new RedisSinkTableFunction<>(config, schema);
        sink.setRedisHandlerForTest(mockHandler);
        sink.open(new Configuration());
    }

    private RowData row(RowKind kind, String id, String name) {
        GenericRowData rowData = new GenericRowData(kind, 2);
        rowData.setField(0, StringData.fromString(id));
        rowData.setField(1, StringData.fromString(name));
        return rowData;
    }

    @Test
    void testInsertBufferedUntilBatchSize() throws Exception {
        sink.invoke(row(RowKind.INSERT, "k1", "v1"), null);
        // below batch size: nothing written yet
        verify(mockHandler, never()).batchInsert(anyList());

        sink.invoke(row(RowKind.INSERT, "k2", "v2"), null);
        // reached batch size: whole buffer flushed in one call
        verify(mockHandler).batchInsert(argThat(rows -> rows.size() == 2));
    }

    @Test
    void testDeleteBufferedUntilBatchSize() throws Exception {
        sink.invoke(row(RowKind.DELETE, "k1", "v1"), null);
        verify(mockHandler, never()).batchDelete(anyList());

        sink.invoke(row(RowKind.DELETE, "k2", "v2"), null);
        verify(mockHandler).batchDelete(argThat(rows -> rows.size() == 2));
        // deletes must not be routed to the insert path
        verify(mockHandler, never()).batchInsert(anyList());
    }

    @Test
    void testUpdateBeforeIgnored() throws Exception {
        sink.invoke(row(RowKind.UPDATE_BEFORE, "k1", "v1"), null);
        verify(mockHandler, never()).batchInsert(anyList());
        verify(mockHandler, never()).batchDelete(anyList());
    }

    @Test
    void testUpdateAfterGoesToInsertBuffer() throws Exception {
        sink.invoke(row(RowKind.UPDATE_AFTER, "k1", "v1"), null);
        sink.invoke(row(RowKind.UPDATE_AFTER, "k2", "v2"), null);
        verify(mockHandler).batchInsert(argThat(rows -> rows.size() == 2));
    }

    @Test
    void testCheckpointFlushesBuffers() throws Exception {
        sink.invoke(row(RowKind.INSERT, "k1", "v1"), null);
        sink.invoke(row(RowKind.DELETE, "k2", "v2"), null);

        // snapshotState must flush both partially filled buffers
        sink.snapshotState(null);
        verify(mockHandler).batchInsert(argThat(rows -> rows.size() == 1));
        verify(mockHandler).batchDelete(argThat(rows -> rows.size() == 1));
    }

    @Test
    void testCloseFlushesAndClosesHandler() throws Exception {
        sink.invoke(row(RowKind.INSERT, "k1", "v1"), null);

        sink.close();
        verify(mockHandler).batchInsert(argThat(rows -> rows.size() == 1));
        verify(mockHandler).close();
    }

    @Test
    void testFlushFailureIsWrapped() throws Exception {
        doThrow(new RuntimeException("redis down")).when(mockHandler).batchInsert(anyList());

        sink.invoke(row(RowKind.INSERT, "k1", "v1"), null);
        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> sink.invoke(row(RowKind.INSERT, "k2", "v2"), null));
        assertTrue(ex.getMessage().contains("Failed to flush insert buffer to Redis"));
    }
}
