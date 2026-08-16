package com.sqlrec.connectors.redis.flink;

import com.sqlrec.common.utils.FlinkSchemaUtils;
import com.sqlrec.connectors.redis.config.RedisConfig;
import com.sqlrec.connectors.redis.handler.RedisHandler;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import java.util.ArrayList;
import java.util.List;

public class RedisSinkTableFunction<IN> extends RichSinkFunction<IN> implements CheckpointedFunction {
    private static final long serialVersionUID = 1L;

    private static final int DEFAULT_BATCH_SIZE = 1000;
    private static final long DEFAULT_FLUSH_INTERVAL_MS = 1000L;

    private RedisConfig redisConfig;
    private List<DataType> dataTypes;
    private transient RedisHandler redisHandler;
    private transient List<Object[]> insertBuffer;
    private transient List<Object[]> deleteBuffer;
    private transient int batchSize;
    private transient long flushIntervalMs;
    private transient long lastFlushTime;

    public RedisSinkTableFunction(RedisConfig redisConfig, ResolvedSchema tableSchema) {
        this.redisConfig = redisConfig;
        dataTypes = tableSchema.getColumnDataTypes();
    }

    /** Test-only: inject a mock handler so open() skips real connection setup. */
    void setRedisHandlerForTest(RedisHandler handler) {
        this.redisHandler = handler;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        if (redisHandler == null) {
            redisHandler = new RedisHandler(redisConfig);
            redisHandler.open();
        }
        insertBuffer = new ArrayList<>();
        deleteBuffer = new ArrayList<>();
        batchSize = (redisConfig.batchSize != null && redisConfig.batchSize > 0)
                ? redisConfig.batchSize : DEFAULT_BATCH_SIZE;
        flushIntervalMs = (redisConfig.flushInterval != null && redisConfig.flushInterval > 0)
                ? redisConfig.flushInterval * 1000L : DEFAULT_FLUSH_INTERVAL_MS;
        lastFlushTime = System.currentTimeMillis();
    }

    @Override
    public void invoke(IN value, Context context) throws Exception {
        super.invoke(value, context);
        if (!(value instanceof RowData)) {
            throw new IllegalArgumentException("Expected RowData but got: " + value.getClass().getName());
        }
        RowData rowData = (RowData) value;
        RowKind kind = rowData.getRowKind();

        Object[] objects = FlinkSchemaUtils.transform(rowData, dataTypes);
        if (kind == RowKind.INSERT || kind == RowKind.UPDATE_AFTER) {
            insertBuffer.add(objects);
            if (insertBuffer.size() >= batchSize) {
                flushInsertBuffer();
            }
        } else if (kind == RowKind.DELETE) {
            deleteBuffer.add(objects);
            if (deleteBuffer.size() >= batchSize) {
                flushDeleteBuffer();
            }
        }

        if (System.currentTimeMillis() - lastFlushTime >= flushIntervalMs) {
            flush();
        }
    }

    private void flush() throws Exception {
        flushInsertBuffer();
        flushDeleteBuffer();
        lastFlushTime = System.currentTimeMillis();
    }

    private void flushInsertBuffer() throws Exception {
        if (insertBuffer.isEmpty()) {
            return;
        }
        // swap in a snapshot so the batch handed to the handler is not mutated
        // afterwards (the buffer is reset immediately)
        List<Object[]> batch = insertBuffer;
        insertBuffer = new ArrayList<>();
        try {
            redisHandler.batchInsert(batch);
        } catch (Exception e) {
            throw new RuntimeException("Failed to flush insert buffer to Redis", e);
        }
    }

    private void flushDeleteBuffer() throws Exception {
        if (deleteBuffer.isEmpty()) {
            return;
        }
        List<Object[]> batch = deleteBuffer;
        deleteBuffer = new ArrayList<>();
        try {
            redisHandler.batchDelete(batch);
        } catch (Exception e) {
            throw new RuntimeException("Failed to flush delete buffer to Redis", e);
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // Flush on checkpoint so buffered records are written before the barrier
        // completes, keeping the sink at-least-once.
        flush();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // No state to restore: unflushed records are replayed from upstream on recovery.
    }

    @Override
    public void close() throws Exception {
        try {
            flush();
        } finally {
            if (redisHandler != null) {
                redisHandler.close();
                redisHandler = null;
            }
            super.close();
        }
    }
}
