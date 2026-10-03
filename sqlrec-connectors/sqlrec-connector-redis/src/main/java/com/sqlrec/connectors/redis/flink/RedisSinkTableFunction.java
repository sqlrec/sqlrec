package com.sqlrec.connectors.redis.flink;

import com.sqlrec.common.connector.ChangelogBatchBuffer;
import com.sqlrec.common.connector.ChangelogBatchBuffer.Operation;
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

import java.util.List;

public class RedisSinkTableFunction<IN> extends RichSinkFunction<IN> implements CheckpointedFunction {
    private static final long serialVersionUID = 1L;

    private static final int DEFAULT_BATCH_SIZE = 1000;
    private static final long DEFAULT_FLUSH_INTERVAL_MS = 1000L;

    private RedisConfig redisConfig;
    private List<DataType> dataTypes;
    private transient RedisHandler redisHandler;
    private transient ChangelogBatchBuffer<Object[]> buffer;

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
        int batchSize = (redisConfig.batchSize != null && redisConfig.batchSize > 0)
                ? redisConfig.batchSize : DEFAULT_BATCH_SIZE;
        long flushIntervalMs = (redisConfig.flushInterval != null && redisConfig.flushInterval > 0)
                ? redisConfig.flushInterval * 1000L : DEFAULT_FLUSH_INTERVAL_MS;
        buffer = new ChangelogBatchBuffer<>(batchSize, flushIntervalMs,
                this::writeInserts, this::writeDeletes);
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
            buffer.add(Operation.WRITE, objects);
        } else if (kind == RowKind.DELETE) {
            buffer.add(Operation.DELETE, objects);
        }
        buffer.flushIfDue();
    }

    private void writeInserts(List<Object[]> batch) throws Exception {
        try {
            redisHandler.batchInsert(batch);
        } catch (Exception e) {
            throw new RuntimeException("Failed to flush insert buffer to Redis", e);
        }
    }

    private void writeDeletes(List<Object[]> batch) throws Exception {
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
        buffer.flush();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // No state to restore: unflushed records are replayed from upstream on recovery.
    }

    @Override
    public void close() throws Exception {
        try {
            buffer.flush();
        } finally {
            if (redisHandler != null) {
                redisHandler.close();
                redisHandler = null;
            }
            super.close();
        }
    }
}
