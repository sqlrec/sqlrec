package com.sqlrec.connectors.milvus.flink;

import com.sqlrec.common.connector.ChangelogBatchBuffer;
import com.sqlrec.common.connector.ChangelogBatchBuffer.Operation;
import com.sqlrec.common.utils.FlinkSchemaUtils;
import com.sqlrec.connectors.milvus.config.MilvusConfig;
import com.sqlrec.connectors.milvus.handler.MilvusHandler;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;

public class MilvusSinkFunction<IN> extends RichSinkFunction<IN> implements CheckpointedFunction, Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(MilvusSinkFunction.class);
    private MilvusConfig milvusConfig;
    private List<org.apache.flink.table.types.DataType> dataTypes;
    private transient MilvusHandler milvusHandler;
    private transient ChangelogBatchBuffer<Object[]> buffer;

    public MilvusSinkFunction(MilvusConfig milvusConfig, ResolvedSchema tableSchema) {
        this.milvusConfig = milvusConfig;
        this.dataTypes = tableSchema.getColumnDataTypes();
    }

    /** Test-only: inject a mock handler so open() skips real connection setup. */
    void setMilvusHandlerForTest(MilvusHandler handler) {
        this.milvusHandler = handler;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        if (this.milvusHandler == null) {
            this.milvusHandler = new MilvusHandler(milvusConfig);
        }
        int batchSize = milvusConfig.batchSize != null && milvusConfig.batchSize > 0
                ? milvusConfig.batchSize : 4096;
        long flushIntervalMs = (milvusConfig.flushInterval != null && milvusConfig.flushInterval > 0
                ? milvusConfig.flushInterval : 1L) * 1000L;
        buffer = new ChangelogBatchBuffer<>(batchSize, flushIntervalMs,
                this::writeUpserts, this::writeDeletes);
        logger.info("MilvusSinkFunction initialized with batch size: {}, flush interval: {}ms",
                batchSize, flushIntervalMs);
    }

    @Override
    public void invoke(IN value, Context context) throws Exception {
        super.invoke(value, context);
        RowData rowData = (RowData) value;
        RowKind kind = rowData.getRowKind();

        Object[] objects = FlinkSchemaUtils.transform(rowData, dataTypes);

        if (kind == RowKind.INSERT || kind == RowKind.UPDATE_AFTER) {
            // INSERT and UPDATE_AFTER both replace rows with an existing primary key.
            buffer.add(Operation.WRITE, objects);
        } else if (kind == RowKind.DELETE) {
            buffer.add(Operation.DELETE, objects);
        }
        buffer.flushIfDue();
    }

    private void writeUpserts(List<Object[]> batch) {
        milvusHandler.addBatch(batch);
        logger.debug("Flushed {} upsert records to Milvus", batch.size());
    }

    private void writeDeletes(List<Object[]> batch) {
        milvusHandler.removeBatch(batch);
        logger.debug("Flushed {} delete records to Milvus", batch.size());
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
        } catch (Exception e) {
            logger.error("Error flushing remaining records in close()", e);
            throw e;
        } finally {
            super.close();
        }
    }
}
