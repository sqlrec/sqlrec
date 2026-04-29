package com.sqlrec.connectors.milvus.flink;

import com.sqlrec.common.utils.FlinkSchemaUtils;
import com.sqlrec.connectors.milvus.config.MilvusConfig;
import com.sqlrec.connectors.milvus.handler.MilvusHandler;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class MilvusSinkFunction<IN> extends RichSinkFunction<IN> implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(MilvusSinkFunction.class);

    private MilvusConfig milvusConfig;
    private List<org.apache.flink.table.types.DataType> dataTypes;
    private transient MilvusHandler milvusHandler;
    private transient List<Object[]> insertBuffer;
    private transient List<Object[]> deleteBuffer;
    private transient int batchSize;
    private transient long flushIntervalMs;
    private transient long lastFlushTime;

    public MilvusSinkFunction(MilvusConfig milvusConfig, ResolvedSchema tableSchema) {
        this.milvusConfig = milvusConfig;
        this.dataTypes = tableSchema.getColumnDataTypes();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.milvusHandler = new MilvusHandler(milvusConfig);
        this.insertBuffer = new ArrayList<>();
        this.deleteBuffer = new ArrayList<>();
        this.batchSize = milvusConfig.batchSize != null ? milvusConfig.batchSize : 1024;
        this.flushIntervalMs = (milvusConfig.flushInterval != null ? milvusConfig.flushInterval : 5L) * 1000L;
        this.lastFlushTime = System.currentTimeMillis();
        
        logger.info("MilvusSinkFunction initialized with batch size: {}, flush interval: {}ms", 
                batchSize, flushIntervalMs);
    }

    @Override
    public void close() throws Exception {
        try {
            flush();
        } catch (Exception e) {
            logger.error("Error flushing remaining records in close()", e);
            throw e;
        } finally {
            super.close();
        }
    }

    @Override
    public void invoke(IN value, Context context) throws Exception {
        super.invoke(value, context);
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
        
        checkAndFlush();
    }

    private void checkAndFlush() throws Exception {
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastFlushTime >= flushIntervalMs) {
            flush();
            lastFlushTime = currentTime;
        }
    }

    private void flush() throws Exception {
        flushInsertBuffer();
        flushDeleteBuffer();
    }

    private void flushInsertBuffer() throws Exception {
        if (insertBuffer.isEmpty()) {
            return;
        }
        
        try {
            milvusHandler.addBatch(insertBuffer);
            logger.debug("Flushed {} insert records to Milvus", insertBuffer.size());
            insertBuffer.clear();
        } catch (Exception e) {
            logger.error("Error flushing insert buffer", e);
            throw e;
        }
    }

    private void flushDeleteBuffer() throws Exception {
        if (deleteBuffer.isEmpty()) {
            return;
        }
        
        try {
            milvusHandler.removeBatch(deleteBuffer);
            logger.debug("Flushed {} delete records to Milvus", deleteBuffer.size());
            deleteBuffer.clear();
        } catch (Exception e) {
            logger.error("Error flushing delete buffer", e);
            throw e;
        }
    }
}