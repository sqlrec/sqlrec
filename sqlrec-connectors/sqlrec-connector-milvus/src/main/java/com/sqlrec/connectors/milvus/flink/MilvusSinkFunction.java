package com.sqlrec.connectors.milvus.flink;

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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class MilvusSinkFunction<IN> extends RichSinkFunction<IN> implements CheckpointedFunction, Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(MilvusSinkFunction.class);
    /**
     * Upper bound on submitted-but-unfinished batches. Without this bound a slow Milvus
     * server would let the flush queue grow without limit (invoke never blocks, so Flink
     * applies no backpressure) until the TaskManager runs out of memory.
     */
    private static final int MAX_PENDING_BATCHES = 4;

    private MilvusConfig milvusConfig;
    private List<org.apache.flink.table.types.DataType> dataTypes;
    private transient MilvusHandler milvusHandler;
    // INSERT and UPDATE_AFTER rows both go through upsert (addBatch) so rows with an
    // existing primary key are replaced, matching the original sink semantics.
    private transient List<Object[]> upsertBuffer;
    private transient List<Object[]> deleteBuffer;
    private transient int batchSize;
    private transient long flushIntervalMs;
    private transient long lastFlushTime;
    /**
     * Single-thread executor draining flush tasks in submission order. Batches are
     * executed one at a time while the invoke thread keeps buffering the next batch,
     * overlapping network/server latency with buffering. Order is preserved, so rows
     * with the same primary key are always applied in arrival order.
     */
    private transient ExecutorService flushExecutor;
    private transient CompletableFuture<Void> pendingFlush;
    private transient AtomicInteger pendingBatches;

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
        this.upsertBuffer = new ArrayList<>();
        this.deleteBuffer = new ArrayList<>();
        this.batchSize = milvusConfig.batchSize != null && milvusConfig.batchSize > 0
                ? milvusConfig.batchSize : 4096;
        this.flushIntervalMs = (milvusConfig.flushInterval != null && milvusConfig.flushInterval > 0
                ? milvusConfig.flushInterval : 1L) * 1000L;
        this.lastFlushTime = System.currentTimeMillis();
        this.flushExecutor = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "milvus-sink-flusher");
            thread.setDaemon(true);
            return thread;
        });
        this.pendingFlush = CompletableFuture.completedFuture(null);
        this.pendingBatches = new AtomicInteger(0);

        logger.info("MilvusSinkFunction initialized with batch size: {}, flush interval: {}ms",
                batchSize, flushIntervalMs);
    }

    @Override
    public void invoke(IN value, Context context) throws Exception {
        super.invoke(value, context);
        rethrowAsyncFlushError();

        RowData rowData = (RowData) value;
        RowKind kind = rowData.getRowKind();

        Object[] objects = FlinkSchemaUtils.transform(rowData, dataTypes);

        if (kind == RowKind.INSERT || kind == RowKind.UPDATE_AFTER) {
            upsertBuffer.add(objects);
            if (upsertBuffer.size() >= batchSize) {
                submitUpsertFlush();
            }
        } else if (kind == RowKind.DELETE) {
            deleteBuffer.add(objects);
            if (deleteBuffer.size() >= batchSize) {
                submitDeleteFlush();
            }
        }

        if (System.currentTimeMillis() - lastFlushTime >= flushIntervalMs) {
            submitAllFlushes();
        }
    }

    /**
     * Submits the non-empty buffers, in upsert -> delete order, as tasks appended to
     * the flush chain.
     */
    private void submitAllFlushes() throws Exception {
        submitUpsertFlush();
        submitDeleteFlush();
        lastFlushTime = System.currentTimeMillis();
    }

    private void submitUpsertFlush() throws Exception {
        if (upsertBuffer.isEmpty()) {
            return;
        }
        List<Object[]> batch = upsertBuffer;
        upsertBuffer = new ArrayList<>();
        submit(() -> {
            milvusHandler.addBatch(batch);
            logger.debug("Flushed {} upsert records to Milvus", batch.size());
        });
    }

    private void submitDeleteFlush() throws Exception {
        if (deleteBuffer.isEmpty()) {
            return;
        }
        List<Object[]> batch = deleteBuffer;
        deleteBuffer = new ArrayList<>();
        submit(() -> {
            milvusHandler.removeBatch(batch);
            logger.debug("Flushed {} delete records to Milvus", batch.size());
        });
    }

    private void submit(Runnable flushTask) throws Exception {
        // During Flink's operator-chain shutdown, operators are closed in reverse
        // order (downstream first). This means close() shuts down the flush executor
        // before upstream operators finish flushing. When the upstream operator's
        // close() sends remaining records through the chain, invoke() is still called
        // on this sink, but the executor is already terminated. In that case, run the
        // flush task synchronously in the caller thread.
        if (flushExecutor.isShutdown()) {
            flushTask.run();
            return;
        }
        Runnable countedTask = () -> {
            try {
                flushTask.run();
            } finally {
                pendingBatches.decrementAndGet();
            }
        };
        pendingBatches.incrementAndGet();
        pendingFlush = pendingFlush.thenRunAsync(countedTask, flushExecutor);
        if (pendingBatches.get() >= MAX_PENDING_BATCHES) {
            // Backpressure: block until the in-flight batches finish (or fail) so the
            // flush queue stays bounded while Milvus is slow.
            awaitFlushComplete();
        }
    }

    /**
     * Propagates an asynchronous flush failure to the pipeline so the job fails over
     * and the records are replayed from the last checkpoint.
     */
    private void rethrowAsyncFlushError() throws Exception {
        if (pendingFlush != null && pendingFlush.isCompletedExceptionally()) {
            pendingFlush.get();
        }
    }

    private void awaitFlushComplete() throws Exception {
        if (pendingFlush != null) {
            pendingFlush.get();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // Flush on checkpoint so buffered records are written before the barrier
        // completes, keeping the sink at-least-once.
        submitAllFlushes();
        awaitFlushComplete();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // No state to restore: unflushed records are replayed from upstream on recovery.
    }

    @Override
    public void close() throws Exception {
        try {
            submitAllFlushes();
            awaitFlushComplete();
        } catch (Exception e) {
            logger.error("Error flushing remaining records in close()", e);
            throw e;
        } finally {
            if (flushExecutor != null) {
                flushExecutor.shutdown();
            }
            super.close();
        }
    }
}