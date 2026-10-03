package com.sqlrec.common.connector;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * Buffers consecutive writes or deletes without reordering changelog operations.
 * Callers flush on checkpoints/close and check the interval when receiving records.
 * This buffer is confined to the sink thread; failed batches are replayed by the runtime.
 */
public final class ChangelogBatchBuffer<T> {
    public enum Operation { WRITE, DELETE }

    @FunctionalInterface
    public interface BatchWriter<T> {
        void write(List<T> batch) throws Exception;
    }

    private final int batchSize;
    private final long flushIntervalMs;
    private final BatchWriter<T> writeBatch;
    private final BatchWriter<T> deleteBatch;
    private final LongSupplier clock;
    private List<T> buffer = new ArrayList<>();
    private Operation operation;
    private long lastFlushTime;

    public ChangelogBatchBuffer(int batchSize, long flushIntervalMs,
            BatchWriter<T> writeBatch, BatchWriter<T> deleteBatch) {
        this(batchSize, flushIntervalMs, writeBatch, deleteBatch, System::currentTimeMillis);
    }

    ChangelogBatchBuffer(int batchSize, long flushIntervalMs,
            BatchWriter<T> writeBatch, BatchWriter<T> deleteBatch, LongSupplier clock) {
        if (batchSize <= 0 || flushIntervalMs <= 0) {
            throw new IllegalArgumentException("batch size and flush interval must be positive");
        }
        this.batchSize = batchSize;
        this.flushIntervalMs = flushIntervalMs;
        this.writeBatch = Objects.requireNonNull(writeBatch, "writeBatch");
        this.deleteBatch = Objects.requireNonNull(deleteBatch, "deleteBatch");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.lastFlushTime = clock.getAsLong();
    }

    public void add(Operation nextOperation, T value) throws Exception {
        Objects.requireNonNull(nextOperation, "operation");
        if (operation != nextOperation) {
            flushBatch();
            operation = nextOperation;
        }
        buffer.add(value);
        if (buffer.size() >= batchSize) {
            flushBatch();
        }
    }

    public void flushIfDue() throws Exception {
        if (clock.getAsLong() - lastFlushTime >= flushIntervalMs) {
            flush();
        }
    }

    public void flush() throws Exception {
        flushBatch();
        lastFlushTime = clock.getAsLong();
    }

    private void flushBatch() throws Exception {
        if (buffer.isEmpty()) {
            return;
        }
        // Hand off a stable batch; subsequent records must not mutate the writer's list.
        List<T> batch = buffer;
        buffer = new ArrayList<>();
        (operation == Operation.WRITE ? writeBatch : deleteBatch).write(batch);
    }
}
