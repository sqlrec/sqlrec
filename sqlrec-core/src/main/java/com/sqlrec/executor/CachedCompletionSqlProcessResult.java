package com.sqlrec.executor;

import com.sqlrec.common.config.SqlRecConfigs;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

abstract class CachedCompletionSqlProcessResult extends SqlProcessResult {
    private volatile long lastCheckTime;
    private volatile boolean cachedCompleted;

    CachedCompletionSqlProcessResult() {
        super();
    }

    CachedCompletionSqlProcessResult(
            Enumerable<Object[]> enumerable,
            List<RelDataTypeField> fields
    ) {
        super(enumerable, fields);
    }

    protected final boolean checkCompletion(BooleanSupplier completionCheck) {
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastCheckTime
                < SqlRecConfigs.COMPLETION_CHECK_CACHE_INTERVAL.getValue()) {
            return cachedCompleted;
        }
        boolean completed = completionCheck.getAsBoolean();
        cachedCompleted = completed;
        lastCheckTime = System.currentTimeMillis();
        return completed;
    }

    @Override
    public void awaitCompletion(long timeoutMillis) throws InterruptedException, TimeoutException {
        // Deployment readiness still uses on-demand polling; Job results use an informer-backed Future.
        long budget = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        long start = System.nanoTime();
        while (!isCompleted()) {
            long remaining = budget - (System.nanoTime() - start);
            if (remaining <= 0) throw new TimeoutException("Operation completion timed out");
            TimeUnit.NANOSECONDS.sleep(Math.min(TimeUnit.SECONDS.toNanos(1), remaining));
        }
    }
}
