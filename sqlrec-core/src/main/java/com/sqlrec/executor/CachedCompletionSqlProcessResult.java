package com.sqlrec.executor;

import com.sqlrec.common.config.SqlRecConfigs;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.List;
import java.util.function.BooleanSupplier;

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
        lastCheckTime = currentTime;

        boolean completed = completionCheck.getAsBoolean();
        cachedCompleted = completed;
        return completed;
    }
}
