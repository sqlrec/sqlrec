package com.sqlrec.executor;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.common.utils.RowTransformUtils;
import com.sqlrec.model.ServiceManager;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ServiceSqlProcessResult extends SqlProcessResult {
    private final String serviceName;
    private volatile long lastCheckTime;
    private volatile boolean cachedCompleted;

    public ServiceSqlProcessResult(
            Enumerable<Object[]> enumerable,
            List<RelDataTypeField> fields,
            String serviceName
    ) {
        super(enumerable, fields);
        this.serviceName = serviceName;
    }

    public static ServiceSqlProcessResult msg(String msg, String fieldName, String serviceName) {
        return new ServiceSqlProcessResult(
                RowTransformUtils.getMsgEnumerable(msg),
                DataTypeUtils.getStringTypeField(fieldName),
                serviceName
        );
    }

    @Override
    public boolean isCompleted() {
        long now = System.currentTimeMillis();
        if (now - lastCheckTime < SqlRecConfigs.COMPLETION_CHECK_CACHE_INTERVAL.getValue()) {
            return cachedCompleted;
        }
        boolean completed = ServiceManager.isServiceOperationCompleted(serviceName);
        cachedCompleted = completed;
        lastCheckTime = System.currentTimeMillis();
        return completed;
    }

    @Override
    public void awaitCompletion(long timeoutMillis) throws InterruptedException, TimeoutException {
        long budget = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        long start = System.nanoTime();
        while (!isCompleted()) {
            long remaining = budget - (System.nanoTime() - start);
            if (remaining <= 0) {
                throw new TimeoutException("Operation completion timed out");
            }
            TimeUnit.NANOSECONDS.sleep(Math.min(TimeUnit.SECONDS.toNanos(1), remaining));
        }
    }
}
