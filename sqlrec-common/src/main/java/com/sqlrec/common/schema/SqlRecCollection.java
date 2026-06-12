package com.sqlrec.common.schema;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.utils.MetricsUtils;
import io.micrometer.core.instrument.Tags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public abstract class SqlRecCollection implements Collection<Object[]> {
    private static final Logger log = LoggerFactory.getLogger(SqlRecCollection.class);

    protected int size = 0;
    protected final String tableName;

    public SqlRecCollection(String tableName) {
        this.tableName = tableName;
    }

    public abstract SqlRecTable getSqlRecTable();

    protected abstract boolean addImpl(Object[] objects);

    protected abstract boolean removeImpl(Object[] objects);

    private void invalidateCacheIfNeeded(Object[] row) {
        SqlRecTable table = getSqlRecTable();
        if (table instanceof SqlRecKvTable) {
            ((SqlRecKvTable) table).invalidateCache(row);
        }
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public boolean contains(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Object[]> iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean add(Object[] objects) {
        long startTime = System.currentTimeMillis();
        String status = "success";

        try {
            boolean result = addImpl(objects);
            size += 1;
            invalidateCacheIfNeeded(objects);
            return result;
        } catch (Throwable e) {
            log.error("add to table {} error", tableName, e);
            status = "error";
            throw e;
        } finally {
            Tags tags = MetricsUtils.createTags(Collections.emptyMap(), "table", tableName, "operation", "add", "status", status);
            MetricsUtils.getCompositeMeterRegistry()
                    .timer(Consts.METRICS_TABLE_COLLECTION_ADD_DURATION, tags)
                    .record(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public boolean remove(Object o) {
        if (!(o instanceof Object[])) {
            throw new RuntimeException("SqlRecCollection only support Object[]");
        }

        long startTime = System.currentTimeMillis();
        String status = "success";

        try {
            boolean result = removeImpl((Object[]) o);
            size += 1;
            invalidateCacheIfNeeded((Object[]) o);
            return result;
        } catch (Throwable e) {
            log.error("remove from table {} error", tableName, e);
            status = "error";
            throw e;
        } finally {
            Tags tags = MetricsUtils.createTags(Collections.emptyMap(), "table", tableName, "operation", "remove", "status", status);
            MetricsUtils.getCompositeMeterRegistry()
                    .timer(Consts.METRICS_TABLE_COLLECTION_REMOVE_DURATION, tags)
                    .record(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends Object[]> c) {
        for (Object[] objects : c) {
            add(objects);
        }
        return true;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        for (Object o : c) {
            remove(o);
        }
        return true;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }
}
