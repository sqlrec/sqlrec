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

    /**
     * Number of rows accepted by the current modification operation.
     *
     * <p>This is deliberately not the remote table cardinality. A remote table may not
     * support an efficient or consistent count query, while Calcite only uses the value
     * before and after a TableModify operation to calculate the affected-row count.</p>
     */
    protected int size = 0;
    protected final String tableName;

    public SqlRecCollection(String tableName) {
        this.tableName = tableName;
    }

    public abstract SqlRecTable getSqlRecTable();

    protected abstract boolean addImpl(Object[] objects);

    protected abstract boolean removeImpl(Object[] objects);

    protected boolean addAllImpl(Collection<? extends Object[]> c) {
        boolean modified = false;
        for (Object[] objects : c) {
            modified |= addImpl(objects);
        }
        return modified;
    }

    protected boolean removeAllImpl(Collection<?> c) {
        boolean modified = false;
        for (Object o : c) {
            modified |= removeImpl((Object[]) o);
        }
        return modified;
    }

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
            if (result) {
                size += 1;
            }
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
            // Keep the counter monotonic. Calcite compares the collection size before and
            // after TableModify; decrementing from the operation-local zero baseline would
            // produce a negative intermediate value for remote deletes.
            if (result) {
                size += 1;
            }
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
        long startTime = System.currentTimeMillis();
        String status = "success";

        try {
            boolean result = addAllImpl(c);
            if (result) {
                // A successful batch is counted as accepted rows. Individual remote
                // affected-row counts are not required by this compatibility layer.
                size += c.size();
            }
            for (Object[] row : c) {
                invalidateCacheIfNeeded(row);
            }
            return result;
        } catch (Throwable e) {
            log.error("addAll to table {} error", tableName, e);
            status = "error";
            throw e;
        } finally {
            Tags tags = MetricsUtils.createTags(Collections.emptyMap(), "table", tableName, "operation", "addAll", "status", status);
            MetricsUtils.getCompositeMeterRegistry()
                    .timer(Consts.METRICS_TABLE_COLLECTION_ADD_DURATION, tags)
                    .record(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        for (Object o : c) {
            if (!(o instanceof Object[])) {
                throw new RuntimeException("SqlRecCollection only support Object[]");
            }
        }

        long startTime = System.currentTimeMillis();
        String status = "success";

        try {
            boolean result = removeAllImpl(c);
            if (result) {
                // A successful batch is counted as accepted rows. The counter remains
                // operation-local and monotonic because remote table cardinality is unknown.
                size += c.size();
            }
            for (Object o : c) {
                invalidateCacheIfNeeded((Object[]) o);
            }
            return result;
        } catch (Throwable e) {
            log.error("removeAll from table {} error", tableName, e);
            status = "error";
            throw e;
        } finally {
            Tags tags = MetricsUtils.createTags(Collections.emptyMap(), "table", tableName, "operation", "removeAll", "status", status);
            MetricsUtils.getCompositeMeterRegistry()
                    .timer(Consts.METRICS_TABLE_COLLECTION_REMOVE_DURATION, tags)
                    .record(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
        }
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
