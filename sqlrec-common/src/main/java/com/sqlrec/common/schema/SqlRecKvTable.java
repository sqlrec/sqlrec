package com.sqlrec.common.schema;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.sqlrec.common.config.Consts;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.common.utils.FilterUtils;
import com.sqlrec.common.utils.MetricsUtils;
import io.micrometer.core.instrument.Tags;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

public abstract class SqlRecKvTable extends SqlRecTable implements ModifiableTable, FilterableTable {
    private static final Logger log = LoggerFactory.getLogger(SqlRecKvTable.class);

    private transient Cache<Object, List<Object[]>> cache;

    protected Enumerable<Object[]> scanImpl(DataContext root, List<RexNode> filters) {
        throw new UnsupportedOperationException("scan not support");
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters) {
        long startTime = System.currentTimeMillis();
        long count = 0;
        String status = "success";

        try {
            Enumerable<Object[]> result;
            Object primaryKeyValue = FilterUtils.extractPrimaryKeyValue(filters, getPrimaryKeyIndex());
            if (primaryKeyValue != null) {
                SqlTypeName primaryKeyType = getPrimaryKeyType();
                primaryKeyValue = DataTypeUtils.convertType(primaryKeyValue, primaryKeyType);
                Map<Object, List<Object[]>> keyResult = getByPrimaryKey(Collections.singleton(primaryKeyValue));
                List<Object[]> rows = keyResult.getOrDefault(primaryKeyValue, Collections.emptyList());
                result = Linq4j.asEnumerable(rows);
            } else {
                result = scanImpl(root, filters);
            }
            if (result != null) {
                count = result.count();
            }
            return result;
        } catch (Throwable e) {
            log.error("scan table {} error", getTableName(), e);
            status = "error";
            throw e;
        } finally {
            Tags tags = MetricsUtils.createTags(Collections.emptyMap(), "table", getTableName(), "status", status);
            MetricsUtils.getCompositeMeterRegistry()
                    .timer(Consts.METRICS_TABLE_SCAN_DURATION, tags)
                    .record(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
            MetricsUtils.getCompositeMeterRegistry()
                    .summary(Consts.METRICS_TABLE_SCAN_DATA_SIZE, tags)
                    .record(count);
        }
    }

    public int getPrimaryKeyIndex() {
        throw new UnsupportedOperationException("getPrimaryKeyIndex not support");
    }

    public SqlTypeName getPrimaryKeyType() {
        RelDataType rowType = getRowType(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT));
        return rowType.getFieldList().get(getPrimaryKeyIndex()).getType().getSqlTypeName();
    }

    public Map<Object, List<Object[]>> getByPrimaryKeyImpl(Set<Object> keySet) {
        throw new UnsupportedOperationException("getByPrimaryKey not support");
    }

    public void initCache(int maxSize, long expireAfterWrite) {
        if (maxSize <= 0 || expireAfterWrite <= 0) {
            return;
        }
        cache = Caffeine.newBuilder()
                .maximumSize(maxSize)
                .expireAfterWrite(expireAfterWrite, TimeUnit.SECONDS)
                .build();
    }

    public Map<Object, List<Object[]>> getByPrimaryKey(Set<Object> keySet) {
        long startTime = System.currentTimeMillis();
        String status = "success";
        int totalCount = keySet.size();

        try {
            SqlTypeName primaryKeyType = getPrimaryKeyType();
            Set<Object> convertedKeySet = DataTypeUtils.convertKeySet(keySet, primaryKeyType);

            if (cache == null) {
                return DataTypeUtils.convertMapKeys(getByPrimaryKeyImpl(convertedKeySet), primaryKeyType);
            }

            int hitCount = 0;
            Map<Object, List<Object[]>> result = new HashMap<>(convertedKeySet.size());
            Set<Object> missKeys = new HashSet<>();
            for (Object key : convertedKeySet) {
                List<Object[]> list = cache.getIfPresent(key);
                if (list != null) {
                    result.put(key, list);
                    hitCount++;
                } else {
                    missKeys.add(key);
                }
            }
            if (!missKeys.isEmpty()) {
                Map<Object, List<Object[]>> missKeyResult = DataTypeUtils.convertMapKeys(
                        getByPrimaryKeyImpl(missKeys), primaryKeyType);
                for (Map.Entry<Object, List<Object[]>> entry : missKeyResult.entrySet()) {
                    result.put(entry.getKey(), entry.getValue());
                    cache.put(entry.getKey(), entry.getValue());
                }
            }

            Tags cacheTags = MetricsUtils.createTags(Collections.emptyMap(), "table", getTableName(), "status", status);
            MetricsUtils.getCompositeMeterRegistry()
                    .counter(Consts.METRICS_TABLE_CACHE_HIT_COUNT, cacheTags)
                    .increment(hitCount);
            MetricsUtils.getCompositeMeterRegistry()
                    .summary(Consts.METRICS_TABLE_CACHE_DATA_SIZE, cacheTags)
                    .record(cache.estimatedSize());

            return result;
        } catch (Throwable e) {
            log.error("getByPrimaryKey table {} error", getTableName(), e);
            status = "error";
            throw e;
        } finally {
            long duration = System.currentTimeMillis() - startTime;
            Tags tags = MetricsUtils.createTags(Collections.emptyMap(), "table", getTableName(), "status", status);
            MetricsUtils.getCompositeMeterRegistry()
                    .timer(Consts.METRICS_TABLE_GET_BY_PRIMARY_KEY_DURATION, tags)
                    .record(duration, TimeUnit.MILLISECONDS);
            MetricsUtils.getCompositeMeterRegistry()
                    .summary(Consts.METRICS_TABLE_GET_BY_PRIMARY_KEY_DATA_SIZE, tags)
                    .record(totalCount);
        }
    }

    public boolean onlyFilterByPrimaryKey() {
        return true;
    }
}
