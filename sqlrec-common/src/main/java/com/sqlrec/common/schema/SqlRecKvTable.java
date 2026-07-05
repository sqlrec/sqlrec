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
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.TimeUnit;

public abstract class SqlRecKvTable extends SqlRecTable implements ModifiableTable, FilterableTable {
    private static final Logger log = LoggerFactory.getLogger(SqlRecKvTable.class);

    private transient Cache<Object, List<Object[]>> cache;

    protected abstract Enumerable<Object[]> scanImpl(List<RexNode> filters);

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
                result = scanImpl(filters);
                if (result != null) {
                    List<Object[]> rows = result.toList();
                    DataTypeUtils.convertRowTypes(rows, getRowType(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT)).getFieldList());
                    result = Linq4j.asEnumerable(rows);
                }
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

    @Override
    public TableModify toModificationRel(RelOptCluster cluster, RelOptTable table, Prepare.CatalogReader catalogReader, RelNode child, TableModify.Operation operation, @Nullable List<String> updateColumnList, @Nullable List<RexNode> sourceExpressionList, boolean flattened) {
        return LogicalTableModify.create(table, catalogReader, child, operation,
                updateColumnList, sourceExpressionList, flattened);
    }

    @Override
    public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
        Enumerable<Object[]> data = scan(null, Collections.emptyList());
        return (Queryable<T>) data.asQueryable();
    }

    @Override
    public Type getElementType() {
        return Object[].class;
    }

    @Override
    public Expression getExpression(SchemaPlus schema, String tableName, Class clazz) {
        return Schemas.tableExpression(schema, getElementType(),
                tableName, clazz);
    }

    public abstract int getPrimaryKeyIndex();

    public SqlTypeName getPrimaryKeyType() {
        RelDataType rowType = getRowType(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT));
        return rowType.getFieldList().get(getPrimaryKeyIndex()).getType().getSqlTypeName();
    }

    public abstract Map<Object, List<Object[]>> getByPrimaryKeyImpl(Set<Object> keySet);

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
                Map<Object, List<Object[]>> result = DataTypeUtils.convertMapKeys(
                        getByPrimaryKeyImpl(convertedKeySet), primaryKeyType);
                List<RelDataTypeField> fields = getRowType(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT)).getFieldList();
                for (List<Object[]> rows : result.values()) {
                    DataTypeUtils.convertRowTypes(rows, fields);
                }
                return result;
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
                List<RelDataTypeField> fields = getRowType(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT)).getFieldList();
                for (Map.Entry<Object, List<Object[]>> entry : missKeyResult.entrySet()) {
                    DataTypeUtils.convertRowTypes(entry.getValue(), fields);
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

    public void invalidateCache(Object[] row) {
        if (cache == null) {
            return;
        }
        int pkIndex = getPrimaryKeyIndex();
        if (pkIndex < row.length) {
            SqlTypeName primaryKeyType = getPrimaryKeyType();
            Object primaryKeyValue = DataTypeUtils.convertType(row[pkIndex], primaryKeyType);
            cache.invalidate(primaryKeyValue);
        }
    }

    public boolean onlyFilterByPrimaryKey() {
        return true;
    }
}
