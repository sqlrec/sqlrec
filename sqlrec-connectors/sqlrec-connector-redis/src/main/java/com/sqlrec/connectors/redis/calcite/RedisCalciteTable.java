package com.sqlrec.connectors.redis.calcite;

import com.sqlrec.common.schema.SqlRecCollection;
import com.sqlrec.common.schema.SqlRecKvTable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.connectors.redis.config.RedisConfig;
import com.sqlrec.connectors.redis.handler.RedisHandler;
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
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Type;
import java.util.*;
import java.util.stream.Collectors;

public class RedisCalciteTable extends SqlRecKvTable {
    private RedisConfig redisConfig;
    private transient RedisHandler redisHandler;

    public RedisCalciteTable(RedisConfig redisConfig) {
        this.redisConfig = redisConfig;
        redisHandler = new RedisHandler(redisConfig);
        redisHandler.open();
        initCache(redisConfig.maxCacheSize, redisConfig.cacheTtl);
    }

    public Map<Object, List<Object[]>> getByPrimaryKeyImpl(Set<Object> keySet) {
        Set<String> keySetStr = keySet.stream()
                .map(Object::toString)
                .collect(Collectors.toSet());
        try {
            Map<String, List<Object[]>> scanData = redisHandler.scan(keySetStr).get();
            Map<Object, List<Object[]>> ret = new HashMap<>();
            for (Object key : keySet) {
                ret.put(key, scanData.getOrDefault(key.toString(), new ArrayList<>()));
            }
            return ret;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return DataTypeUtils.getRelDataType(typeFactory, redisConfig.fieldSchemas);
    }

    @Override
    public Collection getModifiableCollection() {
        return new RedisCollection(getTableName(), redisHandler);
    }

    @Override
    public TableModify toModificationRel(RelOptCluster cluster, RelOptTable table, Prepare.CatalogReader catalogReader, RelNode child, TableModify.Operation operation, @Nullable List<String> updateColumnList, @Nullable List<RexNode> sourceExpressionList, boolean flattened) {
        return LogicalTableModify.create(table, catalogReader, child, operation,
                updateColumnList, sourceExpressionList, flattened);
    }

    @Override
    public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
        throw new UnsupportedOperationException();
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

    @Override
    public int getPrimaryKeyIndex() {
        return redisConfig.primaryKeyIndex;
    }

    public static class RedisCollection extends SqlRecCollection {
        private final RedisHandler redisHandler;

        public RedisCollection(String tableName, RedisHandler redisHandler) {
            super(tableName);
            this.redisHandler = redisHandler;
        }

        @Override
        protected boolean addImpl(Object[] objects) {
            redisHandler.insert(objects);
            return true;
        }

        @Override
        protected boolean removeImpl(Object[] objects) {
            redisHandler.delete(objects);
            return true;
        }
    }
}