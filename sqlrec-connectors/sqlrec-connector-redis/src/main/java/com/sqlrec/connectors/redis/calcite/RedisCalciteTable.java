package com.sqlrec.connectors.redis.calcite;

import com.sqlrec.common.schema.SqlRecKvTable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.connectors.redis.config.RedisConfig;
import com.sqlrec.connectors.redis.handler.RedisHandler;
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
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.sql.SqlKind;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Type;
import java.util.*;
import java.util.stream.Collectors;

public class RedisCalciteTable extends SqlRecKvTable {
    private RedisConfig redisConfig;
    private RedisHandler redisHandler;

    public RedisCalciteTable(RedisConfig redisConfig) {
        this.redisConfig = redisConfig;
        redisHandler = new RedisHandler(redisConfig);
        redisHandler.open();
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters) {
        if (filters.size() != 1) {
            throw new RuntimeException("Redis Calcite Table only support one filter");
        }
        RexNode filter = filters.get(0);
        if (!filter.isA(SqlKind.EQUALS)) {
            throw new RuntimeException("Redis Calcite Table only support EQUALS filter");
        }
        final RexCall call = (RexCall) filter;
        RexNode left = call.getOperands().get(0);
        if (!(left instanceof RexInputRef)) {
            throw new RuntimeException("Redis Calcite Table only support EQUALS filter with input ref");
        }

        int index = ((RexInputRef) left).getIndex();
        if (index != getPrimaryKeyIndex()) {
            throw new RuntimeException("Redis Calcite Table only support EQUALS filter with primary key");
        }

        final RexNode right = call.getOperands().get(1);
        if (!(right instanceof RexLiteral)) {
            throw new RuntimeException("Redis Calcite Table only support EQUALS filter with literal");
        }

        String value = ((RexLiteral) right).getValue2().toString();
        try {
            return Linq4j.asEnumerable(redisHandler.scan(value).get());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Map<Object, List<Object[]>> getByPrimaryKey(Set<Object> keySet) {
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
        return new RedisCollection(redisHandler);
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

    public static class RedisCollection implements Collection<Object[]> {
        private int size = 0;
        private RedisHandler redisHandler;

        public RedisCollection(RedisHandler redisHandler) {
            this.redisHandler = redisHandler;
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
            size += 1;
            redisHandler.insert(objects);
            return true;
        }

        @Override
        public boolean remove(Object o) {
            if (!(o instanceof Object[])) {
                throw new RuntimeException("Redis Collection only support Object[]");
            }
            size += 1;
            redisHandler.delete((Object[]) o);
            return true;
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

}
