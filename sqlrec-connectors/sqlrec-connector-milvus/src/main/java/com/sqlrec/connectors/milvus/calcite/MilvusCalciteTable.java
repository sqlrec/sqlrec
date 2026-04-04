package com.sqlrec.connectors.milvus.calcite;

import com.sqlrec.common.schema.SqlRecVectorTable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.connectors.milvus.config.MilvusConfig;
import com.sqlrec.connectors.milvus.handler.MilvusHandler;
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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Type;
import java.util.*;

public class MilvusCalciteTable extends SqlRecVectorTable {
    private MilvusConfig milvusConfig;
    private MilvusHandler milvusHandler;

    public MilvusCalciteTable(MilvusConfig milvusConfig) {
        this.milvusConfig = milvusConfig;
        this.milvusHandler = new MilvusHandler(milvusConfig);
    }

    @Override
    public Enumerable<@Nullable Object[]> scan(DataContext root, List<RexNode> filters) {
        List<Object[]> rows = milvusHandler.scan(root, filters);
        return Linq4j.asEnumerable(rows);
    }

    public Map<Object, List<Object[]>> getByPrimaryKey(Set<Object> keySet) {
        return milvusHandler.getByPrimaryKey(keySet);
    }

    @Override
    public List<String> getFieldNames() {
        return milvusConfig.fieldSchemas.stream()
                .map(f -> f.getName())
                .collect(java.util.stream.Collectors.toList());
    }

    @Override
    public List<Object[]> searchByEmbeddingWithScore(
            Object[] leftValue,
            List<Float> embedding,
            String annFieldName,
            RexNode filterCondition,
            int limit,
            List<Integer> projectColumns) {
        return milvusHandler.searchByEmbeddingWithScore(
                annFieldName,
                embedding,
                filterCondition,
                leftValue,
                limit,
                projectColumns);
    }

    @Override
    public @Nullable Collection getModifiableCollection() {
        return new MilvusCollection(milvusHandler);
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
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return DataTypeUtils.getRelDataType(typeFactory, milvusConfig.fieldSchemas);
    }

    @Override
    public int getPrimaryKeyIndex() {
        return milvusConfig.primaryKeyIndex;
    }

    @Override
    public boolean onlyFilterByPrimaryKey() {
        return false;
    }

    public static class MilvusCollection implements Collection<Object[]> {
        private int size = 0;
        private MilvusHandler milvusHandler;

        public MilvusCollection(MilvusHandler milvusHandler) {
            this.milvusHandler = milvusHandler;
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
            return milvusHandler.add(objects);
        }

        @Override
        public boolean remove(Object o) {
            if (!(o instanceof Object[])) {
                throw new RuntimeException("Milvus Collection only support Object[]");
            }
            size += 1;
            return milvusHandler.remove((Object[]) o);
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
