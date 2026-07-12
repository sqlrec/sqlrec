package com.sqlrec.connectors.milvus.calcite;

import com.sqlrec.common.schema.SqlRecCollection;
import com.sqlrec.common.schema.SqlRecKvTable;
import com.sqlrec.common.schema.SqlRecTable;
import com.sqlrec.common.schema.VectorSearchable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.connectors.milvus.config.MilvusConfig;
import com.sqlrec.connectors.milvus.handler.MilvusHandler;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MilvusCalciteTable extends SqlRecKvTable implements VectorSearchable {
    private MilvusConfig milvusConfig;
    private transient MilvusHandler milvusHandler;

    public MilvusCalciteTable(MilvusConfig milvusConfig) {
        this.milvusConfig = milvusConfig;
        this.milvusHandler = new MilvusHandler(milvusConfig);
    }

    @Override
    protected Enumerable<Object[]> scanImpl(List<RexNode> filters) {
        List<Object[]> rows = milvusHandler.scan(filters);
        return Linq4j.asEnumerable(rows);
    }

    public Map<Object, List<Object[]>> getByPrimaryKeyImpl(Set<Object> keySet) {
        return milvusHandler.getByPrimaryKey(keySet);
    }

    @Override
    public List<Object[]> searchByEmbeddingWithScoreImpl(
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
        return new MilvusCollection(this, milvusHandler);
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

    public static class MilvusCollection extends SqlRecCollection {
        private final MilvusCalciteTable table;
        private final MilvusHandler milvusHandler;

        public MilvusCollection(MilvusCalciteTable table, MilvusHandler milvusHandler) {
            super(table.getTableName());
            this.table = table;
            this.milvusHandler = milvusHandler;
        }

        @Override
        public SqlRecTable getSqlRecTable() {
            return table;
        }

        @Override
        protected boolean addImpl(Object[] objects) {
            return milvusHandler.add(objects);
        }

        @Override
        protected boolean removeImpl(Object[] objects) {
            return milvusHandler.remove(objects);
        }
    }
}
