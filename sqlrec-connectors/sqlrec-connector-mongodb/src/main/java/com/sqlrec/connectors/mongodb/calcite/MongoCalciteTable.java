package com.sqlrec.connectors.mongodb.calcite;

import com.sqlrec.common.schema.SqlRecCollection;
import com.sqlrec.common.schema.SqlRecKvTable;
import com.sqlrec.common.schema.SqlRecTable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.connectors.mongodb.config.MongoConfig;
import com.sqlrec.connectors.mongodb.handler.MongoHandler;
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

public class MongoCalciteTable extends SqlRecKvTable {
    private final MongoConfig mongoConfig;
    private final MongoHandler mongoHandler;

    public MongoCalciteTable(MongoConfig mongoConfig) {
        this.mongoConfig = mongoConfig;
        this.mongoHandler = new MongoHandler(mongoConfig);
        if (mongoConfig.maxCacheSize != null && mongoConfig.maxCacheSize > 0
                && mongoConfig.cacheTtl != null && mongoConfig.cacheTtl > 0) {
            initCache(mongoConfig.maxCacheSize, mongoConfig.cacheTtl);
        }
    }

    @Override
    protected Enumerable<Object[]> scanImpl(List<RexNode> filters) {
        List<Object[]> rows = mongoHandler.scan(filters);
        return Linq4j.asEnumerable(rows);
    }

    @Override
    public Map<Object, List<Object[]>> getByPrimaryKeyImpl(Set<Object> keySet) {
        return mongoHandler.getByPrimaryKey(keySet);
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return DataTypeUtils.getRelDataType(typeFactory, mongoConfig.fieldSchemas);
    }

    @Override
    public @Nullable Collection getModifiableCollection() {
        return new MongoCollection(this, mongoHandler);
    }

    @Override
    public int getPrimaryKeyIndex() {
        return mongoConfig.primaryKeyIndex;
    }

    @Override
    public boolean onlyFilterByPrimaryKey() {
        return false;
    }

    public static class MongoCollection extends SqlRecCollection {
        private final MongoCalciteTable table;
        private final MongoHandler mongoHandler;

        public MongoCollection(MongoCalciteTable table, MongoHandler mongoHandler) {
            super(table.getTableName());
            this.table = table;
            this.mongoHandler = mongoHandler;
        }

        @Override
        public SqlRecTable getSqlRecTable() {
            return table;
        }

        @Override
        protected boolean addImpl(Object[] objects) {
            return mongoHandler.upsert(objects);
        }

        @Override
        protected boolean removeImpl(Object[] objects) {
            return mongoHandler.delete(objects);
        }
    }
}
