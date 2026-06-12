package com.sqlrec.connectors.jdbc.calcite;

import com.sqlrec.common.schema.SqlRecCollection;
import com.sqlrec.common.schema.SqlRecKvTable;
import com.sqlrec.common.schema.SqlRecTable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.connectors.jdbc.config.JdbcConfig;
import com.sqlrec.connectors.jdbc.handler.JdbcHandler;
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

public class JdbcCalciteTable extends SqlRecKvTable {
    private final JdbcConfig jdbcConfig;
    private transient JdbcHandler jdbcHandler;

    public JdbcCalciteTable(JdbcConfig jdbcConfig) {
        this.jdbcConfig = jdbcConfig;
        this.jdbcHandler = new JdbcHandler(jdbcConfig);
        if (jdbcConfig.maxCacheSize != null && jdbcConfig.maxCacheSize > 0
                && jdbcConfig.cacheTtl != null && jdbcConfig.cacheTtl > 0) {
            initCache(jdbcConfig.maxCacheSize, jdbcConfig.cacheTtl);
        }
    }

    @Override
    protected Enumerable<Object[]> scanImpl(DataContext root, List<RexNode> filters) {
        List<Object[]> rows = jdbcHandler.scan(filters);
        return Linq4j.asEnumerable(rows);
    }

    @Override
    public Map<Object, List<Object[]>> getByPrimaryKeyImpl(Set<Object> keySet) {
        return jdbcHandler.getByPrimaryKey(keySet);
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return DataTypeUtils.getRelDataType(typeFactory, jdbcConfig.fieldSchemas);
    }

    @Override
    public @Nullable Collection getModifiableCollection() {
        return new JdbcCollection(this, jdbcHandler);
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
        return jdbcConfig.primaryKeyIndex;
    }

    @Override
    public boolean onlyFilterByPrimaryKey() {
        return false;
    }

    public static class JdbcCollection extends SqlRecCollection {
        private final JdbcCalciteTable table;
        private final JdbcHandler jdbcHandler;

        public JdbcCollection(JdbcCalciteTable table, JdbcHandler jdbcHandler) {
            super(table.getTableName());
            this.table = table;
            this.jdbcHandler = jdbcHandler;
        }

        @Override
        public SqlRecTable getSqlRecTable() {
            return table;
        }

        @Override
        protected boolean addImpl(Object[] objects) {
            return jdbcHandler.upsert(objects);
        }

        @Override
        protected boolean removeImpl(Object[] objects) {
            return jdbcHandler.delete(objects);
        }
    }
}
