package com.sqlrec.common.schema;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Table;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class CacheProxyTable extends CacheTable {
    private static final Logger logger = LoggerFactory.getLogger(CacheProxyTable.class);

    public CacheProxyTable(String tableName, Enumerable<Object[]> enumerable, List<RelDataTypeField> dataFields) {
        super(tableName, enumerable, dataFields);
    }

    @Override
    public Enumerable<@Nullable Object[]> scan(DataContext root) {
        if (root == null || root.getRootSchema() == null) {
            // may called when java function compile
            logger.warn("root or root schema is null for table {}", getTableName());
            return Linq4j.emptyEnumerable();
        }
        Table cacheTable = root.getRootSchema().getTable(getTableName());
        if (!(cacheTable instanceof CacheTable)) {
            logger.error("cache table {} is not CacheTable", getTableName());
            throw new RuntimeException("cache table " + getTableName() + " is not CacheTable");
        }
        return ((CacheTable) cacheTable).scan(root);
    }
}
