package com.sqlrec.runtime;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.utils.Executor;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class CacheTableBindable extends BindableInterface {
    private static final Logger log = LoggerFactory.getLogger(CacheTableBindable.class);

    private String tableName;
    private BindableInterface bindable;
    private String createSql;

    public CacheTableBindable(String tableName, BindableInterface bindable, String createSql) {
        this.tableName = tableName;
        this.bindable = bindable;
        this.createSql = createSql;

        List<RelDataTypeField> bindableFields = bindable.getReturnDataFields();
        if (bindableFields == null || bindableFields.isEmpty()) {
            throw new RuntimeException("bindable return data fields is null or empty");
        }
    }

    @Override
    public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
        Enumerable<Object[]> enumerable = null;
        long timeout = SqlRecConfigs.NODE_EXEC_TIMEOUT.getValue(context.getVariables());
        boolean needTimeout = timeout > 0 && isTimeoutAble(schema, context);

        try {
            if (needTimeout) {
                enumerable = executeWithTimeout(schema, context, timeout);
            } else {
                enumerable = bindable.bind(schema, context);
            }
        } catch (Exception e) {
            if (isIgnoreException()) {
                log.warn("ignore exception when bind cache table {}: {}", tableName, e.getMessage(), e);
            } else {
                throw e;
            }
        }
        if (enumerable == null) {
            enumerable = Linq4j.emptyEnumerable();
        }

        CacheTable cacheTable = new CacheTable(tableName, enumerable, bindable.getReturnDataFields());
        cacheTable.setCreateSql(createSql);
        schema.add(tableName, cacheTable);

        // return cache table counts
        List<Object[]> list = new ArrayList<>();
        list.add(new Object[]{tableName, (long) enumerable.count()});
        return Linq4j.asEnumerable(list);
    }

    private Enumerable<Object[]> executeWithTimeout(CalciteSchema schema, ExecuteContext context, long timeout) {
        CompletableFuture<Enumerable<Object[]>> future = CompletableFuture.supplyAsync(
                () -> bindable.bind(schema, context), Executor.getExecutorService()
        );
        try {
            return future.get(timeout, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw new RuntimeException("Task execution timeout after " + timeout + "ms", e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<RelDataTypeField> getReturnDataFields() {
        return Arrays.asList(
                DataTypeUtils.getRelDataTypeField("table_name", 0, SqlTypeName.VARCHAR),
                DataTypeUtils.getRelDataTypeField("count", 1, SqlTypeName.BIGINT)
        );
    }

    @Override
    public boolean isParallelizable() {
        return bindable.isParallelizable();
    }

    @Override
    public boolean isTimeoutAble(CalciteSchema schema, ExecuteContext context) {
        return bindable.isTimeoutAble(schema, context);
    }

    @Override
    public Set<String> getReadTables() {
        return bindable.getReadTables();
    }

    @Override
    public Set<String> getWriteTables() {
        Set<String> writeTables = new HashSet<>(bindable.getWriteTables());
        writeTables.add(tableName);
        return writeTables;
    }

    public List<RelDataTypeField> getTableDataFields() {
        return bindable.getReturnDataFields();
    }

    public String getTableName() {
        return tableName;
    }

    public BindableInterface getBindable() {
        return bindable;
    }

    public boolean isUnionSql() {
        return bindable.isUnionSql();
    }

    public String getCacheTableName() {
        return tableName;
    }

    public List<RelDataTypeField> getCacheTableDataFields() {
        return bindable.getReturnDataFields();
    }
}
