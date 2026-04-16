package com.sqlrec.runtime;

import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.utils.DataTypeUtils;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class IfCacheBindable extends BindableInterface {
    private static final Logger log = LoggerFactory.getLogger(IfCacheBindable.class);

    private CalciteBindable condition;
    private CacheTableBindable thenClause;
    private CacheTableBindable elseClause;
    private boolean timein;
    private String tableName;

    public IfCacheBindable(
            CalciteBindable condition,
            CacheTableBindable thenClause,
            CacheTableBindable elseClause,
            boolean timein
    ) {
        this.condition = condition;
        this.thenClause = thenClause;
        this.elseClause = elseClause;
        this.timein = timein;
        this.tableName = thenClause.getTableName();

        if (elseClause != null) {
            if (!thenClause.getTableName().equals(elseClause.getTableName())) {
                throw new RuntimeException("thenClause and elseClause must have the same table name");
            }
            DataTypeUtils.checkTableSchemaCompatible(
                    thenClause.getTableDataFields(),
                    elseClause.getTableDataFields()
            );
        } else {
            if (timein) {
                throw new RuntimeException("must contain else clause when in timein mode");
            }
        }
    }

    @Override
    public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
        if (timein) {
            return bindWithTimein(schema, context);
        } else {
            return bindWithCondition(schema, context);
        }
    }

    private Enumerable<Object[]> bindWithCondition(CalciteSchema schema, ExecuteContext context) {
        Enumerable<Object[]> conditionResult = condition.bind(schema, context);
        List<Object[]> conditionList = new ArrayList<>();
        for (Object[] row : conditionResult) {
            conditionList.add(row);
        }

        if (conditionList.size() != 1) {
            throw new RuntimeException("condition must return exactly one row");
        }

        Object[] row = conditionList.get(0);
        if (row.length != 1) {
            throw new RuntimeException("condition must return exactly one column");
        }

        Object value = row[0];
        if (!(value instanceof Boolean)) {
            throw new RuntimeException("condition must return a boolean value");
        }

        boolean conditionValue = (Boolean) value;
        CacheTableBindable selectedClause = conditionValue ? thenClause : elseClause;

        if (selectedClause != null) {
            return selectedClause.bind(schema, context);
        }
        return null;
    }

    private Enumerable<Object[]> bindWithTimein(CalciteSchema schema, ExecuteContext context) {
        if (elseClause == null) {
            throw new RuntimeException("elseClause must exist when timein is set");
        }

        Enumerable<Object[]> conditionResult = condition.bind(schema, context);
        List<Object[]> conditionList = new ArrayList<>();
        for (Object[] row : conditionResult) {
            conditionList.add(row);
        }

        if (conditionList.size() != 1) {
            throw new RuntimeException("condition must return exactly one row");
        }

        Object[] row = conditionList.get(0);
        if (row.length != 1) {
            throw new RuntimeException("condition must return exactly one column");
        }

        Object value = row[0];
        if (!(value instanceof Number)) {
            throw new RuntimeException("condition must return a numeric value for timein mode");
        }

        long timeout = ((Number) value).longValue();
        if (timeout <= 0) {
            return thenClause.bind(schema, context);
        }

        return executeWithTimeout(schema, context, timeout);
    }

    private Enumerable<Object[]> executeWithTimeout(CalciteSchema schema, ExecuteContext context, long timeout) {
        CompletableFuture<Enumerable<Object[]>> future = CompletableFuture.supplyAsync(
                () -> thenClause.bind(schema, context),
                java.util.concurrent.Executors.newSingleThreadExecutor()
        );

        try {
            return future.get(timeout, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            log.warn("thenClause execution timeout after {}ms, falling back to elseClause", timeout);
            future.cancel(true);
            return elseClause.bind(schema, context);
        } catch (Exception e) {
            log.error("Error executing thenClause, falling back to elseClause", e);
            future.cancel(true);
            return elseClause.bind(schema, context);
        }
    }

    @Override
    public List<RelDataTypeField> getReturnDataFields() {
        return thenClause.getReturnDataFields();
    }

    @Override
    public boolean isParallelizable() {
        return condition.isParallelizable() && thenClause.isParallelizable() &&
                (elseClause == null || elseClause.isParallelizable());
    }

    @Override
    public Set<String> getReadTables() {
        Set<String> readTables = new HashSet<>(condition.getReadTables());
        readTables.addAll(thenClause.getReadTables());
        if (elseClause != null) {
            readTables.addAll(elseClause.getReadTables());
        }
        return readTables;
    }

    @Override
    public Set<String> getWriteTables() {
        Set<String> writeTables = new HashSet<>();
        writeTables.add(tableName);
        return writeTables;
    }

    public String getTableName() {
        return tableName;
    }

    public CalciteBindable getCondition() {
        return condition;
    }

    public CacheTableBindable getThenClause() {
        return thenClause;
    }

    public CacheTableBindable getElseClause() {
        return elseClause;
    }

    public boolean isTimein() {
        return timein;
    }
}
