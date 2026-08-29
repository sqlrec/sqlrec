package com.sqlrec.runtime;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.common.utils.MetricsUtils;
import com.sqlrec.utils.ExecutorServiceUtils;
import com.sqlrec.utils.SchemaUtils;
import io.micrometer.core.instrument.Tags;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class IfBindable extends BindableInterface {
    private static final Logger log = LoggerFactory.getLogger(IfBindable.class);

    private CalciteBindable condition;
    private BindableInterface thenClause;
    private BindableInterface elseClause;
    private boolean timein;

    public IfBindable(
            CalciteBindable condition,
            BindableInterface thenClause,
            BindableInterface elseClause,
            boolean timein
    ) {
        if (thenClause instanceof IfBindable || elseClause instanceof IfBindable) {
            throw new RuntimeException("if statement cannot be nested in then or else clause");
        }
        this.condition = condition;
        this.thenClause = thenClause;
        this.elseClause = elseClause;
        this.timein = timein;

        boolean thenReturns = thenClause.containsReturn();
        boolean elseReturns = elseClause != null && elseClause.containsReturn();
        boolean containsReturn = thenReturns || elseReturns;
        if (containsReturn && (!thenReturns || (elseClause != null && !elseReturns))) {
            throw new RuntimeException(
                    "IF with RETURN must either omit ELSE or return from both THEN and ELSE branches"
            );
        }

        boolean thenIsCache = thenClause instanceof CacheTableBindable;
        if (elseClause != null) {
            if (containsReturn) {
                checkReturnFieldsCompatible();
            } else {
                boolean elseIsCache = elseClause instanceof CacheTableBindable;
                if (thenIsCache != elseIsCache) {
                    throw new RuntimeException(
                            "thenClause and elseClause must be both cache statements or both non-cache statements");
                }
                if (thenIsCache) {
                    if (!thenClause.getCacheTableName().equals(elseClause.getCacheTableName())) {
                        throw new RuntimeException("thenClause and elseClause must have the same table name");
                    }
                    DataTypeUtils.checkTableSchemaSame(
                            ((CacheTableBindable) thenClause).getTableDataFields(),
                            ((CacheTableBindable) elseClause).getTableDataFields()
                    );
                } else {
                    checkReturnFieldsCompatible();
                }
            }
        } else if (elseClause == null && timein) {
            throw new RuntimeException("must contain else clause when in timein mode");
        }

        if (timein && !thenIsCache && !thenReturns) {
            throw new RuntimeException("timein mode only supports cache statement in then clause");
        }
    }

    private void checkReturnFieldsCompatible() {
        List<RelDataTypeField> thenFields = thenClause.getReturnDataFields();
        List<RelDataTypeField> elseFields = elseClause.getReturnDataFields();
        boolean thenHasFields = thenFields != null && !thenFields.isEmpty();
        boolean elseHasFields = elseFields != null && !elseFields.isEmpty();
        if (thenHasFields != elseHasFields) {
            throw new RuntimeException("thenClause and elseClause must return compatible data fields");
        }
        if (thenHasFields) {
            DataTypeUtils.checkTableSchemaSame(thenFields, elseFields);
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

    private Object getSingleConditionValue(CalciteSchema schema, ExecuteContext context) {
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
        return row[0];
    }

    private Enumerable<Object[]> bindWithCondition(CalciteSchema schema, ExecuteContext context) {
        Object value = getSingleConditionValue(schema, context);

        boolean conditionValue = false;
        if (value != null) {
            if (!(value instanceof Boolean)) {
                throw new RuntimeException("condition must return a boolean value");
            }
            conditionValue = (Boolean) value;
        }

        BindableInterface selectedClause = conditionValue ? thenClause : elseClause;

        Tags tags = MetricsUtils.createTags(context.getMetricsTags(), "name", getName(), "branch", conditionValue ? "then" : "else");
        MetricsUtils.getCompositeMeterRegistry()
                .counter(Consts.METRICS_IF_CACHE_BRANCH, tags)
                .increment();

        if (selectedClause != null) {
            return selectedClause.bind(schema, context);
        }

        // no else clause and condition is false: keep the cache table visible as an empty table
        if (thenClause instanceof CacheTableBindable) {
            CacheTableBindable thenCache = (CacheTableBindable) thenClause;
            CacheTable table = SchemaUtils.tryGetCacheTable(thenCache.getTableName(), schema);
            if (table == null) {
                // add empty table
                CacheTable cacheTable = new CacheTable(thenCache.getTableName(), Linq4j.emptyEnumerable(), thenCache.getTableDataFields());
                schema.add(thenCache.getTableName(), cacheTable);
            } else {
                // the existing table must keep the exact same schema as the then clause,
                // otherwise statements compiled against the then schema would read mismatched data
                DataTypeUtils.checkTableSchemaSame(thenCache.getTableDataFields(), table.getDataFields());
            }
        }
        return Linq4j.emptyEnumerable();
    }

    private Enumerable<Object[]> bindWithTimein(CalciteSchema schema, ExecuteContext context) {
        if (elseClause == null) {
            throw new RuntimeException("elseClause must exist when timein is set");
        }

        Object value = getSingleConditionValue(schema, context);
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
        ExecuteContextImpl functionContext = (ExecuteContextImpl) context;
        boolean containsReturn = containsReturn();
        ExecuteContextImpl thenContext = containsReturn
                ? functionContext.createIsolatedReturnContext()
                : functionContext.clone();
        CompletableFuture<Enumerable<Object[]>> future = CompletableFuture.supplyAsync(
                () -> thenClause.bind(schema, thenContext),
                ExecutorServiceUtils.getExecutorService()
        );

        Enumerable<Object[]> result;
        try {
            result = future.get(timeout, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            thenContext.cancel();
            future.cancel(true);
            if (context.isCancelled()) {
                // an ancestor has cancelled the whole subtree (including the else scope), do not fall back
                throw new RuntimeException("if node " + getName() + " cancelled while waiting for thenClause");
            }
            log.warn("thenClause execution timeout after {}ms, falling back to elseClause", timeout);
            incrementFallbackMetric(context, Consts.METRICS_IF_CACHE_TIMEOUT);
            return elseClause.bind(schema, context);
        } catch (Exception e) {
            thenContext.cancel();
            future.cancel(true);
            if (context.isCancelled()) {
                // an ancestor has cancelled the whole subtree, do not fall back, just abort
                throw new RuntimeException("if node " + getName() + " cancelled", e);
            }
            log.error("Error executing thenClause, falling back to elseClause", e);
            incrementFallbackMetric(context, Consts.METRICS_IF_CACHE_EXCEPTION_FALLBACK);
            return elseClause.bind(schema, context);
        }

        if (containsReturn) {
            functionContext.commitFunctionReturnFrom(thenContext);
        }
        return result;
    }

    private void incrementFallbackMetric(ExecuteContext context, String metricName) {
        Tags tags = MetricsUtils.createTags(context.getMetricsTags(), "name", getName());
        MetricsUtils.getCompositeMeterRegistry()
                .counter(metricName, tags)
                .increment();
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
    public boolean containsReturn() {
        return thenClause.containsReturn();
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
        Set<String> writeTables = new HashSet<>(thenClause.getWriteTables());
        if (elseClause != null) {
            writeTables.addAll(elseClause.getWriteTables());
        }
        return writeTables;
    }

    public CalciteBindable getCondition() {
        return condition;
    }

    public BindableInterface getThenClause() {
        return thenClause;
    }

    public BindableInterface getElseClause() {
        return elseClause;
    }

    public boolean isTimein() {
        return timein;
    }

    public boolean isUnionSql() {
        if (elseClause == null) {
            return false;
        }
        return thenClause.isUnionSql() && elseClause.isUnionSql();
    }

    public String getCacheTableName() {
        if (thenClause instanceof CacheTableBindable) {
            return thenClause.getCacheTableName();
        }
        return super.getCacheTableName();
    }

    public List<RelDataTypeField> getCacheTableDataFields() {
        if (thenClause instanceof CacheTableBindable) {
            return thenClause.getCacheTableDataFields();
        }
        return super.getCacheTableDataFields();
    }

    @Override
    public Set<String> getDependencySqlFuncName() {
        Set<String> dependencySqlFuncNames = new HashSet<>(thenClause.getDependencySqlFuncName());
        if (elseClause != null) {
            dependencySqlFuncNames.addAll(elseClause.getDependencySqlFuncName());
        }
        return dependencySqlFuncNames;
    }

    @Override
    public Set<String> getDependencyJavaFuncName() {
        Set<String> dependencyJavaFuncNames = new HashSet<>(thenClause.getDependencyJavaFuncName());
        if (elseClause != null) {
            dependencyJavaFuncNames.addAll(elseClause.getDependencyJavaFuncName());
        }
        return dependencyJavaFuncNames;
    }

    @Override
    public Map<String, String> getAllDependSqlFunctionMap() {
        Map<String, String> dependSqlFunctionMap = new HashMap<>(thenClause.getAllDependSqlFunctionMap());
        if (elseClause != null) {
            dependSqlFunctionMap.putAll(elseClause.getAllDependSqlFunctionMap());
        }
        return dependSqlFunctionMap;
    }
}
