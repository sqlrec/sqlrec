package com.sqlrec.runtime;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.utils.ExecutorServiceUtils;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.common.utils.MetricsUtils;
import io.micrometer.core.instrument.Tags;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

public class IfBindable extends BindableInterface {
    private static final Logger log = LoggerFactory.getLogger(IfBindable.class);

    private final CalciteBindable condition;
    private final BindableInterface thenClause;
    private final BindableInterface elseClause;
    private final boolean timein;
    private final List<RelDataTypeField> returnDataFields;
    private final List<RelDataTypeField> cacheTableDataFields;

    public IfBindable(
            CalciteBindable condition,
            BindableInterface thenClause,
            BindableInterface elseClause,
            boolean timein
    ) {
        this(condition, thenClause, elseClause, timein, null);
    }

    public IfBindable(
            CalciteBindable condition,
            BindableInterface thenClause,
            BindableInterface elseClause,
            boolean timein,
            List<RelDataTypeField> existingCacheFields
    ) {
        if (thenClause instanceof IfBindable || elseClause instanceof IfBindable) {
            throw new RuntimeException("if statement cannot be nested in then or else clause");
        }
        this.condition = condition;
        this.thenClause = thenClause;
        this.elseClause = elseClause;
        this.timein = timein;

        if (timein && elseClause == null) {
            throw new RuntimeException("must contain else clause when in timein mode");
        }
        boolean thenReturns = thenClause.containsReturn();
        boolean thenIsCache = thenClause instanceof CacheTableBindable;
        checkBranches(thenReturns, thenIsCache);
        returnDataFields = mergeReturnFields();
        if (timein && !thenIsCache && !thenReturns) {
            throw new RuntimeException("timein mode only supports cache statement in then clause");
        }
        cacheTableDataFields = thenIsCache ? mergeCacheFields(existingCacheFields) : null;
    }

    private void checkBranches(boolean thenReturns, boolean thenIsCache) {
        if (elseClause == null) {
            return;
        }
        if (thenReturns != elseClause.containsReturn()) {
            throw new RuntimeException(
                    "IF with RETURN must either omit ELSE or return from both THEN and ELSE branches"
            );
        }
        if (thenReturns) {
            return;
        }
        boolean elseIsCache = elseClause instanceof CacheTableBindable;
        if (thenIsCache != elseIsCache) {
            throw new RuntimeException(
                    "thenClause and elseClause must be both cache statements or both non-cache statements");
        }
        if (thenIsCache && !thenClause.getCacheTableName().equals(elseClause.getCacheTableName())) {
            throw new RuntimeException("thenClause and elseClause must have the same table name");
        }
    }

    private List<RelDataTypeField> mergeReturnFields() {
        List<RelDataTypeField> thenFields = thenClause.getReturnDataFields();
        if (elseClause == null) {
            return thenFields;
        }
        List<RelDataTypeField> elseFields = elseClause.getReturnDataFields();
        boolean thenHasFields = thenFields != null && !thenFields.isEmpty();
        boolean elseHasFields = elseFields != null && !elseFields.isEmpty();
        if (thenHasFields != elseHasFields) {
            throw new RuntimeException("thenClause and elseClause must return compatible data fields");
        }
        return thenHasFields ? DataTypeUtils.mergeTableSchemas(thenFields, elseFields) : thenFields;
    }

    private List<RelDataTypeField> mergeCacheFields(List<RelDataTypeField> existingCacheFields) {
        List<RelDataTypeField> thenFields = thenClause.getCacheTableDataFields();
        if (elseClause == null && existingCacheFields == null) {
            return thenFields;
        }
        List<RelDataTypeField> otherFields = elseClause == null ? existingCacheFields : elseClause.getCacheTableDataFields();
        return DataTypeUtils.mergeTableSchemas(thenFields, otherFields);
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
        List<Object[]> conditionList = condition.bind(schema, context).toList();

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

        Tags tags = MetricsUtils.createTags(context.getVariables(), "name", getName(), "branch", conditionValue ? "then" : "else");
        MetricsUtils.getCompositeMeterRegistry()
                .counter(Consts.METRICS_IF_CACHE_BRANCH, tags)
                .increment();

        if (selectedClause != null) {
            return selectedClause.bind(schema, context);
        }

        // no else clause and condition is false: keep the cache table visible as an empty table
        if (thenClause instanceof CacheTableBindable thenCache) {
            thenCache.ensureTable(schema, context);
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

        if (context.isCancelled()) {
            throw new RuntimeException("if node " + getName() + " cancelled");
        }
        long timeout = ((Number) value).longValue();
        if (timeout <= 0) {
            recordTimeinOutcome(context, "direct");
            return elseClause.bind(schema, context);
        }
        return executeThen(schema, context, timeout);
    }

    private Enumerable<Object[]> executeThen(CalciteSchema schema, ExecuteContext context, long timeout) {
        ExecuteContextImpl functionContext = (ExecuteContextImpl) context;
        boolean containsReturn = containsReturn();
        ExecuteContextImpl thenContext = containsReturn
                ? functionContext.createFunctionContext()
                : functionContext.clone();
        Enumerable<Object[]> result;
        try {
            result = ExecutorServiceUtils.execute(() -> thenClause.bind(schema, thenContext), thenContext, timeout);
        } catch (Throwable failure) {
            thenContext.cancel();
            boolean waitingTimedOut = failure instanceof TimeoutException;
            Throwable cause = ExecutorServiceUtils.unwrap(failure);
            if (context.isCancelled()) {
                if (waitingTimedOut) {
                    throw new RuntimeException("if node " + getName() + " cancelled while waiting for thenClause");
                }
                throw new RuntimeException("if node " + getName() + " cancelled", cause);
            }
            if (waitingTimedOut) {
                log.warn("thenClause execution timeout after {}ms, falling back to elseClause", timeout);
                recordTimeinOutcome(context, "timeout");
            } else {
                log.error("Error executing thenClause, falling back to elseClause", cause);
                recordTimeinOutcome(context, "error");
            }
            // ELSE starts after the failed THEN's interruption has been handled.
            Thread.interrupted();
            return elseClause.bind(schema, context);
        }

        recordTimeinOutcome(context, "primary_success");
        if (containsReturn) {
            functionContext.commitFunctionReturnFrom(thenContext);
        }
        return result;
    }

    private void recordTimeinOutcome(ExecuteContext context, String reason) {
        Tags tags = MetricsUtils.createTags(context.getVariables(), "name", getName(), "reason", reason);
        MetricsUtils.getCompositeMeterRegistry().counter(Consts.METRICS_IF_TIMEIN_OUTCOME, tags).increment();
    }

    @Override
    public List<RelDataTypeField> getReturnDataFields() {
        return returnDataFields;
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
        Set<String> readTables = collectBranchNames(BindableInterface::getReadTables);
        readTables.addAll(condition.getReadTables());
        return readTables;
    }

    @Override
    public Set<String> getWriteTables() {
        return collectBranchNames(BindableInterface::getWriteTables);
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

    public String getCacheTableName() {
        if (thenClause instanceof CacheTableBindable) {
            return thenClause.getCacheTableName();
        }
        return super.getCacheTableName();
    }

    public List<RelDataTypeField> getCacheTableDataFields() {
        return cacheTableDataFields;
    }

    @Override
    public Set<String> getDependencySqlFuncName() {
        return collectBranchNames(BindableInterface::getDependencySqlFuncName);
    }

    private Set<String> collectBranchNames(Function<BindableInterface, Set<String>> namesForBranch) {
        Set<String> names = new HashSet<>(namesForBranch.apply(thenClause));
        if (elseClause != null) {
            names.addAll(namesForBranch.apply(elseClause));
        }
        return names;
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
