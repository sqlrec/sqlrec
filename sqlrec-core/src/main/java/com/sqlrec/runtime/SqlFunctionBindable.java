package com.sqlrec.runtime;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.utils.ExecutorServiceUtils;
import com.sqlrec.utils.NodeUtils;
import com.sqlrec.utils.TopologicalSortUtils;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/** Built during compilation; init seals the function structure before it is shared. */
public class SqlFunctionBindable extends BindableInterface {
    private String funName;
    private final List<Map.Entry<String, List<RelDataTypeField>>> inputTables = new ArrayList<>();
    private final List<BindableInterface> bindableList;
    private List<RelDataTypeField> returnDataFields;
    private boolean initialized;

    private Set<String> readTables;
    private Set<String> writeTables;
    private Map<Integer, Set<Integer>> bindableDependency;
    private List<Integer> sortedBindableList;

    private Map<String, String> allDependSqlFunctionMap; // funName to call trace

    public SqlFunctionBindable(
            List<Map.Entry<String, List<RelDataTypeField>>> inputTables,
            List<BindableInterface> bindableList,
            List<RelDataTypeField> returnDataFields
    ) {
        inputTables.forEach(input -> addInputTable(input.getKey(), input.getValue()));
        this.bindableList = new ArrayList<>(bindableList);
        setReturnDataFields(returnDataFields);
    }

    public void init() throws Exception {
        ensureCompiling();
        this.readTables = Set.copyOf(getAccessTables(bindableList, BindableInterface::getReadTables));
        this.writeTables = Set.copyOf(getAccessTables(bindableList, BindableInterface::getWriteTables));
        TopologicalSortUtils.DependencyPlan plan = TopologicalSortUtils.topologicalSort(bindableList);
        this.bindableDependency = plan.dependencies();
        this.sortedBindableList = plan.executionOrder();

        initAllDependSqlFunctionMap();
        allDependSqlFunctionMap = Map.copyOf(allDependSqlFunctionMap);

        // Mark structural UNION sources at compile time. Whether recovery is enabled is
        // evaluated from the execution context, so request-level SET values take effect.
        initExceptionIgnore();
        initialized = true;
    }

    @Override
    public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
        ExecuteContextImpl functionContext = ((ExecuteContextImpl) context).createFunctionContext();
        try {
            if (SqlRecConfigs.PARALLELISM_EXEC.getValue()) {
                execInParallel(schema, functionContext);
            } else {
                for (BindableInterface bindable : bindableList) {
                    if (functionContext.hasReturnedFromFunction()) {
                        break;
                    }
                    bindable.bind(schema, functionContext);
                }
            }
        } catch (Throwable e) {
            // stop nested async tasks still running inside this function (timeout branches within nodes, partitions, async calls, etc.)
            functionContext.cancel();
            throw e;
        }

        if (functionContext.hasReturnedFromFunction()) {
            return functionContext.getFunctionReturnResult();
        }
        throw new IllegalStateException("SQL function completed without executing a RETURN statement");
    }

    private void execInParallel(CalciteSchema schema, ExecuteContextImpl context) {
        Map<Integer, CompletableFuture<Enumerable<Object[]>>> bindFutures = new HashMap<>();
        for (int bindableIndex : sortedBindableList) {
            BindableInterface bindable = bindableList.get(bindableIndex);
            var dependencies = bindableDependency.get(bindableIndex).stream().map(bindFutures::get).toList();
            var bindFuture = ExecutorServiceUtils.submitAfter(
                    dependencies, () -> bindUnlessReturned(bindable, schema, context));
            bindFuture.whenComplete((result, ex) -> {
                if (ex != null) {
                    context.cancel();
                }
            });
            bindFutures.put(bindableIndex, bindFuture);
        }

        ExecutorServiceUtils.awaitAll(context, bindFutures.values());
    }

    private Enumerable<Object[]> bindUnlessReturned(
            BindableInterface bindable,
            CalciteSchema schema,
            ExecuteContextImpl context
    ) {
        if (context.hasReturnedFromFunction()) {
            return null;
        }
        return bindable.bind(schema, context);
    }

    @Override
    public List<RelDataTypeField> getReturnDataFields() {
        return returnDataFields;
    }

    @Override
    public boolean isParallelizable() {
        for (BindableInterface bindable : bindableList) {
            if (!bindable.isParallelizable()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isTimeoutAble(CalciteSchema schema, ExecuteContext context) {
        return false;
    }

    @Override
    public Set<String> getReadTables() {
        return readTables;
    }

    @Override
    public Set<String> getWriteTables() {
        return writeTables;
    }

    private Set<String> getAccessTables(
            List<BindableInterface> bindableList,
            Function<BindableInterface, Set<String>> tableFunction
    ) {
        Set<String> resultTables = new HashSet<>();
        Set<String> cacheTableNames = new HashSet<>();
        for (BindableInterface bindable : bindableList) {
            if (StringUtils.isNotEmpty(bindable.getCacheTableName())) {
                cacheTableNames.add(NodeUtils.normalizeTableName(bindable.getCacheTableName()));
            }
            for (String tableName : tableFunction.apply(bindable)) {
                String normalizedName = NodeUtils.normalizeTableName(tableName);
                if (!cacheTableNames.contains(normalizedName)) {
                    resultTables.add(normalizedName);
                }
            }
        }
        return resultTables;
    }

    public List<Map.Entry<String, List<RelDataTypeField>>> getInputTables() {
        return Collections.unmodifiableList(inputTables);
    }

    public void addInputTable(String tableName, List<RelDataTypeField> dataFields) {
        ensureCompiling();
        inputTables.add(Map.entry(tableName, List.copyOf(dataFields)));
    }

    public void addBindable(BindableInterface bindable) {
        ensureCompiling();
        bindableList.add(Objects.requireNonNull(bindable, "bindable"));
    }

    public List<BindableInterface> getBindableList() {
        return Collections.unmodifiableList(bindableList);
    }

    public Map<Integer, Set<Integer>> getBindableDependency() {
        return bindableDependency;
    }

    public void setReturnDataFields(List<RelDataTypeField> returnDataFields) {
        ensureCompiling();
        this.returnDataFields = returnDataFields == null ? null : List.copyOf(returnDataFields);
    }

    public String getFunName() {
        return funName;
    }

    public void setFunName(String funName) {
        ensureCompiling();
        this.funName = funName;
    }

    private void ensureCompiling() {
        if (initialized) {
            throw new IllegalStateException("SQL function structure is already initialized: " + funName);
        }
    }

    public Set<String> getDependencySqlFunctions() {
        Set<String> dependencies = new HashSet<>();
        for (BindableInterface bindable : bindableList) {
            for (String name : bindable.getDependencySqlFuncName()) {
                if (StringUtils.isNotEmpty(name)) {
                    dependencies.add(name);
                }
            }
        }
        return dependencies;
    }

    public Map<String, String> getAllDependSqlFunctionMap() {
        return allDependSqlFunctionMap;
    }

    private void initAllDependSqlFunctionMap() throws Exception {
        allDependSqlFunctionMap = new HashMap<>();

        for (BindableInterface bindable : bindableList) {
            for (String directDependency : bindable.getDependencySqlFuncName()) {
                if (StringUtils.isEmpty(directDependency)) {
                    continue;
                }
                allDependSqlFunctionMap.put(directDependency, directDependency);
                Map<String, String> transitiveDependencies = bindable.getAllDependSqlFunctionMap();
                for (Map.Entry<String, String> entry : transitiveDependencies.entrySet()) {
                    allDependSqlFunctionMap.putIfAbsent(entry.getKey(), directDependency + "->" + entry.getValue());
                }
            }
        }

        if (allDependSqlFunctionMap.containsKey(funName)) {
            throw new Exception("Sql function " + funName + " has circular dependency: " +
                    allDependSqlFunctionMap.get(funName));
        }
    }

    private void initExceptionIgnore() {
        Set<Integer> unionSources = TopologicalSortUtils.findRecoverableUnionSources(bindableList);
        for (int i = 0; i < bindableList.size(); i++) {
            bindableList.get(i).setIgnoreException(unionSources.contains(i));
        }
    }
}
