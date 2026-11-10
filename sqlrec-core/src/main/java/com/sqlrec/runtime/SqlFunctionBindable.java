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

    private void execInParallel(CalciteSchema schema, ExecuteContext context) {
        Map<Integer, CompletableFuture<Object>> bindFutures = new HashMap<>();
        for (int bindableIndex : sortedBindableList) {
            BindableInterface bindable = bindableList.get(bindableIndex);
            Set<Integer> dependencyIndices = bindableDependency.get(bindableIndex);
            CompletableFuture<Object> bindFuture = schedule(
                    bindable, dependencyIndices, bindFutures, schema, context
            );
            bindFuture.whenComplete((result, ex) -> {
                if (ex != null) {
                    context.cancel();
                }
            });
            bindFutures.put(bindableIndex, bindFuture);
        }

        ExecutorServiceUtils.awaitAll(context, bindFutures.values());
    }

    private CompletableFuture<Object> schedule(
            BindableInterface bindable,
            Set<Integer> dependencyIndices,
            Map<Integer, CompletableFuture<Object>> bindFutures,
            CalciteSchema schema,
            ExecuteContext context
    ) {
        List<CompletableFuture<Object>> dependencies = dependencyIndices == null
                ? List.of()
                : dependencyIndices.stream().map(bindFutures::get).toList();
        return ExecutorServiceUtils.submitAfter(dependencies, () -> bindUnlessReturned(bindable, schema, context));
    }

    private Enumerable<Object[]> bindUnlessReturned(
            BindableInterface bindable,
            CalciteSchema schema,
            ExecuteContext context
    ) {
        ExecuteContextImpl functionContext = (ExecuteContextImpl) context;
        if (functionContext.hasReturnedFromFunction()) {
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
            Set<String> bindableTables = new HashSet<>();
            for (String tableName : tableFunction.apply(bindable)) {
                bindableTables.add(NodeUtils.normalizeTableName(tableName));
            }
            bindableTables.removeAll(cacheTableNames);
            resultTables.addAll(bindableTables);
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
        Set<String> dependencySqlFunctions = new HashSet<>();
        for (BindableInterface bindable : bindableList) {
            for (String dependencySqlFuncName : bindable.getDependencySqlFuncName()) {
                if (StringUtils.isNotEmpty(dependencySqlFuncName)) {
                    dependencySqlFunctions.add(dependencySqlFuncName);
                }
            }
        }
        return dependencySqlFunctions;
    }

    public Set<String> getDependencyJavaFunctions() {
        Set<String> dependencyJavaFunctions = new HashSet<>();
        for (BindableInterface bindable : bindableList) {
            for (String dependencyJavaFuncName : bindable.getDependencyJavaFuncName()) {
                if (StringUtils.isNotEmpty(dependencyJavaFuncName)) {
                    dependencyJavaFunctions.add(dependencyJavaFuncName);
                }
            }
        }
        return dependencyJavaFunctions;
    }

    public Map<String, String> getAllDependSqlFunctionMap() {
        return allDependSqlFunctionMap;
    }

    private void initAllDependSqlFunctionMap() throws Exception {
        allDependSqlFunctionMap = new HashMap<>();

        for (BindableInterface bindable : bindableList) {
            for (String directDependencySqlFuncName : bindable.getDependencySqlFuncName()) {
                if (StringUtils.isEmpty(directDependencySqlFuncName)) {
                    continue;
                }
                allDependSqlFunctionMap.put(directDependencySqlFuncName, directDependencySqlFuncName);
                Map<String, String> aSqlFunctionAllDependSqlFunctionMap = bindable.getAllDependSqlFunctionMap();
                for (Map.Entry<String, String> entry : aSqlFunctionAllDependSqlFunctionMap.entrySet()) {
                    if (!allDependSqlFunctionMap.containsKey(entry.getKey())) {
                        allDependSqlFunctionMap.put(entry.getKey(), directDependencySqlFuncName + "->" + entry.getValue());
                    }
                }
            }
        }

        if (allDependSqlFunctionMap.containsKey(funName)) {
            throw new Exception("Sql function " + funName + " has circular dependency: " +
                    allDependSqlFunctionMap.get(funName));
        }
    }

    private void initExceptionIgnore() {
        Map<Integer, Boolean> isUnionSource = TopologicalSortUtils.getIsUnionSource(
                bindableList,
                sortedBindableList
        );
        for (int i = 0; i < bindableList.size(); i++) {
            BindableInterface bindable = bindableList.get(i);
            bindable.setIgnoreException(isUnionSource.getOrDefault(i, false));
        }
    }
}
