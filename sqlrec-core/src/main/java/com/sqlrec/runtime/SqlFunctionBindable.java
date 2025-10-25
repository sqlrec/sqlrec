package com.sqlrec.runtime;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.schema.ExecuteContext;
import com.sqlrec.utils.TopologicalSortUtils;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Table;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

public class SqlFunctionBindable extends BindableInterface {
    public static final ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();

    private String funName;
    private List<Map.Entry<String, List<RelDataTypeField>>> inputTables;
    private List<BindableInterface> bindableList;
    private String returnTableName;
    private List<RelDataTypeField> returnDataFields;
    private Set<String> readTables;
    private Set<String> writeTables;
    private Map<Integer, Set<Integer>> bindableDependency;
    private List<Integer> sortedBindableList;

    public SqlFunctionBindable(
            List<Map.Entry<String, List<RelDataTypeField>>> inputTables,
            List<BindableInterface> bindableList,
            String returnTableName,
            List<RelDataTypeField> returnDataFields
    ) {
        this.inputTables = inputTables;
        this.bindableList = bindableList;
        this.returnTableName = returnTableName;
        this.returnDataFields = returnDataFields;
    }

    public void init() {
        this.readTables = getAccessTables(bindableList, BindableInterface::getReadTables);
        this.writeTables = getAccessTables(bindableList, BindableInterface::getWriteTables);
        Map.Entry<List<Integer>, Map<Integer, Set<Integer>>> topologicalSortIndex = TopologicalSortUtils
                .topologicalSort(bindableList);
        this.bindableDependency = topologicalSortIndex.getValue();
        this.sortedBindableList = topologicalSortIndex.getKey();
    }

    @Override
    public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
        if (SqlRecConfigs.PARALLELISM_EXEC.getValue().equalsIgnoreCase("true")) {
            execInParallel(schema, context);
        } else {
            for (BindableInterface bindable : bindableList) {
                bindable.bind(schema, context);
            }
        }

        if (returnTableName == null) {
            return null;
        }

        CalciteSchema.TableEntry tableEntry = schema.getTable(returnTableName, false);
        if (tableEntry == null) {
            throw new RuntimeException("function return table not exist");
        }
        Table table = tableEntry.getTable();
        if (table instanceof CacheTable) {
            return ((CacheTable) table).scan(null);
        } else {
            throw new RuntimeException("function not return cache table");
        }
    }

    private void execInParallel(CalciteSchema schema, ExecuteContext context) {
        List<CompletableFuture<Object>> bindFutures = new ArrayList<>();
        for (int i : sortedBindableList) {
            BindableInterface bindable = bindableList.get(i);
            Set<Integer> dependentBindableIndices = bindableDependency.get(i);
            if (dependentBindableIndices == null || dependentBindableIndices.isEmpty()) {
                CompletableFuture<Object> bindFuture = CompletableFuture.supplyAsync(
                        () -> bindable.bind(schema, context), executorService
                );
                bindFutures.add(bindFuture);
            } else {
                List<CompletableFuture<Object>> dependentBindFutures = new ArrayList<>();
                for (int dependentBindableIndex : dependentBindableIndices) {
                    dependentBindFutures.add(bindFutures.get(dependentBindableIndex));
                }
                CompletableFuture<Void> dependentBindFuturesAll = CompletableFuture.allOf(
                        dependentBindFutures.toArray(new CompletableFuture[0])
                );
                CompletableFuture<Object> bindFuture = dependentBindFuturesAll.thenApplyAsync(
                        (v) -> bindable.bind(schema, context), executorService
                );
                bindFutures.add(bindFuture);
            }
        }
        CompletableFuture<Void> allBindFutures = CompletableFuture.allOf(bindFutures.toArray(new CompletableFuture[0]));
        allBindFutures.join();
    }

    @Override
    public List<RelDataTypeField> getReturnDataFields() {
        return returnDataFields;
    }

    @Override
    public boolean isParallelizable() {
        return true;
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
            if (bindable instanceof CacheTableBindable) {
                cacheTableNames.add(((CacheTableBindable) bindable).getTableName());
            }
            Set<String> bindableTables = new HashSet<>(tableFunction.apply(bindable));
            bindableTables.removeAll(cacheTableNames);
            resultTables.addAll(bindableTables);
        }
        return resultTables;
    }

    public List<Map.Entry<String, List<RelDataTypeField>>> getInputTables() {
        return inputTables;
    }

    public void addInputTable(String tableName, List<RelDataTypeField> dataFields) {
        inputTables.add(new AbstractMap.SimpleEntry<>(tableName, dataFields));
    }

    public void setInputTables(List<Map.Entry<String, List<RelDataTypeField>>> inputTables) {
        this.inputTables = inputTables;
    }

    public List<BindableInterface> getBindableList() {
        return bindableList;
    }

    public void setBindableList(List<BindableInterface> bindableList) {
        this.bindableList = bindableList;
    }

    public String getReturnTableName() {
        return returnTableName;
    }

    public void setReturnTableName(String returnTableName) {
        this.returnTableName = returnTableName;
    }

    public void setReturnDataFields(List<RelDataTypeField> returnDataFields) {
        this.returnDataFields = returnDataFields;
    }

    public String getFunName() {
        return funName;
    }

    public void setFunName(String funName) {
        this.funName = funName;
    }

    public Set<String> getDependencySqlFunctions() {
        Set<String> dependencySqlFunctions = new HashSet<>();
        for (BindableInterface bindable : bindableList) {
            if (bindable instanceof CallSqlFunctionBindable) {
                dependencySqlFunctions.add(((CallSqlFunctionBindable) bindable).getFunName());
            }
        }
        return dependencySqlFunctions;
    }

    public Set<String> getDependencyJavaFunctions() {
        Set<String> dependencyJavaFunctions = new HashSet<>();
        for (BindableInterface bindable : bindableList) {
            if (bindable instanceof JavaFunctionBindable) {
                dependencyJavaFunctions.add(((JavaFunctionBindable) bindable).getFunName());
            }
        }
        return dependencyJavaFunctions;
    }
}
