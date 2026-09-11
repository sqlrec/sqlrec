package com.sqlrec.runtime;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.schema.CalciteSchemaFactory;
import com.sqlrec.schema.JavaFunctionUtils;
import com.sqlrec.sql.parser.SqlCallSqlFunction;
import com.sqlrec.sql.parser.SqlGetVariable;
import com.sqlrec.utils.ExecutorServiceUtils;
import com.sqlrec.utils.NodeUtils;
import com.sqlrec.utils.SchemaUtils;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

public class FunctionProxyBindable extends BindableInterface {
    private static final Logger log = LoggerFactory.getLogger(FunctionProxyBindable.class);

    private List<SqlNode> inputList;
    private SqlGetVariable funcNameVariable;
    private BindableInterface delegate;
    private List<RelDataTypeField> returnDataFields;
    private boolean isAsync;
    private String partitionBy;
    private SqlNode partitionSizeNode;

    public FunctionProxyBindable(
            SqlCallSqlFunction callSqlFunction,
            List<SqlNode> inputList,
            SqlGetVariable funcNameVariable,
            List<RelDataTypeField> returnDataFields,
            boolean isAsync
    ) {
        if (returnDataFields == null) {
            throw new RuntimeException("return data fields is null");
        }

        this.inputList = inputList;
        this.delegate = null;
        this.funcNameVariable = funcNameVariable;
        this.returnDataFields = returnDataFields;
        this.isAsync = isAsync;
        extractPartitionInfo(callSqlFunction, inputList);
    }

    public FunctionProxyBindable(
            SqlCallSqlFunction callSqlFunction,
            List<SqlNode> inputList,
            BindableInterface delegate,
            boolean isAsync
    ) {
        this.inputList = inputList;
        this.delegate = delegate;
        this.funcNameVariable = null;
        this.returnDataFields = delegate.getReturnDataFields();
        this.isAsync = isAsync;
        extractPartitionInfo(callSqlFunction, inputList);
    }

    private void extractPartitionInfo(SqlCallSqlFunction callSqlFunction, List<SqlNode> inputList) {
        SqlNode partitionByNode = callSqlFunction.getPartitionBy();
        SqlNode partitionSizeNode = callSqlFunction.getPartitionSize();
        if (partitionByNode != null) {
            if (partitionByNode instanceof SqlIdentifier) {
                this.partitionBy = ((SqlIdentifier) partitionByNode).getSimple();
            } else {
                throw new RuntimeException("PARTITION BY must be a simple identifier");
            }
            // validate partitionBy must be one of the function input tables
            boolean found = false;
            if (inputList != null) {
                for (SqlNode input : inputList) {
                    if (input instanceof SqlIdentifier
                            && NodeUtils.normalizeTableName(((SqlIdentifier) input).getSimple())
                            .equals(NodeUtils.normalizeTableName(this.partitionBy))) {
                        found = true;
                        break;
                    }
                }
            }
            if (!found) {
                throw new RuntimeException("PARTITION BY table '" + this.partitionBy + "' must be one of the function input tables");
            }
        }
        if (partitionSizeNode != null) {
            if (!(partitionSizeNode instanceof SqlLiteral)
                    && !(partitionSizeNode instanceof SqlGetVariable)) {
                throw new RuntimeException("SIZE must be an integer literal, get(), or get_or_default()");
            }
            this.partitionSizeNode = partitionSizeNode;
        }
    }

    public static BindableInterface getFunctionBindable(
            SqlCallSqlFunction callSqlFunction,
            CalciteSchema schema,
            CompileManager compileManager
    ) throws Exception {
        List<SqlNode> inputList = callSqlFunction.getInputTableList();
        SqlGetVariable funcNameVariable = callSqlFunction.getFuncNameVariable();
        SqlIdentifier likeTableNameIdentifier = callSqlFunction.getLikeTableName();
        SqlNode likeFunctionNameNode = callSqlFunction.getLikeFunctionName();

        List<RelDataTypeField> returnDataFields = null;
        if (likeTableNameIdentifier != null) {
            String likeTableName = likeTableNameIdentifier.getSimple();
            returnDataFields = SchemaUtils.getDataTypeByLikeTableName(likeTableName, schema);
        } else if (likeFunctionNameNode != null) {
            String likeFunctionName = SchemaUtils.getValueOfStringLiteral(likeFunctionNameNode);
            SqlFunctionBindable likeFunctionBindable = compileManager.getSqlFunction(likeFunctionName);
            if (likeFunctionBindable == null) {
                throw new RuntimeException("like function not found: " + likeFunctionName);
            }
            returnDataFields = likeFunctionBindable.getReturnDataFields();
        }

        if (funcNameVariable != null) {
            return new FunctionProxyBindable(
                    callSqlFunction, inputList, funcNameVariable, returnDataFields, callSqlFunction.isAsync()
            );
        }

        String functionName = callSqlFunction.getFuncName().getSimple();
        BindableInterface delegate = getFunctionBindableByName(
                functionName, schema, inputList, returnDataFields, compileManager
        );
        return new FunctionProxyBindable(callSqlFunction, inputList, delegate, callSqlFunction.isAsync());
    }

    public static BindableInterface getFunctionBindableByName(
            String functionName,
            CalciteSchema schema,
            List<SqlNode> inputList,
            List<RelDataTypeField> returnDataFields,
            CompileManager compileManager
    ) throws Exception {
        Object javaFunctionObj = JavaFunctionUtils.getTableFunction(Consts.DEFAULT_SCHEMA_NAME, functionName);
        if (javaFunctionObj != null) {
            return new JavaFunctionBindable(
                    functionName, javaFunctionObj, inputList, returnDataFields, schema
            );
        }

        SqlFunctionBindable sqlFunctionBindable = compileManager.getSqlFunction(functionName);
        if (sqlFunctionBindable != null) {
            List<String> inputTableList = new ArrayList<>();
            for (SqlNode input : inputList) {
                if (input instanceof SqlIdentifier) {
                    inputTableList.add(((SqlIdentifier) input).getSimple());
                } else {
                    throw new Exception("function input table must be table name");
                }
            }
            CallSqlFunctionBindable callSqlFunctionBindable = new CallSqlFunctionBindable(
                    functionName, inputTableList, sqlFunctionBindable);
            callSqlFunctionBindable.checkInputTable(schema);
            return callSqlFunctionBindable;
        }

        throw new Exception("function not find: " + functionName);
    }

    @Override
    public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
        BindableInterface targetBindable = resolveBindable(schema, context);

        if (partitionBy != null) {
            if (isAsync) {
                // Async calls are intentionally fire-and-forget; failures are reported by the
                // existing logging and metrics instrumentation instead of being propagated synchronously.
                ExecutorServiceUtils.getExecutorService().submit(() -> bindWithPartition(schema, context, targetBindable));
                return null;
            }
            return bindWithPartition(schema, context, targetBindable);
        }

        if (isAsync) {
            // Async calls are intentionally fire-and-forget; failures are reported by the
            // existing logging and metrics instrumentation instead of being propagated synchronously.
            ExecutorServiceUtils.getExecutorService().submit(() -> targetBindable.bind(schema, context));
            return null;
        } else {
            return targetBindable.bind(schema, context);
        }
    }

    private Enumerable<Object[]> bindWithPartition(CalciteSchema schema, ExecuteContext context, BindableInterface targetBindable) {
        // get the CacheTable to partition by partitionBy (which is a table name)
        CacheTable partitionTable = SchemaUtils.getCacheTable(partitionBy, schema);
        List<RelDataTypeField> fields = partitionTable.getDataFields();

        // Resolve the size at execution time so request variables can control partitioning.
        int partitionSize = resolvePartitionSize(context);

        // read all rows and split by partitionSize
        List<Object[]> allRows = new ArrayList<>();
        partitionTable.scan(null).forEach(allRows::add);
        List<List<Object[]>> partitions = splitBySize(allRows, partitionSize);

        ExecuteContextImpl partitionContext = ((ExecuteContextImpl) context).clone();

        // execute each partition concurrently and merge results
        List<CompletableFuture<Enumerable<Object[]>>> futures = new ArrayList<>();
        for (List<Object[]> partitionRows : partitions) {
            CompletableFuture<Enumerable<Object[]>> future = CompletableFuture.supplyAsync(() -> {
                // create a temporary schema, replacing the partitioned table with a sub-table
                CalciteSchema partitionSchema = CalciteSchemaFactory.createCalciteSchema();
                for (String tableName : schema.getTableNames()) {
                    CalciteSchema.TableEntry entry = schema.getTable(tableName, false);
                    if (entry.getTable() instanceof CacheTable) {
                        if (NodeUtils.normalizeTableName(tableName)
                                .equals(NodeUtils.normalizeTableName(partitionBy))) {
                            partitionSchema.add(tableName, new CacheTable(tableName, Linq4j.asEnumerable(partitionRows), fields));
                        } else {
                            partitionSchema.add(tableName, entry.getTable());
                        }
                    }
                }
                return targetBindable.bind(partitionSchema, partitionContext);
            }, ExecutorServiceUtils.getExecutorService());
            futures.add(future);
        }

        boolean ignorePartitionException = SqlRecConfigs.IGNORE_PARTITION_EXCEPTION
                .getValue(context.getVariables());

        // wait for all partitions and merge results
        List<Object[]> mergedResults = new ArrayList<>();
        List<Throwable> failures = new ArrayList<>();
        int successCount = 0;
        for (int i = 0; i < futures.size(); i++) {
            try {
                Enumerable<Object[]> result = futures.get(i).join();
                // Materialize each result separately so a failure during enumeration does not
                // leave a partially merged result from that partition.
                List<Object[]> partitionResults = new ArrayList<>();
                if (result != null) {
                    result.forEach(partitionResults::add);
                }
                mergedResults.addAll(partitionResults);
                successCount++;
            } catch (Exception e) {
                Throwable failure = unwrapPartitionFailure(e);
                if (!ignorePartitionException
                        || context.isCancelled()
                        || partitionContext.isCancelled()
                        || failure instanceof InterruptedException
                        || failure instanceof Error) {
                    cancelPartitionTasks(partitionContext, futures);
                    throw new RuntimeException("Partition execution failed", failure);
                }

                failures.add(failure);
                log.warn("[{}] partition {}/{} execution failed and its result is discarded: {}",
                        context.getLogId(), i + 1, futures.size(), failure.getMessage(), failure);
            }
        }

        if (successCount == 0 && !failures.isEmpty()) {
            cancelPartitionTasks(partitionContext, futures);
            RuntimeException allFailed = new RuntimeException(
                    "All " + failures.size() + " partition executions failed", failures.get(0));
            for (int i = 1; i < failures.size(); i++) {
                allFailed.addSuppressed(failures.get(i));
            }
            throw allFailed;
        }

        return Linq4j.asEnumerable(mergedResults);
    }

    private int resolvePartitionSize(ExecuteContext context) {
        if (partitionSizeNode instanceof SqlLiteral) {
            return ((SqlLiteral) partitionSizeNode).intValue(false);
        }

        SqlGetVariable getVariable = (SqlGetVariable) partitionSizeNode;
        String variableName = SchemaUtils.getValueOfStringLiteral(getVariable.getVariableName());
        String value = context.getVariable(variableName);
        if (value == null && getVariable.hasDefaultValue()) {
            value = SchemaUtils.getValueOfStringLiteral((SqlCharStringLiteral) getVariable.getDefaultValue());
        }
        if (value == null) {
            throw new RuntimeException("cant get partition size from variable: " + variableName);
        }

        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new RuntimeException(
                    "Invalid partition size from variable '" + variableName + "': '" + value + "'", e);
        }
    }

    private Throwable unwrapPartitionFailure(Throwable failure) {
        Throwable result = failure;
        while (result instanceof CompletionException && result.getCause() != null) {
            result = result.getCause();
        }
        return result;
    }

    private void cancelPartitionTasks(
            ExecuteContextImpl partitionContext,
            List<CompletableFuture<Enumerable<Object[]>>> futures
    ) {
        partitionContext.cancel();
        for (CompletableFuture<Enumerable<Object[]>> future : futures) {
            future.cancel(true);
        }
    }

    private List<List<Object[]>> splitBySize(List<Object[]> rows, int size) {
        if (size <= 0) {
            return Collections.singletonList(rows);
        }
        List<List<Object[]>> partitions = new ArrayList<>();
        for (int i = 0; i < rows.size(); i += size) {
            partitions.add(rows.subList(i, Math.min(i + size, rows.size())));
        }
        return partitions;
    }

    private BindableInterface resolveBindable(CalciteSchema schema, ExecuteContext context) {
        if (delegate != null) {
            return delegate;
        }

        String variableName = SchemaUtils.getValueOfStringLiteral(funcNameVariable.getVariableName());
        String functionName = context.getVariable(variableName);
        if (StringUtils.isEmpty(functionName)) {
            if (funcNameVariable.hasDefaultValue()) {
                functionName = SchemaUtils.getValueOfStringLiteral((SqlCharStringLiteral) funcNameVariable.getDefaultValue());
            } else {
                throw new RuntimeException("cant get function name from variable: " + variableName);
            }
        }
        try {
            return getFunctionBindableByName(
                    functionName, schema, inputList, returnDataFields, new CompileManager()
            );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<RelDataTypeField> getReturnDataFields() {
        if (delegate != null) {
            return delegate.getReturnDataFields();
        }
        return returnDataFields;
    }

    @Override
    public boolean isParallelizable() {
        if (isAsync) {
            return true;
        }
        if (delegate != null) {
            return delegate.isParallelizable();
        }
        return false;
    }

    @Override
    public boolean isTimeoutAble(CalciteSchema schema, ExecuteContext context) {
        if (delegate != null) {
            return delegate.isTimeoutAble(schema, context);
        }
        try {
            String variableName = SchemaUtils.getValueOfStringLiteral(funcNameVariable.getVariableName());
            String functionName = context.getVariable(variableName);
            if (StringUtils.isEmpty(functionName)) {
                if (funcNameVariable.hasDefaultValue()) {
                    functionName = SchemaUtils.getValueOfStringLiteral((SqlCharStringLiteral) funcNameVariable.getDefaultValue());
                } else {
                    return false;
                }
            }
            Object javaFunctionObj = JavaFunctionUtils.getTableFunction(Consts.DEFAULT_SCHEMA_NAME, functionName);
            return javaFunctionObj != null;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public Set<String> getReadTables() {
        if (delegate != null) {
            return delegate.getReadTables();
        }
        Set<String> readTables = new HashSet<>();
        if (inputList != null) {
            for (SqlNode input : inputList) {
                if (input instanceof SqlIdentifier) {
                    readTables.add(NodeUtils.normalizeTableName(((SqlIdentifier) input).getSimple()));
                }
            }
        }
        return readTables;
    }

    @Override
    public Set<String> getWriteTables() {
        if (delegate != null) {
            return delegate.getWriteTables();
        }
        return Set.of();
    }

    @Override
    public Set<String> getDependencyJavaFuncName() {
        if (delegate != null) {
            return delegate.getDependencyJavaFuncName();
        }
        return new HashSet<>();
    }

    @Override
    public Set<String> getDependencySqlFuncName() {
        if (delegate != null) {
            return delegate.getDependencySqlFuncName();
        }
        return new HashSet<>();
    }

    @Override
    public Map<String, String> getAllDependSqlFunctionMap() {
        if (delegate != null) {
            return delegate.getAllDependSqlFunctionMap();
        }
        return super.getAllDependSqlFunctionMap();
    }

    @Override
    public String getCacheTableName() {
        if (delegate != null) {
            return delegate.getCacheTableName();
        }
        return null;
    }

    @Override
    public List<RelDataTypeField> getCacheTableDataFields() {
        if (delegate != null) {
            return delegate.getCacheTableDataFields();
        }
        return null;
    }

    @Override
    public boolean isUnionSql() {
        if (delegate != null) {
            return delegate.isUnionSql();
        }
        return false;
    }

    @Override
    public String getLogicalPlan() {
        if (delegate != null) {
            return delegate.getLogicalPlan();
        }
        return null;
    }

    @Override
    public String getPhysicalPlan() {
        if (delegate != null) {
            return delegate.getPhysicalPlan();
        }
        return null;
    }

    @Override
    public String getJavaExpression() {
        if (delegate != null) {
            return delegate.getJavaExpression();
        }
        return null;
    }
}
