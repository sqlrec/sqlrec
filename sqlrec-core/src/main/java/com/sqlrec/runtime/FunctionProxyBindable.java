package com.sqlrec.runtime;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.schema.CalciteSchemaFactory;
import com.sqlrec.schema.JavaFunctionUtils;
import com.sqlrec.sql.parser.SqlCallSqlFunction;
import com.sqlrec.sql.parser.SqlGetVariable;
import com.sqlrec.utils.Executor;
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

public class FunctionProxyBindable extends BindableInterface {
    private static final Logger log = LoggerFactory.getLogger(FunctionProxyBindable.class);

    private List<SqlNode> inputList;
    private SqlGetVariable funcNameVariable;
    private BindableInterface delegate;
    private List<RelDataTypeField> returnDataFields;
    private boolean isAsync;
    private String partitionBy;
    private int partitionSize;

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
                    if (input instanceof SqlIdentifier && ((SqlIdentifier) input).getSimple().equals(this.partitionBy)) {
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
            if (partitionSizeNode instanceof SqlLiteral) {
                this.partitionSize = ((SqlLiteral) partitionSizeNode).intValue(false);
            } else {
                throw new RuntimeException("SIZE must be an integer literal");
            }
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
                Executor.getExecutorService().submit(() -> bindWithPartition(schema, context, targetBindable));
                return null;
            }
            return bindWithPartition(schema, context, targetBindable);
        }

        if (isAsync) {
            Executor.getExecutorService().submit(() -> targetBindable.bind(schema, context));
            return null;
        } else {
            return targetBindable.bind(schema, context);
        }
    }

    private Enumerable<Object[]> bindWithPartition(CalciteSchema schema, ExecuteContext context, BindableInterface targetBindable) {
        // get the CacheTable to partition by partitionBy (which is a table name)
        CacheTable partitionTable = SchemaUtils.getCacheTable(partitionBy, schema);
        List<RelDataTypeField> fields = partitionTable.getDataFields();

        // read all rows and split by partitionSize
        List<Object[]> allRows = new ArrayList<>();
        partitionTable.scan(null).forEach(allRows::add);
        List<List<Object[]>> partitions = splitBySize(allRows, partitionSize);

        // execute each partition concurrently and merge results
        List<CompletableFuture<Enumerable<Object[]>>> futures = new ArrayList<>();
        for (List<Object[]> partitionRows : partitions) {
            CompletableFuture<Enumerable<Object[]>> future = CompletableFuture.supplyAsync(() -> {
                // create a temporary schema, replacing the partitioned table with a sub-table
                CalciteSchema partitionSchema = CalciteSchemaFactory.createCalciteSchema();
                for (String tableName : schema.getTableNames()) {
                    CalciteSchema.TableEntry entry = schema.getTable(tableName, false);
                    if (entry.getTable() instanceof CacheTable) {
                        if (tableName.equals(partitionBy)) {
                            partitionSchema.add(tableName, new CacheTable(tableName, Linq4j.asEnumerable(partitionRows), fields));
                        } else {
                            partitionSchema.add(tableName, entry.getTable());
                        }
                    }
                }
                return targetBindable.bind(partitionSchema, context);
            }, Executor.getExecutorService());
            futures.add(future);
        }

        // wait for all partitions and merge results
        List<Object[]> mergedResults = new ArrayList<>();
        for (int i = 0; i < futures.size(); i++) {
            try {
                Enumerable<Object[]> result = futures.get(i).join();
                if (result != null) {
                    result.forEach(mergedResults::add);
                }
            } catch (Exception e) {
                throw new RuntimeException("Partition " + i + " execution failed", e);
            }
        }

        return Linq4j.asEnumerable(mergedResults);
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
        return Set.of();
    }

    @Override
    public Set<String> getWriteTables() {
        if (delegate != null) {
            return delegate.getWriteTables();
        }
        return Set.of();
    }

    @Override
    public String getDependencyJavaFuncName() {
        if (delegate != null) {
            return delegate.getDependencyJavaFuncName();
        }
        return null;
    }

    @Override
    public String getDependencySqlFuncName() {
        if (delegate != null) {
            return delegate.getDependencySqlFuncName();
        }
        return null;
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
