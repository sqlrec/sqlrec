package com.sqlrec.runtime;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.schema.JavaFunctionUtils;
import com.sqlrec.sql.parser.SqlCallSqlFunction;
import com.sqlrec.sql.parser.SqlGetVariable;
import com.sqlrec.utils.ExecutorServiceUtils;
import com.sqlrec.utils.NodeUtils;
import com.sqlrec.utils.SchemaUtils;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang3.StringUtils;
import java.util.*;

public class FunctionProxyBindable extends BindableInterface {
    private final List<SqlNode> inputList;
    private final SqlGetVariable funcNameVariable;
    private final BindableInterface delegate;
    private final List<RelDataTypeField> returnDataFields;
    private final boolean async;
    private final PartitionExecutor partitionExecutor;

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
        this.async = isAsync;
        PartitionSpec partitionSpec = extractPartitionInfo(callSqlFunction, inputList);
        this.partitionExecutor = partitionSpec.tableName() == null ? null
                : new PartitionExecutor(partitionSpec.tableName(), partitionSpec.sizeNode());
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
        this.async = isAsync;
        PartitionSpec partitionSpec = extractPartitionInfo(callSqlFunction, inputList);
        this.partitionExecutor = partitionSpec.tableName() == null ? null
                : new PartitionExecutor(partitionSpec.tableName(), partitionSpec.sizeNode());
    }

    private static PartitionSpec extractPartitionInfo(
            SqlCallSqlFunction callSqlFunction,
            List<SqlNode> inputList
    ) {
        SqlNode partitionByNode = callSqlFunction.getPartitionBy();
        String partitionTable = null;
        if (partitionByNode != null) {
            if (partitionByNode instanceof SqlIdentifier) {
                partitionTable = ((SqlIdentifier) partitionByNode).getSimple();
            } else {
                throw new RuntimeException("PARTITION BY must be a simple identifier");
            }
            // validate partitionBy must be one of the function input tables
            boolean found = false;
            if (inputList != null) {
                for (SqlNode input : inputList) {
                    if (input instanceof SqlIdentifier
                            && NodeUtils.normalizeTableName(((SqlIdentifier) input).getSimple())
                            .equals(NodeUtils.normalizeTableName(partitionTable))) {
                        found = true;
                        break;
                    }
                }
            }
            if (!found) {
                throw new RuntimeException("PARTITION BY table '" + partitionTable
                        + "' must be one of the function input tables");
            }
        }

        SqlNode sizeNode = callSqlFunction.getPartitionSize();
        if (sizeNode != null) {
            if (!(sizeNode instanceof SqlLiteral)
                    && !(sizeNode instanceof SqlGetVariable)) {
                throw new RuntimeException("SIZE must be an integer literal, get(), or get_or_default()");
            }
        }
        return new PartitionSpec(partitionTable, sizeNode);
    }

    public static BindableInterface getFunctionBindable(
            SqlCallSqlFunction callSqlFunction,
            CalciteSchema schema,
            CompileManager compileManager
    ) throws Exception {
        return FunctionBindableFactory.create(callSqlFunction, schema, compileManager);
    }

    public static BindableInterface getFunctionBindableByName(
            String functionName,
            CalciteSchema schema,
            List<SqlNode> inputList,
            List<RelDataTypeField> returnDataFields,
            CompileManager compileManager
    ) throws Exception {
        return FunctionBindableFactory.createByName(
                functionName, schema, inputList, returnDataFields, compileManager);
    }

    @Override
    public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
        BindableInterface targetBindable = resolveBindable(schema, context);

        if (partitionExecutor != null) {
            if (async) {
                submitAsync(() -> partitionExecutor.execute(schema, context, targetBindable));
                return null;
            }
            return partitionExecutor.execute(schema, context, targetBindable);
        }

        if (async) {
            submitAsync(() -> targetBindable.bind(schema, context));
            return null;
        }
        return targetBindable.bind(schema, context);
    }

    private void submitAsync(Runnable task) {
        ExecutorServiceUtils.getExecutorService().submit(task);
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
        if (async) {
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

    private record PartitionSpec(String tableName, SqlNode sizeNode) {
    }
}
