package com.sqlrec.runtime;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.utils.ExecutorServiceUtils;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.schema.JavaFunctionUtils;
import com.sqlrec.sql.parser.SqlCallSqlFunction;
import com.sqlrec.sql.parser.SqlGetVariable;
import com.sqlrec.utils.NodeUtils;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import java.util.*;

public class FunctionProxyBindable extends ForwardingBindable {
    private final List<SqlNode> inputList;
    private final SqlGetVariable funcNameVariable;
    private final BindableInterface delegate;
    private final List<RelDataTypeField> declaredReturnFields;
    private final boolean async;
    private final PartitionExecutor partitionExecutor;

    public FunctionProxyBindable(
            SqlCallSqlFunction callSqlFunction,
            List<SqlNode> inputList,
            SqlGetVariable funcNameVariable,
            List<RelDataTypeField> declaredReturnFields,
            boolean isAsync
    ) {
        if (declaredReturnFields == null) {
            throw new RuntimeException("return data fields is null");
        }

        this.inputList = inputList;
        this.delegate = null;
        this.funcNameVariable = funcNameVariable;
        this.declaredReturnFields = declaredReturnFields;
        this.async = isAsync;
        this.partitionExecutor = createPartitionExecutor(callSqlFunction, inputList);
    }

    public FunctionProxyBindable(
            SqlCallSqlFunction callSqlFunction,
            List<SqlNode> inputList,
            BindableInterface delegate,
            boolean isAsync
    ) {
        this.inputList = inputList;
        this.delegate = Objects.requireNonNull(delegate);
        this.funcNameVariable = null;
        this.declaredReturnFields = null;
        this.async = isAsync;
        this.partitionExecutor = createPartitionExecutor(callSqlFunction, inputList);
    }

    private static PartitionExecutor createPartitionExecutor(
            SqlCallSqlFunction callSqlFunction,
            List<SqlNode> inputList
    ) {
        SqlNode partitionByNode = callSqlFunction.getPartitionBy();
        String partitionTable = null;
        if (partitionByNode != null) {
            if (!(partitionByNode instanceof SqlIdentifier identifier)) {
                throw new RuntimeException("PARTITION BY must be a simple identifier");
            }
            partitionTable = identifier.getSimple();
            if (!inputTableNames(inputList).contains(NodeUtils.normalizeTableName(partitionTable))) {
                throw new RuntimeException("PARTITION BY table '" + partitionTable
                        + "' must be one of the function input tables");
            }
        }

        SqlNode sizeNode = callSqlFunction.getPartitionSize();
        if (sizeNode != null && !(sizeNode instanceof SqlLiteral) && !(sizeNode instanceof SqlGetVariable)) {
            throw new RuntimeException("SIZE must be an integer literal or get()");
        }
        return partitionTable == null ? null : new PartitionExecutor(partitionTable, sizeNode);
    }

    public static BindableInterface getFunctionBindable(
            SqlCallSqlFunction callSqlFunction,
            CalciteSchema schema,
            CompileManager compileManager
    ) throws Exception {
        return FunctionBindableFactory.create(callSqlFunction, schema, compileManager);
    }

    @Override
    protected BindableInterface metadataDelegate() {
        return delegate;
    }

    @Override
    public Enumerable<Object[]> bind(CalciteSchema schema, ExecuteContext context) {
        BindableInterface targetBindable = resolveBindable(schema, context);

        if (async) {
            ExecutorServiceUtils.submit(() -> executeTarget(targetBindable, schema, context));
            return null;
        }
        return executeTarget(targetBindable, schema, context);
    }

    private Enumerable<Object[]> executeTarget(
            BindableInterface target, CalciteSchema schema, ExecuteContext context) {
        return partitionExecutor == null
                ? target.bind(schema, context)
                : partitionExecutor.execute(schema, context, target);
    }

    private BindableInterface resolveBindable(CalciteSchema schema, ExecuteContext context) {
        if (delegate != null) {
            return delegate;
        }

        String functionName = resolveFunctionName(context);
        try {
            return FunctionBindableFactory.createByName(
                    functionName, schema, inputList, declaredReturnFields, new CompileManager()
            );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String resolveFunctionName(ExecuteContext context) {
        String functionName = RuntimeValueResolver.resolveVariable(funcNameVariable, context,
                RuntimeValueResolver.MissingValuePolicy.NULL_OR_EMPTY);
        // A declared default is returned even when it is empty, matching dynamic call semantics.
        if ((functionName != null && !functionName.isEmpty()) || funcNameVariable.hasDefaultValue()) {
            return functionName;
        }
        throw new RuntimeException("cant get function name from variable: "
                + RuntimeValueResolver.variableName(funcNameVariable));
    }

    @Override
    public List<RelDataTypeField> getReturnDataFields() {
        return delegate == null ? declaredReturnFields : delegate.getReturnDataFields();
    }

    @Override
    public boolean isParallelizable() {
        return async || super.isParallelizable();
    }

    @Override
    public boolean isTimeoutAble(CalciteSchema schema, ExecuteContext context) {
        if (delegate != null) {
            return delegate.isTimeoutAble(schema, context);
        }
        try {
            String functionName = resolveFunctionName(context);
            Object javaFunctionObj = JavaFunctionUtils.getTableFunction(Consts.DEFAULT_SCHEMA_NAME, functionName);
            return javaFunctionObj != null;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public Set<String> getReadTables() {
        return delegate == null ? inputTableNames(inputList) : delegate.getReadTables();
    }

    private static Set<String> inputTableNames(List<SqlNode> inputList) {
        Set<String> names = new HashSet<>();
        if (inputList != null) {
            for (SqlNode input : inputList) {
                if (input instanceof SqlIdentifier identifier) {
                    names.add(NodeUtils.normalizeTableName(identifier.getSimple()));
                }
            }
        }
        return names;
    }

    @Override
    public Set<String> getWriteTables() {
        if (delegate != null) {
            return delegate.getWriteTables();
        }
        return Set.of();
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
}
