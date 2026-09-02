package com.sqlrec.compiler;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.common.utils.ResourceNames;
import com.sqlrec.runtime.BindableInterface;
import com.sqlrec.runtime.SqlFunctionBindable;
import com.sqlrec.schema.CalciteSchemaFactory;
import com.sqlrec.sql.parser.SqlCreateSqlFunction;
import com.sqlrec.sql.parser.SqlDefineInputTable;
import com.sqlrec.sql.parser.SqlIfCache;
import com.sqlrec.sql.parser.SqlReturn;
import com.sqlrec.utils.SchemaUtils;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class FunctionCompiler {
    public enum FunctionCompileStage {
        FUNCTION_DEFINITION,
        FUNCTION_PARAM,
        FUNCTION_BODY,
        FUNCTION_RETURN,
    }

    private FunctionCompileStage stage;
    private Boolean isOrReplace;
    private CalciteSchema schema;
    private SqlFunctionBindable sqlFunctionBindable;
    private List<String> sqlList;
    private Set<String> cacheTableNames;
    private CompileManager compileManager;
    private List<RelDataTypeField> returnDataFields;
    private boolean returnDataFieldsInitialized;
    private boolean awaitingEmptyReturnAfterExhaustiveIf;

    public FunctionCompiler(CalciteSchema schema, CompileManager compileManager) {
        this.isOrReplace = false;
        this.stage = FunctionCompileStage.FUNCTION_DEFINITION;
        if (schema != null) {
            this.schema = schema;
        } else {
            this.schema = CalciteSchemaFactory.createCalciteSchema();
        }
        this.sqlFunctionBindable = new SqlFunctionBindable(
                new ArrayList<>(),
                new ArrayList<>(),
                null
        );
        sqlList = new ArrayList<>();
        if (compileManager != null) {
            this.compileManager = compileManager;
        } else {
            this.compileManager = new CompileManager();
        }
        cacheTableNames = new HashSet<>();
        returnDataFields = null;
        returnDataFieldsInitialized = false;
        awaitingEmptyReturnAfterExhaustiveIf = false;
    }

    public SqlFunctionBindable getFunctionBindable() {
        if (!isFunctionCompileFinish()) {
            throw new RuntimeException("function compile not finish");
        }
        return sqlFunctionBindable;
    }

    public List<String> getSqlList() {
        return sqlList;
    }

    public boolean isFunctionCompileFinish() {
        return stage == FunctionCompileStage.FUNCTION_RETURN;
    }

    public void compileAllSql(List<String> sqls) throws Exception {
        for (String sql : sqls) {
            compile(sql);
        }
    }

    public void compile(String sql) throws Exception {
        SqlNode flinkSqlNode = CompileManager.parseFlinkSql(sql);
        compile(flinkSqlNode, sql);
    }

    public void compile(SqlNode flinkSqlNode, String sql) throws Exception {
        switch (stage) {
            case FUNCTION_DEFINITION:
                compileFunctionDefinition(flinkSqlNode);
                break;
            case FUNCTION_PARAM:
                compileFunctionParam(flinkSqlNode, sql);
                break;
            case FUNCTION_BODY:
                compileFunctionBody(flinkSqlNode, sql);
                break;
            case FUNCTION_RETURN:
                throw new Exception("sql after return is invalid");
            default:
                throw new Exception("stage is invalid:" + stage);
        }
        sqlList.add(sql);
    }

    private void compileFunctionDefinition(SqlNode flinkSqlNode) {
        if (flinkSqlNode instanceof SqlCreateSqlFunction) {
            SqlCreateSqlFunction sqlCreateFunction = (SqlCreateSqlFunction) flinkSqlNode;
            sqlFunctionBindable.setFunName(ResourceNames.of(sqlCreateFunction.getFuncName()));
            isOrReplace = sqlCreateFunction.isOrReplace();
            stage = FunctionCompileStage.FUNCTION_PARAM;
        } else {
            throw new RuntimeException("sql before function definition is invalid");
        }
    }

    private void compileFunctionParam(SqlNode flinkSqlNode, String sql) throws Exception {
        if (flinkSqlNode instanceof SqlDefineInputTable) {
            SqlDefineInputTable sqlDefineInputTable = (SqlDefineInputTable) flinkSqlNode;
            List<RelDataTypeField> relDataTypeFields;
            if (sqlDefineInputTable.getLikeTable() != null) {
                String likeTableName = sqlDefineInputTable.getLikeTable().toString();
                Table table = SchemaUtils.getTableObj(schema, Consts.DEFAULT_SCHEMA_NAME, likeTableName);
                if (table == null) {
                    throw new Exception("like table not found: " + likeTableName);
                }
                RelDataType rowType = table.getRowType(new JavaTypeFactoryImpl());
                relDataTypeFields = rowType.getFieldList();
            } else {
                relDataTypeFields = getTableFieldsTypes(
                        sqlDefineInputTable.getColumnList(),
                        sqlDefineInputTable.getColumnTypeList()
                );
            }
            sqlFunctionBindable.addInputTable(sqlDefineInputTable.getTableName().getSimple(), relDataTypeFields);
            CacheTable tmpTable = new CacheTable(
                    sqlDefineInputTable.getTableName().getSimple(),
                    null,
                    relDataTypeFields
            );
            schema.add(sqlDefineInputTable.getTableName().getSimple(), tmpTable);
        } else {
            stage = FunctionCompileStage.FUNCTION_BODY;
            compileFunctionBody(flinkSqlNode, sql);
        }
    }

    private void compileFunctionBody(SqlNode flinkSqlNode, String sql) throws Exception {
        if (awaitingEmptyReturnAfterExhaustiveIf) {
            compileEmptyTerminatingReturn(flinkSqlNode, sql);
            return;
        }

        if (flinkSqlNode instanceof SqlReturn) {
            compileTopLevelReturn((SqlReturn) flinkSqlNode, sql);
        } else {
            BindableInterface bindable = compileManager.compileSql(
                    flinkSqlNode, schema, Consts.DEFAULT_SCHEMA_NAME, sql
            );
            collectReturnDataFields(bindable);
            addBodyBindable(bindable, sql, false);
            if (isExhaustiveReturnIf(flinkSqlNode)) {
                awaitingEmptyReturnAfterExhaustiveIf = true;
            }
        }
    }

    private void compileEmptyTerminatingReturn(SqlNode flinkSqlNode, String sql) throws Exception {
        if (!(flinkSqlNode instanceof SqlReturn) || !isEmptyReturn((SqlReturn) flinkSqlNode)) {
            throw new Exception(
                    "IF whose THEN and ELSE branches both RETURN must be followed by exactly one empty RETURN"
            );
        }

        // The empty RETURN is only the syntactic function terminator. Both IF branches
        // already determine the function's result schema and return at runtime.
        compileTopLevelReturn((SqlReturn) flinkSqlNode, sql, false);
    }

    private boolean isExhaustiveReturnIf(SqlNode sqlNode) {
        if (!(sqlNode instanceof SqlIfCache)) {
            return false;
        }
        SqlIfCache sqlIf = (SqlIfCache) sqlNode;
        return sqlIf.getThenClause() instanceof SqlReturn
                && sqlIf.getElseClause() instanceof SqlReturn;
    }

    private boolean isEmptyReturn(SqlReturn sqlReturn) {
        return sqlReturn.getTableName() == null
                && sqlReturn.getSelect() == null
                && sqlReturn.getCallSqlFunction() == null;
    }

    private void compileTopLevelReturn(SqlReturn sqlReturn, String sql) throws Exception {
        compileTopLevelReturn(sqlReturn, sql, true);
    }

    private void compileTopLevelReturn(
            SqlReturn sqlReturn,
            String sql,
            boolean collectDataFields
    ) throws Exception {
        BindableInterface returnBindable = compileManager.compileSql(
                sqlReturn, schema, Consts.DEFAULT_SCHEMA_NAME, sql
        );
        if (collectDataFields) {
            collectReturnDataFields(returnBindable);
        }
        addBodyBindable(returnBindable, sql, true);
        sqlFunctionBindable.setReturnDataFields(returnDataFields);
        sqlFunctionBindable.init();
        stage = FunctionCompileStage.FUNCTION_RETURN;
    }

    private void addBodyBindable(BindableInterface bindable, String sql, boolean returnNode) {
        sqlFunctionBindable.getBindableList().add(bindable);
        int bindableIndex = sqlFunctionBindable.getBindableList().size();
        if (returnNode) {
            bindable.setName(sqlFunctionBindable.getFunName() + ":RETURN");
        } else if (StringUtils.isNotEmpty(bindable.getCacheTableName())) {
            CacheTable tmpTable = new CacheTable(
                    bindable.getCacheTableName(),
                    null,
                    bindable.getCacheTableDataFields()
            );
            schema.add(bindable.getCacheTableName(), tmpTable);
            if (!cacheTableNames.contains(bindable.getCacheTableName())) {
                cacheTableNames.add(bindable.getCacheTableName());
                bindable.setName(sqlFunctionBindable.getFunName() + ":" + bindable.getCacheTableName());
            } else {
                bindable.setName(sqlFunctionBindable.getFunName() +
                        ":" + bindable.getCacheTableName() + ":" + bindableIndex);
            }
        } else {
            bindable.setName(sqlFunctionBindable.getFunName() + ":" +
                    SchemaUtils.getSqlFirstWord(sql) + ":" + bindableIndex);
        }
    }

    private void collectReturnDataFields(BindableInterface bindable) {
        if (bindable.containsReturn()) {
            registerReturnDataFields(bindable.getReturnDataFields());
        }
    }

    private void registerReturnDataFields(List<RelDataTypeField> dataFields) {
        boolean hasDataFields = dataFields != null && !dataFields.isEmpty();
        if (!returnDataFieldsInitialized) {
            returnDataFields = hasDataFields ? dataFields : null;
            returnDataFieldsInitialized = true;
            return;
        }

        boolean returnHasDataFields = returnDataFields != null && !returnDataFields.isEmpty();
        if (returnHasDataFields != hasDataFields) {
            throw new RuntimeException("return statements must either all return data or all return empty");
        }
        if (hasDataFields) {
            DataTypeUtils.checkTableSchemaSame(returnDataFields, dataFields);
        }
    }

    private List<RelDataTypeField> getTableFieldsTypes(List<SqlIdentifier> columnList, List<SqlTypeNameSpec> columnTypeList) {
        SqlValidator validator = NormalSqlCompiler.createSqlValidate(schema, Consts.DEFAULT_SCHEMA_NAME);
        return DataTypeUtils.getRelDataTypeFields(columnList, columnTypeList, validator);
    }

    public boolean isOrReplace() {
        return isOrReplace;
    }
}
