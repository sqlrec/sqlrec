package com.sqlrec.compiler;

import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.runtime.BindableInterface;
import com.sqlrec.runtime.CacheTableBindable;
import com.sqlrec.runtime.SqlFunctionBindable;
import com.sqlrec.schema.HmsSchema;
import com.sqlrec.sql.parser.SqlCreateSqlFunction;
import com.sqlrec.sql.parser.SqlDefineInputTable;
import com.sqlrec.sql.parser.SqlReturn;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.calcite.sql.validate.SqlValidator;

import java.util.ArrayList;
import java.util.List;

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

    public FunctionCompiler(CalciteSchema schema) {
        this.isOrReplace = false;
        this.stage = FunctionCompileStage.FUNCTION_DEFINITION;
        if (schema != null) {
            this.schema = schema;
        } else {
            this.schema = HmsSchema.getHmsCalciteSchema();
        }
        this.sqlFunctionBindable = new SqlFunctionBindable(
                new ArrayList<>(),
                new ArrayList<>(),
                null,
                null
        );
        sqlList = new ArrayList<>();
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
                compileFunctionParam(flinkSqlNode);
                break;
            case FUNCTION_BODY:
                compileFunctionBody(flinkSqlNode);
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
            sqlFunctionBindable.setFunName(sqlCreateFunction.getFuncName().getSimple());
            isOrReplace = sqlCreateFunction.isOrReplace();
            stage = FunctionCompileStage.FUNCTION_PARAM;
        } else {
            throw new RuntimeException("sql before function definition is invalid");
        }
    }

    private void compileFunctionParam(SqlNode flinkSqlNode) throws Exception {
        if (flinkSqlNode instanceof SqlDefineInputTable) {
            SqlDefineInputTable sqlDefineInputTable = (SqlDefineInputTable) flinkSqlNode;
            List<RelDataTypeField> relDataTypeFields = getTableFieldsTypes(
                    sqlDefineInputTable.getColumnList(),
                    sqlDefineInputTable.getColumnTypeList()
            );
            sqlFunctionBindable.addInputTable(sqlDefineInputTable.getTableName().getSimple(), relDataTypeFields);
            CacheTable tmpTable = new CacheTable(
                    sqlDefineInputTable.getTableName().getSimple(),
                    null,
                    relDataTypeFields
            );
            schema.add(sqlDefineInputTable.getTableName().getSimple(), tmpTable);
        } else {
            stage = FunctionCompileStage.FUNCTION_BODY;
            compileFunctionBody(flinkSqlNode);
        }
    }

    private void compileFunctionBody(SqlNode flinkSqlNode) throws Exception {
        if (flinkSqlNode instanceof SqlReturn) {
            SqlReturn sqlReturn = (SqlReturn) flinkSqlNode;
            if (sqlReturn.getTableName() != null) {
                String returnTableName = sqlReturn.getTableName().getSimple();
                CalciteSchema.TableEntry tableEntry = schema.getTable(returnTableName, false);
                if (tableEntry == null) {
                    throw new Exception("return table not found: " + returnTableName);
                }

                CacheTable table;
                if (tableEntry.getTable() instanceof CacheTable) {
                    table = (CacheTable) tableEntry.getTable();
                } else {
                    throw new Exception("return table is not cache table");
                }

                sqlFunctionBindable.setReturnTableName(returnTableName);
                sqlFunctionBindable.setReturnDataFields(table.getDataFields());
            }

            sqlFunctionBindable.init();
            stage = FunctionCompileStage.FUNCTION_RETURN;
        } else {
            BindableInterface bindable = CompileManager.compileSql(flinkSqlNode, schema, NormalSqlCompiler.DEFAULT_SCHEMA_NAME);
            sqlFunctionBindable.getBindableList().add(bindable);
            if (bindable instanceof CacheTableBindable) {
                CacheTableBindable cacheTableBindable = (CacheTableBindable) bindable;
                CacheTable tmpTable = new CacheTable(
                        cacheTableBindable.getTableName(),
                        null,
                        cacheTableBindable.getTableDataFields()
                );
                schema.add(cacheTableBindable.getTableName(), tmpTable);
            }
        }
    }

    private List<RelDataTypeField> getTableFieldsTypes(List<SqlIdentifier> columnList, List<SqlTypeNameSpec> columnTypeList) {
        SqlValidator validator = NormalSqlCompiler.createSqlValidate(schema, NormalSqlCompiler.DEFAULT_SCHEMA_NAME);
        return DataTypeUtils.getRelDataTypeFields(columnList, columnTypeList, validator);
    }

    public boolean isOrReplace() {
        return isOrReplace;
    }
}
