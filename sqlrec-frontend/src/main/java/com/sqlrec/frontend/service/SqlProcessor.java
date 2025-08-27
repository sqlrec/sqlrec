package com.sqlrec.frontend.service;

import com.sqlrec.compiler.CompileManager;
import com.sqlrec.compiler.FunctionCompiler;
import com.sqlrec.compiler.NormalSqlCompiler;
import com.sqlrec.compiler.SqlTypeChecker;
import com.sqlrec.runtime.BindableInterface;
import com.sqlrec.schema.HmsSchema;
import com.sqlrec.sql.parser.SqlCreateApi;
import com.sqlrec.sql.parser.SqlCreateSqlFunction;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlNode;
import org.apache.hive.service.rpc.thrift.THandleIdentifier;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SqlProcessor {
    private CalciteSchema schema;
    private String defaultSchema;
    private FunctionCompiler functionCompiler;
    private Map<THandleIdentifier, SqlProcessResult> sqlProcessorMap;

    public SqlProcessor() {
        schema = HmsSchema.getHmsCalciteSchema();
        defaultSchema = NormalSqlCompiler.DEFAULT_SCHEMA_NAME;
        sqlProcessorMap = new ConcurrentHashMap<>();
    }

    public SqlProcessResult getProcessProcessResult(THandleIdentifier handleIdentifier) {
        return sqlProcessorMap.getOrDefault(handleIdentifier, null);
    }

    public void closeProcessProcessResult(THandleIdentifier handleIdentifier) {
        sqlProcessorMap.remove(handleIdentifier);
    }

    public SqlProcessResult tryExecuteSql(String sql) throws Exception {
        SqlProcessResult result = executeSql(sql);
        if (result != null) {
            sqlProcessorMap.put(result.handleIdentifier, result);
        }
        return result;
    }

    private SqlProcessResult executeSql(String sql) throws Exception {
        SqlNode sqlNode = CompileManager.parseFlinkSql(sql);

        SqlProcessResult result = tryCompileFunction(sqlNode, sql);
        if (result != null) {
            return result;
        }

        if (sqlNode instanceof SqlCreateApi) {
            // todo save api
            return Utils.convertMsgToResult("create api success");
        }

        if (SqlTypeChecker.isFlinkSqlCompilable(sqlNode, schema, defaultSchema)) {
            BindableInterface bindableInterface = CompileManager.compileSql(sqlNode, schema, defaultSchema);
            Enumerable<Object[]> enumerable = bindableInterface.bind(schema);
            List<RelDataTypeField> fields = bindableInterface.getReturnDataFields();
            return Utils.convertEnumerableToTRowSet(enumerable, fields);
        }

        return null;
    }

    private SqlProcessResult tryCompileFunction(SqlNode sqlNode, String sql) {
        try {
            if (functionCompiler != null) {
                functionCompiler.compile(sqlNode, sql);
                if (functionCompiler.isFunctionCompileFinish()) {
                    functionCompiler = null;
                    // todo save function
                    return Utils.convertMsgToResult("function compile success");
                } else {
                    return Utils.convertMsgToResult("sql compile finish");
                }
            } else if (sqlNode instanceof SqlCreateSqlFunction) {
                functionCompiler = new FunctionCompiler(null);
                functionCompiler.compile(sqlNode, sql);
                return Utils.convertMsgToResult("sql compile success");
            }
        } catch (Exception e) {
            functionCompiler = null;
            return Utils.convertMsgToResult("compile fcuntion error: " + e.getMessage());
        }

        return null;
    }
}
