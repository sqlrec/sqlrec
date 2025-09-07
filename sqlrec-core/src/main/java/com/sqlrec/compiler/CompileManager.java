package com.sqlrec.compiler;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.sqlrec.common.schema.TableFunction;
import com.sqlrec.entity.SqlApi;
import com.sqlrec.entity.SqlFunction;
import com.sqlrec.runtime.*;
import com.sqlrec.sql.parser.SqlCache;
import com.sqlrec.sql.parser.SqlCallSqlFunction;
import com.sqlrec.utils.DbUtils;
import com.sqlrec.utils.TableFunctionUtils;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class CompileManager {
    private static Map<String, FunctionBindable> functionBindableMap = new ConcurrentHashMap<>();

    public static SqlNode parseFlinkSql(String sql) throws Exception {
        SqlParser.Config parserConfig = SqlParser.config()
                .withConformance(FlinkSqlConformance.DEFAULT)
                .withParserFactory(FlinkSqlParserImpl.FACTORY)
                .withLex(Lex.JAVA);
        SqlParser parser = SqlParser.create(sql, parserConfig);
        return parser.parseQuery();
    }

    public static BindableInterface compileSql(SqlNode flinkSqlNode, CalciteSchema schema, String defaultSchema) throws Exception {
        if (!SqlTypeChecker.isFlinkSqlCompilable(flinkSqlNode, schema, defaultSchema)) {
            throw new Exception("sql is not compilable");
        }

        if (flinkSqlNode instanceof SqlCallSqlFunction) {
            return getCallSqlFunctionBindable((SqlCallSqlFunction) flinkSqlNode, schema);
        }
        if (flinkSqlNode instanceof SqlCache) {
            return getCacheBindable((SqlCache) flinkSqlNode, schema, defaultSchema);
        }

        return getNormalSqlBindable(getSqlStr(flinkSqlNode), schema, defaultSchema);
    }

    private static BindableInterface getCallSqlFunctionBindable(SqlCallSqlFunction callSqlFunction, CalciteSchema schema) throws Exception {
        String functionName = callSqlFunction.getFuncName().getSimple();
        List<String> inputTableList = callSqlFunction.getInputTableList()
                .stream()
                .map(SqlIdentifier::getSimple)
                .collect(Collectors.toList());

        // todo check is function name ambiguous
        TableFunction tableFunction = TableFunctionUtils.getTableFunction(NormalSqlCompiler.DEFAULT_SCHEMA_NAME, functionName);
        if (tableFunction != null) {
            if (inputTableList.size() != 1) {
                throw new Exception("table function only support one table input for " + functionName);
            }
            return new TableFunctionBindable(tableFunction, inputTableList.get(0), schema);
        }

        FunctionBindable sqlFunctionBindable = compileSqlFunction(functionName);
        if (sqlFunctionBindable != null) {
            if (sqlFunctionBindable.getInputTables().size() != inputTableList.size()) {
                throw new Exception("function input table not match");
            }
            return new CallFunctionBindable(functionName, inputTableList, sqlFunctionBindable);
        }

        throw new Exception("function not find: " + functionName);
    }

    private static BindableInterface getCacheBindable(SqlCache cache, CalciteSchema schema, String defaultSchema) throws Exception {
        String tableName = cache.getTableName().getSimple();
        String createSql = getSqlStr(cache);

        SqlCallSqlFunction callSqlFunction = cache.getCallSqlFunction();
        if (callSqlFunction != null) {
            BindableInterface bindableInterface = getCallSqlFunctionBindable(callSqlFunction, schema);
            if (bindableInterface.getReturnDataFields() == null) {
                throw new Exception("function without return table is not cacheable: " + callSqlFunction.getFuncName());
            }
            return new CacheTableBindable(tableName, bindableInterface, createSql);
        }

        SqlSelect select = cache.getSelect();
        if (select != null) {
            BindableInterface bindableInterface = getNormalSqlBindable(getSqlStr(select), schema, defaultSchema);
            return new CacheTableBindable(tableName, bindableInterface, createSql);
        }

        throw new Exception("cache sql obj is invalid");
    }

    private static BindableInterface getNormalSqlBindable(String sqlStr, CalciteSchema schema, String defaultSchema) throws Exception {
        return NormalSqlCompiler.getNormalSqlBindable(sqlStr, schema, defaultSchema);
    }

    private static String getSqlStr(SqlNode sqlNode) {
        return sqlNode.toSqlString(AnsiSqlDialect.DEFAULT).getSql();
    }

    public static FunctionBindable compileSqlFunction(String functionName) throws Exception {
        functionName = functionName.toUpperCase();
        if (functionBindableMap.containsKey(functionName)) {
            return functionBindableMap.get(functionName);
        }
        SqlFunction sqlFunction = DbUtils.getSqlFunction(functionName);
        if (sqlFunction == null) {
            return null;
        }
        List<String> sqlList = new Gson().fromJson(sqlFunction.getSqlList(), new TypeToken<List<String>>() {}.getType());
        FunctionBindable functionBindable = compileSqlFunction(functionName, sqlList);
        return functionBindable;
    }

    public static FunctionBindable compileSqlFunction(String functionName, List<String> sqlList) throws Exception {
        functionName = functionName.toUpperCase();
        FunctionCompiler functionCompiler = new FunctionCompiler(null);
        functionCompiler.compileAllSql(sqlList);
        if (functionCompiler.isFunctionCompileFinish()) {
            if (!functionName.equalsIgnoreCase(functionCompiler.getFunctionBindable().getFunName())) {
                throw new RuntimeException("function name not match");
            }
            functionBindableMap.put(functionName, functionCompiler.getFunctionBindable());
            return functionCompiler.getFunctionBindable();
        }
        throw new RuntimeException("function compile failed");
    }

    public static FunctionBindable getApiBindSqlFunction(String apiName) throws Exception {
        SqlApi sqlApi = DbUtils.getSqlApi(apiName);
        if (sqlApi == null || StringUtils.isAllEmpty(sqlApi.getFunctionName())) {
            throw new Exception("api not fund : " + apiName);
        }
        return compileSqlFunction(sqlApi.getFunctionName());
    }
}
