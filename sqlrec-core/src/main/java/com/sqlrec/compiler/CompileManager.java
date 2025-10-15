package com.sqlrec.compiler;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.sqlrec.entity.SqlApi;
import com.sqlrec.entity.SqlFunction;
import com.sqlrec.runtime.*;
import com.sqlrec.sql.parser.SqlCache;
import com.sqlrec.sql.parser.SqlCallSqlFunction;
import com.sqlrec.utils.DbUtils;
import com.sqlrec.utils.SchemaUtils;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.sql.parser.ddl.SqlSet;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CompileManager {
    private static Map<String, SqlFunctionBindable> functionBindableMap = new ConcurrentHashMap<>();

    public static SqlNode parseFlinkSql(String sql) throws Exception {
        sql = SqlPreProcesser.preProcessSql(sql);

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
            return getCallSqlFunctionBindable((SqlCallSqlFunction) flinkSqlNode, schema, false);
        }
        if (flinkSqlNode instanceof SqlCache) {
            return getCacheBindable((SqlCache) flinkSqlNode, schema, defaultSchema);
        }
        if (flinkSqlNode instanceof SqlSet) {
            return getSetBindable((SqlSet) flinkSqlNode);
        }

        return getNormalSqlBindable(getSqlStr(flinkSqlNode), schema, defaultSchema);
    }

    private static BindableInterface getCallSqlFunctionBindable(
            SqlCallSqlFunction callSqlFunction,
            CalciteSchema schema,
            boolean needReturnSchema
    ) throws Exception {
        return FunctionProxyBindable.getFunctionBindable(callSqlFunction, schema, needReturnSchema);
    }

    private static BindableInterface getCacheBindable(SqlCache cache, CalciteSchema schema, String defaultSchema) throws Exception {
        String tableName = cache.getTableName().getSimple();
        String createSql = getSqlStr(cache);

        SqlCallSqlFunction callSqlFunction = cache.getCallSqlFunction();
        if (callSqlFunction != null) {
            BindableInterface bindableInterface = getCallSqlFunctionBindable(callSqlFunction, schema, true);
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

    public static SqlFunctionBindable compileSqlFunction(String functionName) throws Exception {
        functionName = functionName.toUpperCase();
        if (functionBindableMap.containsKey(functionName)) {
            return functionBindableMap.get(functionName);
        }
        SqlFunction sqlFunction = DbUtils.getSqlFunction(functionName);
        if (sqlFunction == null) {
            return null;
        }
        List<String> sqlList = new Gson().fromJson(sqlFunction.getSqlList(), new TypeToken<List<String>>() {
        }.getType());
        SqlFunctionBindable sqlFunctionBindable = compileSqlFunction(functionName, sqlList);
        return sqlFunctionBindable;
    }

    public static SqlFunctionBindable compileSqlFunction(String functionName, List<String> sqlList) throws Exception {
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

    public static SqlFunctionBindable getApiBindSqlFunction(String apiName) throws Exception {
        SqlApi sqlApi = DbUtils.getSqlApi(apiName);
        if (sqlApi == null || StringUtils.isAllEmpty(sqlApi.getFunctionName())) {
            throw new Exception("api not fund : " + apiName);
        }
        return compileSqlFunction(sqlApi.getFunctionName());
    }

    public static SetBindable getSetBindable(SqlSet set) {
        SqlCharStringLiteral key = (SqlCharStringLiteral) set.getKey();
        SqlCharStringLiteral value = (SqlCharStringLiteral) set.getValue();
        return new SetBindable(SchemaUtils.getValueOfStringLiteral(key), SchemaUtils.getValueOfStringLiteral(value));
    }
}
