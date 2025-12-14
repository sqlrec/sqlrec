package com.sqlrec.compiler;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.utils.JsonUtils;
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
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.flink.sql.parser.ddl.SqlSet;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class CompileManager {
    private static final Logger log = LoggerFactory.getLogger(CompileManager.class);

    private static Map<String, SqlFunctionBindable> functionBindableMap = new ConcurrentHashMap<>();
    private static Cache<String, SqlApi> sqlApiCache = Caffeine.newBuilder()
            .expireAfterWrite(SqlRecConfigs.SCHEMA_CACHE_EXPIRE.getValue(), TimeUnit.SECONDS)
            .build();

    private List<String> compilingSqlFunctions = new ArrayList<>();

    public CompileManager() {

    }

    public static SqlNode parseFlinkSql(String sql) throws Exception {
        sql = SqlPreProcesser.preProcessSql(sql);

        SqlParser.Config parserConfig = SqlParser.config()
                .withConformance(FlinkSqlConformance.DEFAULT)
                .withParserFactory(FlinkSqlParserImpl.FACTORY)
                .withLex(Lex.JAVA);
        SqlParser parser = SqlParser.create(sql, parserConfig);
        return parser.parseQuery();
    }

    public BindableInterface compileSql(SqlNode flinkSqlNode, CalciteSchema schema, String defaultSchema) throws Exception {
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

    private BindableInterface getCallSqlFunctionBindable(
            SqlCallSqlFunction callSqlFunction,
            CalciteSchema schema,
            boolean needReturnSchema
    ) throws Exception {
        return FunctionProxyBindable.getFunctionBindable(callSqlFunction, schema, needReturnSchema, this);
    }

    private BindableInterface getCacheBindable(SqlCache cache, CalciteSchema schema, String defaultSchema) throws Exception {
        String tableName = cache.getTableName().getSimple();
        String createSql = getSqlStr(cache);

        SqlCallSqlFunction callSqlFunction = cache.getCallSqlFunction();
        if (callSqlFunction != null) {
            if (callSqlFunction.isAsync()) {
                throw new Exception("async function not support in cache");
            }
            BindableInterface bindableInterface = getCallSqlFunctionBindable(callSqlFunction, schema, true);
            return new CacheTableBindable(tableName, bindableInterface, createSql);
        }

        SqlNode select = cache.getSelect();
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

    public SqlFunctionBindable getSqlFunction(String functionName) throws Exception {
        functionName = functionName.toUpperCase();
        if (functionBindableMap.containsKey(functionName)) {
            return functionBindableMap.get(functionName);
        }
        return compileSqlFunction(functionName);
    }

    public SqlFunctionBindable compileSqlFunction(String functionName) throws Exception {
        functionName = functionName.toUpperCase();
        SqlFunction sqlFunction = DbUtils.getSqlFunction(functionName);
        if (sqlFunction == null) {
            throw new Exception("function not fund : " + functionName);
        }
        List<String> sqlList = JsonUtils.parseStringList(sqlFunction.getSqlList());
        return compileSqlFunction(functionName, sqlList);
    }

    public SqlFunctionBindable compileSqlFunction(String functionName, List<String> sqlList) throws Exception {
        functionName = functionName.toUpperCase();
        if (compilingSqlFunctions.contains(functionName)) {
            throw new Exception("circular dependency: " + functionName + " trace: "
                    + String.join("->", compilingSqlFunctions));
        }

        compilingSqlFunctions.add(functionName);
        FunctionCompiler functionCompiler = new FunctionCompiler(null, this);
        functionCompiler.compileAllSql(sqlList);
        compilingSqlFunctions.removeLast();

        if (functionCompiler.isFunctionCompileFinish()) {
            if (!functionName.equalsIgnoreCase(functionCompiler.getFunctionBindable().getFunName())) {
                throw new RuntimeException("function name not match");
            }
            functionBindableMap.put(functionName, functionCompiler.getFunctionBindable());
            return functionCompiler.getFunctionBindable();
        }
        throw new RuntimeException("function define end without return");
    }

    public static SqlFunctionBindable getApiBindSqlFunction(String apiName) throws Exception {
        SqlApi sqlApi = sqlApiCache.getIfPresent(apiName);
        if (sqlApi == null) {
            sqlApi = DbUtils.getSqlApi(apiName);
            if (sqlApi != null) {
                sqlApiCache.put(apiName, sqlApi);
            }
        }

        if (sqlApi == null) {
            throw new Exception("api not fund : " + apiName);
        }
        return new CompileManager().getSqlFunction(sqlApi.getFunctionName());
    }

    public static SetBindable getSetBindable(SqlSet set) {
        SqlCharStringLiteral key = (SqlCharStringLiteral) set.getKey();
        SqlCharStringLiteral value = (SqlCharStringLiteral) set.getValue();
        return new SetBindable(SchemaUtils.getValueOfStringLiteral(key), SchemaUtils.getValueOfStringLiteral(value));
    }

    public static Map<String, SqlFunctionBindable> getFunctionBindableMap() {
        return functionBindableMap;
    }
}
