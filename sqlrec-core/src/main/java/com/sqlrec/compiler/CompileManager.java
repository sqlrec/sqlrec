package com.sqlrec.compiler;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.utils.HiveTableUtils;
import com.sqlrec.entity.SqlApi;
import com.sqlrec.entity.SqlFunction;
import com.sqlrec.runtime.*;
import com.sqlrec.schema.HmsSchema;
import com.sqlrec.sql.parser.SqlCache;
import com.sqlrec.sql.parser.SqlCallSqlFunction;
import com.sqlrec.utils.DbUtils;
import com.sqlrec.utils.JavaFunctionUtils;
import com.sqlrec.utils.SchemaUtils;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.flink.sql.parser.ddl.SqlSet;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class CompileManager {
    private static final int UPDATE_SUCCESS = 1;
    private static final int UPDATE_FAILED = -1;
    private static final int NOT_UPDATE = 0;

    private static final Logger log = LoggerFactory.getLogger(CompileManager.class);
    private static ScheduledExecutorService executor;
    private static Map<String, SqlFunctionBindable> functionBindableMap = new ConcurrentHashMap<>();

    private static Cache<String, SqlApi> sqlApiCache = Caffeine.newBuilder()
            .expireAfterWrite(SqlRecConfigs.SCHEMA_CACHE_EXPIRE.getValue(), TimeUnit.SECONDS)
            .build();

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

    public static SqlFunctionBindable getSqlFunction(String functionName) throws Exception {
        functionName = functionName.toUpperCase();
        if (functionBindableMap.containsKey(functionName)) {
            return functionBindableMap.get(functionName);
        }
        return compileSqlFunction(functionName);
    }

    public static SqlFunctionBindable compileSqlFunction(String functionName) throws Exception {
        functionName = functionName.toUpperCase();
        SqlFunction sqlFunction = DbUtils.getSqlFunction(functionName);
        if (sqlFunction == null) {
            throw new Exception("function not fund : " + functionName);
        }
        List<String> sqlList = new Gson().fromJson(sqlFunction.getSqlList(), new TypeToken<List<String>>() {
        }.getType());
        return compileSqlFunction(functionName, sqlList);
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
        return getSqlFunction(sqlApi.getFunctionName());
    }

    public static SetBindable getSetBindable(SqlSet set) {
        SqlCharStringLiteral key = (SqlCharStringLiteral) set.getKey();
        SqlCharStringLiteral value = (SqlCharStringLiteral) set.getValue();
        return new SetBindable(SchemaUtils.getValueOfStringLiteral(key), SchemaUtils.getValueOfStringLiteral(value));
    }

    public static synchronized void initFunctionUpdateService() {
        if (executor != null) {
            return;
        }
        executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleAtFixedRate(
                () -> {
                    try {
                        updateFunctionBindable();
                    } catch (Exception e) {
                        log.error("update function bindable failed", e);
                    }
                },
                SqlRecConfigs.FUNCTION_UPDATE_INTERVAL.getValue(),
                SqlRecConfigs.FUNCTION_UPDATE_INTERVAL.getValue(),
                TimeUnit.SECONDS
        );
    }

    public static void updateFunctionBindable() {
        Map<String, Integer> functionUpdateStatusMap = new HashMap<>();
        Set<String> functionNames = new HashSet<>(functionBindableMap.keySet());
        for (String functionName : functionNames) {
            tryFlushFunctionBindable(functionName, functionUpdateStatusMap);
        }
    }

    private static int tryFlushFunctionBindable(String functionName, Map<String, Integer> functionUpdateStatusMap) {
        if (functionUpdateStatusMap.containsKey(functionName)) {
            return functionUpdateStatusMap.get(functionName);
        }

        try {
            boolean needFlush = false;
            SqlFunctionBindable functionBindable = functionBindableMap.get(functionName);
            Set<String> dependencySqlFunctions = functionBindable.getDependencySqlFunctions();
            for (String dependencySqlFunction : dependencySqlFunctions) {
                int dependencyStatus = tryFlushFunctionBindable(dependencySqlFunction, functionUpdateStatusMap);
                if (dependencyStatus == UPDATE_SUCCESS) {
                    needFlush = true;
                }
            }

            SqlFunction sqlFunction = DbUtils.getSqlFunction(functionBindable.getFunName());
            if (sqlFunction == null) {
                functionBindableMap.remove(functionName);
                functionUpdateStatusMap.put(functionName, UPDATE_SUCCESS);
                log.info("function bindable {} removed", functionName);
                return UPDATE_SUCCESS;
            }
            if (sqlFunction.getUpdatedAt() > functionBindable.getCreateTime()) {
                needFlush = true;
            }

            if (!needFlush) {
                needFlush = isSqlFunctionDependentResourceUpdate(functionBindable);
            }

            if (needFlush) {
                compileSqlFunction(functionName);
                functionUpdateStatusMap.put(functionName, UPDATE_SUCCESS);
                log.info("function bindable {} updated", functionName);
            } else {
                functionUpdateStatusMap.put(functionName, NOT_UPDATE);
            }
        } catch (Exception e) {
            log.error("try flush function bindable failed : {}", functionName, e);
            functionUpdateStatusMap.put(functionName, UPDATE_FAILED);
        }

        return functionUpdateStatusMap.get(functionName);
    }

    private static boolean isSqlFunctionDependentResourceUpdate(SqlFunctionBindable functionBindable) throws Exception {
        List<String> tablePlaceholders = new ArrayList<>();
        for (Map.Entry<String, List<RelDataTypeField>> placeholder : functionBindable.getInputTables()) {
            tablePlaceholders.add(placeholder.getKey());
        }
        Set<String> accessTables = functionBindable.getAccessTables();
        accessTables.removeAll(tablePlaceholders);

        for (String accessTable : accessTables) {
            Map.Entry<String, String> dbAndTable = HiveTableUtils.getDbAndTable(accessTable);
            long lastModifiedTime = HmsSchema.getTableUpdateTime(dbAndTable.getKey(), dbAndTable.getValue());
            if (lastModifiedTime > functionBindable.getCreateTime()) {
                return true;
            }
        }

        Set<String> javaFunctions = functionBindable.getDependencyJavaFunctions();
        for (String javaFunction : javaFunctions) {
            Object javaFunctionClass = JavaFunctionUtils.getTableFunctionClass(
                    NormalSqlCompiler.DEFAULT_SCHEMA_NAME, javaFunction);
            long functionModificationTime = JavaFunctionUtils.getFunctionUpdateTime(
                    NormalSqlCompiler.DEFAULT_SCHEMA_NAME, javaFunction);
            if (functionModificationTime > functionBindable.getCreateTime()) {
                return true;
            }
        }

        return false;
    }
}
