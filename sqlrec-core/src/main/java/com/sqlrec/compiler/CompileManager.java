package com.sqlrec.compiler;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.JsonUtils;
import com.sqlrec.common.utils.ResourceNames;
import com.sqlrec.db.MetadataAccess;
import com.sqlrec.db.MetadataAccessFactory;
import com.sqlrec.entity.SqlFunction;
import com.sqlrec.runtime.*;
import com.sqlrec.sql.parser.SqlAssert;
import com.sqlrec.sql.parser.SqlCache;
import com.sqlrec.sql.parser.SqlCallSqlFunction;
import com.sqlrec.sql.parser.SqlIfCache;
import com.sqlrec.sql.parser.SqlRecSqlParser;
import com.sqlrec.sql.parser.SqlReturn;
import com.sqlrec.utils.SchemaUtils;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.sql.parser.ddl.SqlSet;

import java.util.ArrayList;
import java.util.List;

public class CompileManager {
    private final List<String> compilingSqlFunctions = new ArrayList<>();
    private final Cache<String, SqlFunctionBindable> functionCache;

    public CompileManager() {
        this(SqlFunctionCache.getCache());
    }

    CompileManager(Cache<String, SqlFunctionBindable> functionCache) {
        this.functionCache = functionCache;
    }

    public static SqlNode parseSql(String sql) throws Exception {
        return SqlRecSqlParser.parse(sql);
    }

    public static String getSqlStr(SqlNode sqlNode) {
        SqlWriterConfig config = SqlWriterConfig.of()
                .withDialect(AnsiSqlDialect.DEFAULT)
                .withIndentation(4)
                .withClauseStartsLine(true)
                .withSelectListItemsOnSeparateLines(true);
        SqlPrettyWriter writer = new SqlPrettyWriter(config);
        sqlNode.unparse(writer, 0, 0);
        return writer.toSqlString().getSql();
    }

    public BindableInterface compileSql(
            SqlNode flinkSqlNode,
            CalciteSchema schema,
            String defaultSchema,
            String originSql
    ) throws Exception {
        BindableInterface bindable = compileRawSql(flinkSqlNode, schema, defaultSchema, originSql);
        bindable.setName(StringUtils.defaultIfEmpty(
                bindable.getCacheTableName(),
                SchemaUtils.getSqlFirstWord(originSql)
        ));
        return ProxyAllBindable.wrap(bindable);
    }

    private BindableInterface compileRawSql(
            SqlNode flinkSqlNode,
            CalciteSchema schema,
            String defaultSchema,
            String originSql
    ) throws Exception {
        if (!SqlTypeChecker.isFlinkSqlCompilable(flinkSqlNode, schema, defaultSchema)) {
            throw new Exception("sql is not compilable");
        }

        BindableInterface bindable = switch (flinkSqlNode) {
            case SqlReturn sqlReturn -> getReturnBindable(sqlReturn, schema, defaultSchema);
            case SqlCallSqlFunction call -> FunctionProxyBindable.getFunctionBindable(call, schema, this);
            case SqlIfCache ifCache -> getIfBindable(ifCache, schema, defaultSchema);
            case SqlCache cache -> getCacheBindable(cache, schema, defaultSchema);
            case SqlAssert assertion -> getAssertBindable(assertion, schema, defaultSchema);
            case SqlSet set -> getSetBindable(set);
            default -> NormalSqlCompiler.getNormalSqlBindable(getSqlStr(flinkSqlNode), schema, defaultSchema);
        };
        bindable.setSql(originSql);
        return bindable;
    }

    private BindableInterface getReturnBindable(
            SqlReturn sqlReturn,
            CalciteSchema schema,
            String defaultSchema
    ) throws Exception {
        if (sqlReturn.getTableName() != null) {
            String tableName = sqlReturn.getTableName().getSimple();
            CalciteSchema.TableEntry tableEntry = schema.getTable(tableName, false);
            if (tableEntry == null) {
                throw new Exception("return table not found: " + tableName);
            }
            if (!(tableEntry.getTable() instanceof CacheTable cacheTable)) {
                throw new Exception("return table is not cache table");
            }
            return new ReturnBindable(tableName, cacheTable.getDataFields());
        }
        SqlCallSqlFunction call = sqlReturn.getCallSqlFunction();
        if (call != null) {
            if (call.isAsync()) {
                throw new Exception("async function is not supported in return statement");
            }
            return new ReturnBindable(FunctionProxyBindable.getFunctionBindable(call, schema, this));
        }
        SqlNode select = sqlReturn.getSelect();
        return new ReturnBindable(select == null ? null
                : NormalSqlCompiler.getNormalSqlBindable(getSqlStr(select), schema, defaultSchema));
    }

    private BindableInterface getCacheBindable(SqlCache cache, CalciteSchema schema, String defaultSchema) throws Exception {
        SqlCallSqlFunction callSqlFunction = cache.getCallSqlFunction();
        SqlNode select = cache.getSelect();
        BindableInterface source;
        if (callSqlFunction != null) {
            if (callSqlFunction.isAsync()) {
                throw new Exception("async function not support in cache");
            }
            source = FunctionProxyBindable.getFunctionBindable(callSqlFunction, schema, this);
        } else if (select != null) {
            source = NormalSqlCompiler.getNormalSqlBindable(getSqlStr(select), schema, defaultSchema);
        } else {
            throw new Exception("cache sql obj is invalid");
        }
        return new CacheTableBindable(cache.getTableName().getSimple(), source);
    }

    private BindableInterface getIfBindable(
            SqlIfCache ifCache,
            CalciteSchema schema,
            String defaultSchema
    ) throws Exception {
        SqlNode conditionNode = ifCache.getCondition();
        SqlNode thenClause = ifCache.getThenClause();
        SqlNode elseClause = ifCache.getElseClause();
        boolean timein = ifCache.isTimein();

        CalciteBindable conditionBindable = NormalSqlCompiler.getNormalSqlBindable(
                getSqlStr(conditionNode), schema, defaultSchema
        );

        BindableInterface thenBindable = compileIfBranch(thenClause, schema, defaultSchema);

        BindableInterface elseBindable = null;
        List<RelDataTypeField> existingCacheFields = null;
        if (elseClause != null) {
            elseBindable = compileIfBranch(elseClause, schema, defaultSchema);
        } else if (thenBindable instanceof CacheTableBindable thenCache) {
            CacheTable table = SchemaUtils.tryGetCacheTable(thenCache.getCacheTableName(), schema);
            if (table != null) {
                existingCacheFields = table.getDataFields();
            }
        }

        return new IfBindable(conditionBindable, thenBindable, elseBindable, timein, existingCacheFields);
    }

    private BindableInterface compileIfBranch(
            SqlNode branch,
            CalciteSchema schema,
            String defaultSchema
    ) throws Exception {
        // IF assembly relies on the concrete branch type for validation and cache-table handling.
        // Only the completed IF node is exposed as an executable proxy.
        return compileRawSql(branch, schema, defaultSchema, getSqlStr(branch));
    }

    private BindableInterface getAssertBindable(SqlAssert assertStmt, CalciteSchema schema, String defaultSchema) throws Exception {
        SqlNode select = assertStmt.getSelect();
        BindableInterface selectBindable = NormalSqlCompiler.getNormalSqlBindable(getSqlStr(select), schema, defaultSchema);
        return new AssertBindable(selectBindable);
    }

    public SqlFunctionBindable getSqlFunction(String functionName) throws Exception {
        String name = ResourceNames.normalize(functionName);
        SqlFunctionBindable cached = functionCache.getIfPresent(name);
        if (cached == null) {
            return compileStoredSqlFunction(name);
        }

        if (functionCache instanceof LoadingCache<String, SqlFunctionBindable> loadingCache) {
            return loadingCache.get(name);
        }
        return cached;
    }

    public static BindableInterface prepareSqlFunctionForExecution(SqlFunctionBindable sqlFunctionBindable) {
        BindableInterface executable = ProxyAllBindable.wrap(sqlFunctionBindable);
        executable.setName(sqlFunctionBindable.getFunName());
        return executable;
    }

    private SqlFunctionBindable compileStoredSqlFunction(String functionName) throws Exception {
        MetadataAccess db = MetadataAccessFactory.getInstance();
        SqlFunction sqlFunction = db.getSqlFunction(functionName);
        if (sqlFunction == null) {
            throw new Exception("function not fund : " + functionName);
        }
        List<String> sqlList = JsonUtils.parseStringList(sqlFunction.getSqlList());
        return compileAndCacheSqlFunction(functionName, sqlList);
    }

    public SqlFunctionBindable compileSqlFunction(String functionName, List<String> sqlList) throws Exception {
        return compileAndCacheSqlFunction(ResourceNames.normalize(functionName), sqlList);
    }

    private SqlFunctionBindable compileAndCacheSqlFunction(
            String functionName,
            List<String> sqlList
    ) throws Exception {
        if (compilingSqlFunctions.contains(functionName)) {
            throw new Exception("circular dependency: " + functionName + " trace: "
                    + String.join("->", compilingSqlFunctions));
        }

        compilingSqlFunctions.add(functionName);
        FunctionCompiler functionCompiler;
        try {
            functionCompiler = new FunctionCompiler(null, this);
            functionCompiler.compileAllSql(sqlList);
        } finally {
            compilingSqlFunctions.removeLast();
        }

        if (functionCompiler.isFunctionCompileFinish()) {
            SqlFunctionBindable compiled = functionCompiler.getFunctionBindable();
            if (!functionName.equals(compiled.getFunName())) {
                throw new RuntimeException("function name not match");
            }
            functionCache.put(functionName, compiled);
            return compiled;
        }
        throw new RuntimeException("function define end without return");
    }

    public static SetBindable getSetBindable(SqlSet set) {
        SqlCharStringLiteral key = (SqlCharStringLiteral) set.getKey();
        SqlCharStringLiteral value = (SqlCharStringLiteral) set.getValue();
        return new SetBindable(SchemaUtils.getValueOfStringLiteral(key), SchemaUtils.getValueOfStringLiteral(value));
    }
}
