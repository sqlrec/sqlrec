package com.sqlrec.executor;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.compiler.FunctionCompiler;
import com.sqlrec.compiler.SqlFunctionCache;
import com.sqlrec.compiler.SqlTypeChecker;
import com.sqlrec.db.MetadataAccessFactory;
import com.sqlrec.runtime.BindableInterface;
import com.sqlrec.runtime.ExecuteContextImpl;
import com.sqlrec.schema.CacheManager;
import com.sqlrec.schema.CalciteSchemaFactory;
import com.sqlrec.sql.parser.SqlCreateApi;
import com.sqlrec.sql.parser.SqlCreateSqlFunction;
import com.sqlrec.sql.parser.SqlDropSqlFunction;
import com.sqlrec.sql.parser.SqlFlush;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.sql.parser.ddl.SqlUseDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/** Coordinates parsing, routing and execution of one SQL session. */
public class SqlExecutor {
    private static final Logger logger = LoggerFactory.getLogger(SqlExecutor.class);
    private static final String MESSAGE_FIELD = "msg";

    private final CalciteSchema schema;
    private final ExecuteContext context;
    private String defaultSchema;
    private FunctionCompiler functionCompiler;

    public SqlExecutor() {
        schema = CalciteSchemaFactory.createCalciteSchema();
        context = new ExecuteContextImpl();
        defaultSchema = Consts.DEFAULT_SCHEMA_NAME;
    }

    public void setExecuteParams(Map<String, String> params) {
        if (params != null) {
            params.forEach(context::setVariable);
        }
    }

    public ExecuteContext getExecuteContext() {
        return context;
    }

    public SqlProcessResult executeSqlAsync(String sql) throws Exception {
        SqlNode node = CompileManager.parseSql(sql);

        if (SqlRecConfigs.isFileSystemMetadata() && node instanceof SqlCreateSqlFunction) {
            throw new UnsupportedOperationException(
                    "SQL function DDL is not supported in local SQL file metadata mode; "
                            + "define it under SQL_SCHEMA_DIR and restart");
        }

        SqlProcessResult result = tryCompileFunction(node, sql);
        if (result != null) {
            return result;
        }
        if (node instanceof SqlUseDatabase command) {
            defaultSchema = command.getDatabaseName().getSimple();
            return message("database changed to " + defaultSchema);
        }
        if (node instanceof SqlFlush) {
            CacheManager.invalidateAll();
            return message("all caches flushed");
        }

        result = new ResourceQueryExecutor(MetadataAccessFactory.getInstance(), schema)
                .execute(node, defaultSchema);
        if (result != null) {
            return result;
        }
        if (SqlTypeChecker.isFlinkSqlCompilable(node, schema, defaultSchema)) {
            return compileAndExecute(node, sql);
        }
        if (SqlRecConfigs.isFileSystemMetadata()) {
            throw new RuntimeException("sql can not exec in filesystem meta");
        }

        result = new ResourceCommandExecutor(MetadataAccessFactory.getInstance())
                .execute(node, defaultSchema);
        invalidateCaches(node);
        return result;
    }

    public CacheTable executeSql(String sql) throws Exception {
        SqlProcessResult result = executeSqlAsync(sql);
        if (result == null) {
            throw new RuntimeException("cannot exec sql: " + sql);
        }
        if (!result.isCompleted()) {
            waitForCompletion(result, sql);
        }
        return new CacheTable("result", result.getEnumerable(), result.getFields());
    }

    private SqlProcessResult compileAndExecute(SqlNode node, String sql) throws Exception {
        BindableInterface bindable = new CompileManager()
                .compileSql(node, schema, defaultSchema, sql);
        Enumerable<Object[]> rows = bindable.bind(schema, context);
        return rows == null
                ? message("sql run success without output")
                : SqlProcessResult.of(rows, bindable.getReturnDataFields());
    }

    private SqlProcessResult tryCompileFunction(SqlNode node, String sql) throws Exception {
        try {
            if (functionCompiler != null) {
                functionCompiler.compile(node, sql);
                if (!functionCompiler.isFunctionCompileFinish()) {
                    return message("add a sql to function");
                }
                saveSqlFunction(functionCompiler);
                functionCompiler = null;
                return message("function compile success");
            }
            if (node instanceof SqlCreateSqlFunction) {
                functionCompiler = new FunctionCompiler(null, null);
                functionCompiler.compile(node, sql);
                return message("start compile function");
            }
            return null;
        } catch (Exception exception) {
            functionCompiler = null;
            logger.error("compile function error: " + exception.getMessage(), exception);
            throw exception;
        }
    }

    private static void invalidateCaches(SqlNode node) {
        if (node instanceof SqlDropSqlFunction) {
            SqlFunctionCache.invalidateAll();
        } else {
            CacheManager.invalidateAll();
        }
    }

    private static void waitForCompletion(SqlProcessResult result, String sql)
            throws InterruptedException {
        long timeout = SqlRecConfigs.SQL_SYNC_EXECUTE_TIMEOUT.getValue();
        long start = System.currentTimeMillis();
        logger.info("executeSql start, timeout: {}ms, sql: {}", timeout, sql);
        while (!result.isCompleted()) {
            long elapsed = System.currentTimeMillis() - start;
            if (elapsed > timeout) {
                throw new RuntimeException("sql execution timeout after " + timeout + "ms");
            }
            logger.info("executeSql duration {}ms, sql: {}", elapsed, sql);
            Thread.sleep(1000);
        }
        logger.info("executeSql completed in {}ms, sql: {}",
                System.currentTimeMillis() - start, sql);
    }

    private static SqlProcessResult message(String text) {
        return SqlProcessResult.msg(text, MESSAGE_FIELD);
    }

    public static void saveSqlFunction(FunctionCompiler compiler) {
        ResourceCommandExecutor.saveSqlFunction(
                MetadataAccessFactory.getInstance(), compiler);
    }

    public static void saveSqlApi(SqlCreateApi api) {
        ResourceCommandExecutor.saveSqlApi(MetadataAccessFactory.getInstance(), api);
    }
}
