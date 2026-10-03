package com.sqlrec.executor;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.compiler.FunctionCompiler;
import com.sqlrec.compiler.SqlFunctionCache;
import com.sqlrec.compiler.SqlTypeChecker;
import com.sqlrec.db.MetadataAccess;
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
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlCreateTableAs;
import org.apache.flink.sql.parser.ddl.SqlDropTable;
import org.apache.flink.sql.parser.ddl.SqlAlterTable;
import org.apache.flink.sql.parser.ddl.SqlCreateDatabase;
import org.apache.flink.sql.parser.ddl.SqlAlterDatabase;
import org.apache.flink.sql.parser.ddl.SqlDropDatabase;
import org.apache.flink.sql.parser.ddl.SqlCreateFunction;
import org.apache.flink.sql.parser.ddl.SqlAlterFunction;
import org.apache.flink.sql.parser.ddl.SqlDropFunction;
import org.apache.flink.sql.parser.ddl.SqlReplaceTableAs;
import org.apache.flink.sql.parser.ddl.SqlUseCatalog;
import org.apache.flink.sql.parser.ddl.SqlUseDatabase;
import org.apache.flink.sql.parser.ddl.SqlSet;
import org.apache.flink.sql.parser.ddl.SqlReset;
import com.sqlrec.utils.SchemaUtils;
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
    private final java.util.LinkedHashMap<String, String> sessionSettings = new java.util.LinkedHashMap<>();

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

    public String getDefaultSchema() {
        return defaultSchema;
    }

    public synchronized Map<String, String> getSessionSettings() {
        return new java.util.LinkedHashMap<>(sessionSettings);
    }

    /** Called only after a forwarded RESET has completed successfully. */
    public synchronized void resetSessionSettings(String key) {
        if (key == null) {
            sessionSettings.keySet().forEach(setting -> context.setVariable(setting, null));
            sessionSettings.clear();
        } else if (sessionSettings.remove(key) != null) {
            context.setVariable(key, null);
        }
    }

    public synchronized SqlProcessResult executeSqlAsync(String sql) throws Exception {
        return executeSqlAsync(CompileManager.parseSql(sql), sql);
    }

    /** Executes a parsed statement; null means it requires the remote route. */
    public synchronized SqlProcessResult executeSqlAsync(SqlNode node, String sql) throws Exception {
        if (SqlRecConfigs.isFileSystemMetadata() && node instanceof SqlCreateSqlFunction) {
            throw new UnsupportedOperationException(
                    "SQL function DDL is not supported in local SQL file metadata mode; "
                            + "define it under SQL_SCHEMA_DIR and restart");
        }

        SqlProcessResult result = tryCompileFunction(node, sql);
        if (result != null) {
            return result;
        }
        if (node instanceof SqlUseCatalog) {
            throw new UnsupportedOperationException("USE CATALOG is not supported; SQLRec uses the configured Hive catalog");
        }
        if ((node instanceof SqlCreateTable create && create.isTemporary())
                || (node instanceof SqlReplaceTableAs replace && replace.isTemporary())) {
            throw new UnsupportedOperationException("Creating temporary tables is not supported");
        }
        if (isMetadataDdl(node)) {
            return executeMetadataDdl(node);
        }
        if (node instanceof SqlUseDatabase command) {
            return useDatabase(command);
        }
        if (node instanceof SqlSet set) {
            return setSessionVariable(set);
        }
        if (node instanceof SqlReset) {
            return null;
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
        if (result != null) {
            invalidateCaches(node);
        } else if (node instanceof com.sqlrec.sql.parser.SqlRecStatement) {
            throw new UnsupportedOperationException("SQLRec statement cannot execute locally and cannot be forwarded to Flink");
        }
        return result;
    }

    private SqlProcessResult executeMetadataDdl(SqlNode node) throws Exception {
        MetadataAccess metadata = MetadataAccessFactory.getInstance();
        try {
            metadata.executeMetadataDdl(node, defaultSchema);
        } finally {
            // A failed DDL may have changed metadata partially or lost its response after committing.
            CacheManager.invalidateAll();
        }
        return message("metadata DDL completed");
    }

    private SqlProcessResult useDatabase(SqlUseDatabase command) {
        java.util.List<String> names = command.getDatabaseName().names;
        if (names.size() > 2 || names.size() == 2 &&
                !Consts.HIVE_CATALOG_NAME.equals(names.get(0))) {
            throw new IllegalArgumentException("database catalog is not configured: " + command.getDatabaseName());
        }
        defaultSchema = names.get(names.size() - 1);
        return message("database changed to " + defaultSchema);
    }

    private SqlProcessResult setSessionVariable(SqlSet set) {
        if (set.getKey() == null || set.getValue() == null) {
            // Listing Flink's full configuration remains a Gateway operation.
            return null;
        }
        String key = SchemaUtils.getValueOfStringLiteral(set.getKey());
        String value = SchemaUtils.getValueOfStringLiteral(set.getValue());
        context.setVariable(key, value);
        sessionSettings.put(key, value);
        return message("setting saved in SQLRec session; Flink settings apply before the next remote operation");
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

    private static boolean isMetadataDdl(SqlNode node) {
        // CTAS/RTAS require an execution engine and are left to the remote route.
        if (node instanceof SqlCreateTableAs || node instanceof SqlReplaceTableAs) {
            return false;
        }
        if (node instanceof SqlCreateTable create) {
            return !create.isTemporary();
        }
        if (node instanceof SqlDropTable drop) {
            return !drop.isTemporary();
        }
        if (node instanceof SqlCreateFunction create) {
            return !create.isTemporary() && !create.isSystemFunction();
        }
        if (node instanceof SqlAlterFunction alter) {
            return !alter.isTemporary() && !alter.isSystemFunction();
        }
        if (node instanceof SqlDropFunction drop) {
            return !drop.isTemporary() && !drop.isSystemFunction();
        }
        return node instanceof SqlAlterTable || node instanceof SqlCreateDatabase
                || node instanceof SqlAlterDatabase || node instanceof SqlDropDatabase;
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
