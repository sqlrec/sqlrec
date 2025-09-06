package com.sqlrec.frontend.service;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.compiler.FunctionCompiler;
import com.sqlrec.compiler.NormalSqlCompiler;
import com.sqlrec.compiler.SqlTypeChecker;
import com.sqlrec.entity.SqlApi;
import com.sqlrec.entity.SqlFunction;
import com.sqlrec.runtime.BindableInterface;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.schema.HmsClient;
import com.sqlrec.schema.HmsSchema;
import com.sqlrec.sql.parser.*;
import com.sqlrec.utils.DbUtils;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.sql.parser.ddl.SqlUseDatabase;
import org.apache.flink.sql.parser.dql.SqlRichDescribeTable;
import org.apache.flink.sql.parser.dql.SqlShowCreateTable;
import org.apache.flink.sql.parser.dql.SqlShowTables;
import org.apache.hive.service.rpc.thrift.THandleIdentifier;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

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

    public SqlProcessResult tryExecuteSql(String sql) {
        SqlProcessResult result = null;
        try {
            result = executeSql(sql);
        } catch (Exception e) {
            String stackTrace = ExceptionUtils.getStackTrace(e);
            result = Utils.convertMsgToResult("process sql error: " + stackTrace, "error");
            result.exception = e;
        }
        if (result != null) {
            sqlProcessorMap.put(result.handleIdentifier, result);
        }
        return result;
    }

    private SqlProcessResult executeSql(String sql) throws Exception {
        sql = preProcessSql(sql);
        SqlNode sqlNode = CompileManager.parseFlinkSql(sql);

        SqlProcessResult result = tryCompileFunction(sqlNode, sql);
        if (result != null) {
            return result;
        }

        if (sqlNode instanceof SqlCreateApi) {
            SqlProcessor.saveSqlApi((SqlCreateApi) sqlNode);
            return Utils.convertMsgToResult("create api success", "msg");
        }

        if (sqlNode instanceof SqlUseDatabase) {
            defaultSchema = ((SqlUseDatabase) sqlNode).getDatabaseName().getSimple();
            return null;
        }

        result = processTableSchemaQuery(sqlNode);
        if (result != null) {
            return result;
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
                    SqlProcessor.saveSqlFunction(functionCompiler);
                    functionCompiler = null;
                    return Utils.convertMsgToResult("function compile success", "msg");
                } else {
                    return Utils.convertMsgToResult("add a sql to function", "msg");
                }
            } else if (sqlNode instanceof SqlCreateSqlFunction) {
                functionCompiler = new FunctionCompiler(null);
                functionCompiler.compile(sqlNode, sql);
                return Utils.convertMsgToResult("start compile function", "msg");
            }
        } catch (Exception e) {
            functionCompiler = null;
            return Utils.convertMsgToResult("compile fcuntion error: " + e.getMessage(), "error");
        }

        return null;
    }

    private SqlProcessResult processTableSchemaQuery(SqlNode sqlNode) throws Exception {
        if (sqlNode instanceof SqlShowTables) {
            String db = defaultSchema;
            String[] dbInSql = ((SqlShowTables) sqlNode).fullDatabaseName();
            if (dbInSql.length > 0) {
                db = dbInSql[0];
            }

            CalciteSchema subSchema = schema.getSubSchema(db, false);
            if (subSchema == null) {
                return Utils.convertMsgToResult("database not exists: " + db, "error");
            }

            List<String> tableNames = HmsClient.getAllTables(db);
            if (defaultSchema.equalsIgnoreCase(db)){
                tableNames.addAll(schema.getTableNames());
            }
            tableNames = tableNames.stream().distinct().collect(Collectors.toList());
            return Utils.convertStringListToResult(tableNames, "table name");
        }

        if (sqlNode instanceof SqlRichDescribeTable) {
            String[] fullTableName = ((SqlRichDescribeTable) sqlNode).fullTableName();
            String db = defaultSchema;
            String table = fullTableName[fullTableName.length - 1];
            if (fullTableName.length > 1) {
                db = fullTableName[0];
            }

            if (defaultSchema.equalsIgnoreCase(db)) {
                CalciteSchema.TableEntry tableEntry = schema.getTable(table, false);
                if (tableEntry != null && tableEntry.getTable()!=null) {
                    Table tableObj = tableEntry.getTable();
                    if (tableObj instanceof CacheTable) {
                        List<RelDataTypeField> dataFields = ((CacheTable) tableObj).getDataFields();
                        return Utils.getTableTypeDescResult(dataFields);
                    }
                }
            }
        }

        if (sqlNode instanceof SqlShowCreateTable) {
            ImmutableList<String> names = ((SqlShowCreateTable) sqlNode).getTableName().names;
            String db = defaultSchema;
            if (names.size() > 1) {
                db = names.get(0);
            }
            String table = names.get(names.size() - 1);

            if (defaultSchema.equalsIgnoreCase(db)) {
                CalciteSchema.TableEntry tableEntry = schema.getTable(table, false);
                if (tableEntry != null && tableEntry.getTable()!=null) {
                    Table tableObj = tableEntry.getTable();
                    if (tableObj instanceof CacheTable) {
                        return Utils.convertMsgToResult(((CacheTable) tableObj).getCreateSql(), "create sql");
                    }
                }
            }
        }

        if (sqlNode instanceof SqlShowSqlFunction) {
            List<SqlFunction> sqlFunctions = DbUtils.getSqlFunctionList();
            return Utils.convertStringListToResult(
                    sqlFunctions.stream().map(SqlFunction::getName).collect(Collectors.toList()),
                    "sql function"
            );
        }

        if (sqlNode instanceof SqlShowCreateSqlFunction) {
            SqlShowCreateSqlFunction showCreateSqlFunction = (SqlShowCreateSqlFunction) sqlNode;
            SqlFunction sqlFunction = DbUtils.getSqlFunction(showCreateSqlFunction.getFuncName().getSimple());
            if (sqlFunction == null) {
                return Utils.convertMsgToResult(
                        "sql function not exists: " + showCreateSqlFunction.getFuncName().getSimple(),
                        "error"
                );
            }
            List<String> sqlList = new Gson().fromJson(sqlFunction.getSqlList(), new TypeToken<List<String>>() {}.getType());
            return Utils.convertMsgToResult(String.join("\n", sqlList), "create sql");
        }

        if (sqlNode instanceof SqlShowApi) {
            List<SqlApi> sqlApis = DbUtils.getSqlApiList();
            return Utils.convertStringListToResult(
                    sqlApis.stream().map(SqlApi::getName).collect(Collectors.toList()),
                    "api"
            );
        }

        if (sqlNode instanceof SqlShowCreateApi) {
            SqlShowCreateApi showCreateApi = (SqlShowCreateApi) sqlNode;
            SqlApi sqlApi = DbUtils.getSqlApi(showCreateApi.getApiName().getSimple());
            if (sqlApi == null) {
                return Utils.convertMsgToResult("api not exists: " + showCreateApi.getApiName(), "error");
            }
            String sql = "create api " + sqlApi.getName() + " with " + sqlApi.getFunctionName();
            return Utils.convertMsgToResult(sql, "create sql");
        }

        return null;
    }

    public static String preProcessSql(String sql) {
        if(StringUtils.isEmpty(sql)) {
            return sql;
        }

        if (StringUtils.deleteWhitespace(sql).equals(StringUtils.deleteWhitespace(Constant.USE_DEFAULT))) {
            return Constant.USE_DEFAULT_FORMATTED;
        }
        if (StringUtils.deleteWhitespace(sql).equals(StringUtils.deleteWhitespace(Constant.SHOW_TABLES_FROM_DEFAULT))) {
            return Constant.SHOW_TABLES_FROM_DEFAULT_FORMATTED;
        }
        if (StringUtils.deleteWhitespace(sql).equals(StringUtils.deleteWhitespace(Constant.SHOW_TABLES_IN_DEFAULT))) {
            return Constant.SHOW_TABLES_IN_DEFAULT_FORMATTED;
        }
        return sql;
    }

    public static void saveSqlFunction(FunctionCompiler compiler) {
        SqlFunction sqlFunction = new SqlFunction();
        sqlFunction.setName(compiler.getFunctionBindable().getFunName());
        sqlFunction.setSqlList(new Gson().toJson(compiler.getSqlList()));
        DbUtils.insertSqlFunction(sqlFunction);
    }

    public static void saveSqlApi(SqlCreateApi api) {
        SqlApi sqlApi = new SqlApi();
        sqlApi.setName(api.getApiName());
        sqlApi.setFunctionName(api.getFuncName());
        DbUtils.insertSqlApi(sqlApi);
    }
}
