package com.sqlrec.frontend.common;

import com.google.common.collect.ImmutableList;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.schema.ExecuteContext;
import com.sqlrec.common.utils.JsonUtils;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.compiler.FunctionCompiler;
import com.sqlrec.compiler.SqlTypeChecker;
import com.sqlrec.entity.Checkpoint;
import com.sqlrec.entity.Model;
import com.sqlrec.entity.SqlApi;
import com.sqlrec.entity.SqlFunction;
import com.sqlrec.entity.Service;
import com.sqlrec.frontend.service.Utils;
import com.sqlrec.model.ModelManager;
import com.sqlrec.model.common.ModelConfig;
import com.sqlrec.runtime.BindableInterface;
import com.sqlrec.runtime.ExecuteContextImpl;
import com.sqlrec.schema.HmsClient;
import com.sqlrec.schema.HmsSchema;
import com.sqlrec.sql.parser.*;
import com.sqlrec.utils.Const;
import com.sqlrec.utils.DbUtils;
import com.sqlrec.utils.SchemaUtils;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.sql.parser.ddl.SqlSet;
import org.apache.flink.sql.parser.ddl.SqlUseDatabase;
import org.apache.flink.sql.parser.dql.SqlRichDescribeTable;
import org.apache.flink.sql.parser.dql.SqlShowCreateTable;
import org.apache.flink.sql.parser.dql.SqlShowTables;
import org.apache.hive.service.rpc.thrift.THandleIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class SqlProcessor {
    private static Logger logger = LoggerFactory.getLogger(SqlProcessor.class);

    private CalciteSchema schema;
    private ExecuteContext context;
    private String defaultSchema;
    private FunctionCompiler functionCompiler;
    private Map<THandleIdentifier, SqlProcessResult> sqlProcessorMap;

    public SqlProcessor() {
        schema = HmsSchema.getHmsCalciteSchema();
        context = new ExecuteContextImpl();
        defaultSchema = Const.DEFAULT_SCHEMA_NAME;
        sqlProcessorMap = new ConcurrentHashMap<>();
    }

    public void setExecuteParams(Map<String, String> params) {
        if (params != null) {
            params.forEach(context::setVariable);
        }
    }

    public SqlProcessResult getProcessResult(THandleIdentifier handleIdentifier) {
        return sqlProcessorMap.getOrDefault(handleIdentifier, null);
    }

    public void closeProcessResult(THandleIdentifier handleIdentifier) {
        sqlProcessorMap.remove(handleIdentifier);
    }

    public SqlProcessResult tryExecuteSql(String sql) {
        SqlProcessResult result = null;
        try {
            result = executeSql(sql);
        } catch (Exception e) {
            String stackTrace = ExceptionUtils.getStackTrace(e);
            result = Utils.convertMsgToResult("process sql error: " + stackTrace, "error");
            result.msg = "process sql error: " + e.getMessage() + " stack trace: " + stackTrace;
            result.exception = e;
        }
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
            SqlProcessor.saveSqlApi((SqlCreateApi) sqlNode);
            return Utils.convertMsgToResult("create api success", "msg");
        }

        if (sqlNode instanceof SqlCreateModel) {
            SqlCreateModel createModel = (SqlCreateModel) sqlNode;
            ModelConfig model = ModelManager.getAndCheckModel(createModel);
            saveModel(createModel);
            return Utils.convertMsgToResult("create model success", "msg");
        }

        if (sqlNode instanceof SqlTrainModel) {
            SqlTrainModel trainModel = (SqlTrainModel) sqlNode;
            ModelManager.trainModel(trainModel, defaultSchema);
            return Utils.convertMsgToResult("train model success", "msg");
        }

        if (sqlNode instanceof SqlExportModel) {
            SqlExportModel exportModel = (SqlExportModel) sqlNode;
            ModelManager.exportModel(exportModel, defaultSchema);
            return Utils.convertMsgToResult("export model success", "msg");
        }

        if (sqlNode instanceof SqlDropModel) {
            SqlDropModel dropModel = (SqlDropModel) sqlNode;
            String modelName = dropModel.getModelName().getSimple();
            DbUtils.deleteModel(modelName);
            return Utils.convertMsgToResult("drop model success", "msg");
        }

        if (sqlNode instanceof SqlCreateService) {
            SqlCreateService createService = (SqlCreateService) sqlNode;
            ModelManager.createService(createService);
            return Utils.convertMsgToResult("create service success", "msg");
        }

        if (sqlNode instanceof SqlDropService) {
            SqlDropService dropService = (SqlDropService) sqlNode;
            ModelManager.deleteService(dropService.getServiceName().getSimple());
            return Utils.convertMsgToResult("drop service success", "msg");
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
            BindableInterface bindableInterface = new CompileManager().compileSql(sqlNode, schema, defaultSchema);
            Enumerable<Object[]> enumerable = bindableInterface.bind(schema, context);
            // set statement should also execute on sql gateway
            if (sqlNode instanceof SqlSet) {
                return null;
            }
            if (enumerable == null) {
                return Utils.convertMsgToResult("sql run success without output", "msg");
            }
            List<RelDataTypeField> fields = bindableInterface.getReturnDataFields();
            return Utils.convertEnumerableToTRowSet(enumerable, fields);
        }

        return null;
    }

    private SqlProcessResult tryCompileFunction(SqlNode sqlNode, String sql) throws Exception {
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
                functionCompiler = new FunctionCompiler(null, null);
                functionCompiler.compile(sqlNode, sql);
                return Utils.convertMsgToResult("start compile function", "msg");
            }
        } catch (Exception e) {
            functionCompiler = null;
            logger.error("compile fcuntion error: " + e.getMessage(), e);
            throw e;
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
            if (defaultSchema.equalsIgnoreCase(db)) {
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
                if (tableEntry != null && tableEntry.getTable() != null) {
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
                if (tableEntry != null && tableEntry.getTable() != null) {
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
            List<String> sqlList = JsonUtils.parseStringList(sqlFunction.getSqlList());
            return Utils.convertMsgToResult(String.join(";\n\n", sqlList) + ";", "create sql");
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

        if (sqlNode instanceof SqlShowModel) {
            List<Model> models = DbUtils.getModelList();
            return Utils.convertStringListToResult(
                    models.stream().map(Model::getName).collect(Collectors.toList()),
                    "model"
            );
        }

        if (sqlNode instanceof SqlShowCreateModel) {
            SqlShowCreateModel showCreateModel = (SqlShowCreateModel) sqlNode;
            if (showCreateModel.hasCheckpoint()) {
                String checkpointName = SchemaUtils.removeQuotes(showCreateModel.getCheckpoint().toString());
                Checkpoint checkpoint = DbUtils.getCheckpoint(
                        showCreateModel.getModelName().getSimple(),
                        checkpointName
                );
                if (checkpoint == null) {
                    return Utils.convertMsgToResult(
                            "checkpoint not exists: " + checkpointName + " for model " + showCreateModel.getModelName().getSimple(),
                            "error"
                    );
                }
                return Utils.convertMsgToResult(checkpoint.getDdl(), "create sql");
            } else {
                Model model = DbUtils.getModel(showCreateModel.getModelName().getSimple());
                if (model == null) {
                    return Utils.convertMsgToResult(
                            "model not exists: " + showCreateModel.getModelName().getSimple(),
                            "error"
                    );
                }
                return Utils.convertMsgToResult(model.getDdl(), "create sql");
            }
        }

        if (sqlNode instanceof SqlShowCheckpoint) {
            SqlShowCheckpoint showCheckpoint = (SqlShowCheckpoint) sqlNode;
            List<Checkpoint> checkpoints = DbUtils.getCheckpointListByModelName(showCheckpoint.getModelName().getSimple());
            return Utils.convertStringListToResult(
                    checkpoints.stream().map(Checkpoint::getCheckpointName).collect(Collectors.toList()),
                    "checkpoint"
            );
        }

        if (sqlNode instanceof SqlShowService) {
            List<Service> services = DbUtils.getServiceList();
            return Utils.convertStringListToResult(
                    services.stream().map(Service::getName).collect(Collectors.toList()),
                    "service"
            );
        }

        if (sqlNode instanceof SqlShowCreateService) {
            SqlShowCreateService showCreateService = (SqlShowCreateService) sqlNode;
            Service service = DbUtils.getService(showCreateService.getServiceName().getSimple());
            if (service == null) {
                return Utils.convertMsgToResult(
                        "service not exists: " + showCreateService.getServiceName().getSimple(),
                        "error"
                );
            }
            return Utils.convertMsgToResult(service.getDdl(), "create sql");
        }

        return null;
    }

    public static void saveSqlFunction(FunctionCompiler compiler) {
        SqlFunction sqlFunction = new SqlFunction();
        sqlFunction.setName(compiler.getFunctionBindable().getFunName());
        sqlFunction.setSqlList(JsonUtils.toJson(compiler.getSqlList()));
        sqlFunction.setCreatedAt(System.currentTimeMillis());
        sqlFunction.setUpdatedAt(System.currentTimeMillis());
        if (compiler.isOrReplace()) {
            DbUtils.upsertSqlFunction(sqlFunction);
        } else {
            DbUtils.insertSqlFunction(sqlFunction);
        }
    }

    public static void saveSqlApi(SqlCreateApi api) {
        SqlApi sqlApi = new SqlApi();
        sqlApi.setName(api.getApiName());
        sqlApi.setFunctionName(api.getFuncName());
        sqlApi.setCreatedAt(System.currentTimeMillis());
        sqlApi.setUpdatedAt(System.currentTimeMillis());
        if (api.isOrReplace()) {
            DbUtils.upsertSqlApi(sqlApi);
        } else {
            DbUtils.insertSqlApi(sqlApi);
        }
    }

    public static void saveModel(SqlCreateModel sqlCreateModel) {
        Model model = new Model();
        model.setName(sqlCreateModel.getModelName().getSimple());
        model.setDdl(sqlCreateModel.toString());
        model.setCreatedAt(System.currentTimeMillis());
        model.setUpdatedAt(System.currentTimeMillis());
        if (sqlCreateModel.isIfNotExists()) {
            DbUtils.insertModel(model);
        } else {
            DbUtils.upsertModel(model);
        }
    }
}
