package com.sqlrec.executor;

import com.google.common.collect.ImmutableList;
import com.sqlrec.common.config.Consts;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.model.CheckpointInfo;
import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.schema.SqlRecTable;
import com.sqlrec.common.utils.ExecEnv;
import com.sqlrec.common.utils.JsonUtils;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.compiler.FunctionCompiler;
import com.sqlrec.compiler.SqlTypeChecker;
import com.sqlrec.db.MetadataAccess;
import com.sqlrec.db.MetadataAccessFactory;
import com.sqlrec.entity.*;
import com.sqlrec.model.ModelManager;
import com.sqlrec.model.ServiceManager;
import com.sqlrec.runtime.BindableInterface;
import com.sqlrec.runtime.ExecuteContextImpl;
import com.sqlrec.schema.CacheManager;
import com.sqlrec.schema.CalciteSchemaFactory;
import com.sqlrec.sql.parser.*;
import com.sqlrec.utils.ModelUtils;
import com.sqlrec.utils.SchemaUtils;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.sql.parser.ddl.SqlSet;
import org.apache.flink.sql.parser.ddl.SqlUseDatabase;
import org.apache.flink.sql.parser.dql.SqlRichDescribeTable;
import org.apache.flink.sql.parser.dql.SqlShowCreateTable;
import org.apache.flink.sql.parser.dql.SqlShowDatabases;
import org.apache.flink.sql.parser.dql.SqlShowTables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SqlExecutor {
    private static final Logger logger = LoggerFactory.getLogger(SqlExecutor.class);

    private CalciteSchema schema;
    private ExecuteContext context;
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
        SqlNode sqlNode = CompileManager.parseFlinkSql(sql);

        SqlProcessResult result = tryCompileFunction(sqlNode, sql);
        if (result != null) {
            return result;
        }

        if (sqlNode instanceof SqlUseDatabase) {
            defaultSchema = ((SqlUseDatabase) sqlNode).getDatabaseName().getSimple();
            return SqlProcessResult.msg("database changed to " + defaultSchema, "msg");
        }

        result = processResourceQuery(sqlNode);
        if (result != null) {
            return result;
        }

        if (SqlTypeChecker.isFlinkSqlCompilable(sqlNode, schema, defaultSchema)) {
            BindableInterface bindableInterface = new CompileManager().compileSql(sqlNode, schema, defaultSchema, sql);
            Enumerable<Object[]> enumerable = bindableInterface.bind(schema, context);
            if (enumerable == null) {
                return SqlProcessResult.msg("sql run success without output", "msg");
            }
            List<RelDataTypeField> fields = bindableInterface.getReturnDataFields();
            return SqlProcessResult.of(enumerable, fields);
        }

        if (ExecEnv.isFileSystemMeta()) {
            throw new RuntimeException("sql can not exec in filesystem meta");
        }

        CacheManager.invalidateAll();

        result = processResourceEdit(sqlNode);
        if (result != null) {
            return result;
        }

        return null;
    }

    public CacheTable executeSql(String sql) throws Exception {
        SqlProcessResult result = executeSqlAsync(sql);
        if (result == null) {
            throw new RuntimeException("cannot exec sql: " + sql);
        }
        if (result.isCompleted()) {
            return new CacheTable("result", result.getEnumerable(), result.getFields());
        }

        long timeout = SqlRecConfigs.SQL_SYNC_EXECUTE_TIMEOUT.getValue();
        long start = System.currentTimeMillis();
        logger.info("executeSql start, timeout: {}ms, sql: {}", timeout, sql);
        while (!result.isCompleted()) {
            long duration = System.currentTimeMillis() - start;
            logger.error("executeSql duration {}ms, sql: {}", timeout, sql);
            if (duration > timeout) {
                throw new RuntimeException("sql execution timeout after " + timeout + "ms");
            }
            Thread.sleep(1000);
        }
        logger.info("executeSql completed in {}ms, sql: {}", System.currentTimeMillis() - start, sql);
        return new CacheTable("result", result.getEnumerable(), result.getFields());
    }

    private SqlProcessResult tryCompileFunction(SqlNode sqlNode, String sql) throws Exception {
        try {
            if (functionCompiler != null) {
                functionCompiler.compile(sqlNode, sql);
                if (functionCompiler.isFunctionCompileFinish()) {
                    SqlExecutor.saveSqlFunction(functionCompiler);
                    functionCompiler = null;
                    return SqlProcessResult.msg("function compile success", "msg");
                } else {
                    return SqlProcessResult.msg("add a sql to function", "msg");
                }
            } else if (sqlNode instanceof SqlCreateSqlFunction) {
                functionCompiler = new FunctionCompiler(null, null);
                functionCompiler.compile(sqlNode, sql);
                return SqlProcessResult.msg("start compile function", "msg");
            }
        } catch (Exception e) {
            functionCompiler = null;
            logger.error("compile function error: " + e.getMessage(), e);
            throw e;
        }

        return null;
    }

    private SqlProcessResult processResourceEdit(SqlNode sqlNode) throws Exception {
        MetadataAccess db = MetadataAccessFactory.getInstance();
        if (sqlNode instanceof SqlCreateApi) {
            SqlExecutor.saveSqlApi((SqlCreateApi) sqlNode);
            return SqlProcessResult.msg("create api success", "msg");
        }

        if (sqlNode instanceof SqlCreateModel) {
            SqlCreateModel createModel = (SqlCreateModel) sqlNode;
            ModelManager.createModel(createModel);
            return SqlProcessResult.msg("create model success", "msg");
        }

        if (sqlNode instanceof SqlTrainModel) {
            SqlTrainModel trainModel = (SqlTrainModel) sqlNode;
            List<CheckpointInfo> checkpointInfos = ModelManager.trainModel(trainModel, defaultSchema);
            return ModelSqlProcessResult.msg("train model success", "msg", checkpointInfos);
        }

        if (sqlNode instanceof SqlExportModel) {
            SqlExportModel exportModel = (SqlExportModel) sqlNode;
            List<CheckpointInfo> checkpointInfos = ModelManager.exportModel(exportModel, defaultSchema);
            return ModelSqlProcessResult.msg("export model success", "msg", checkpointInfos);
        }

        if (sqlNode instanceof SqlDropModel) {
            SqlDropModel dropModel = (SqlDropModel) sqlNode;
            String modelName = dropModel.getModelName().getSimple();
            ModelManager.deleteModel(modelName);
            return SqlProcessResult.msg("drop model success", "msg");
        }

        if (sqlNode instanceof SqlAlterModelDropCheckpoint) {
            SqlAlterModelDropCheckpoint alterModelDropCheckpoint = (SqlAlterModelDropCheckpoint) sqlNode;
            String modelName = alterModelDropCheckpoint.getModelName().getSimple();
            String checkpointName = SchemaUtils.removeQuotes(alterModelDropCheckpoint.getCheckpointName().toString());
            Checkpoint checkpoint = db.getCheckpoint(modelName, checkpointName);
            if (checkpoint == null) {
                if (alterModelDropCheckpoint.isIfExists()) {
                    return SqlProcessResult.msg("drop checkpoint success", "msg");
                }
                throw new RuntimeException("checkpoint not exists: " + checkpointName + " for model " + modelName);
            }
            ModelManager.deleteCheckpoint(modelName, checkpointName);
            return SqlProcessResult.msg("drop checkpoint success", "msg");
        }

        if (sqlNode instanceof SqlCreateService) {
            SqlCreateService createService = (SqlCreateService) sqlNode;
            String serviceName = ServiceManager.createService(createService);
            return ServiceSqlProcessResult.msg("create service success", "msg", serviceName);
        }

        if (sqlNode instanceof SqlDropService) {
            SqlDropService dropService = (SqlDropService) sqlNode;
            ServiceManager.deleteService(dropService.getServiceName().getSimple());
            return SqlProcessResult.msg("drop service success", "msg");
        }

        if (sqlNode instanceof SqlDropSqlFunction) {
            SqlDropSqlFunction dropSqlFunction = (SqlDropSqlFunction) sqlNode;
            String funcName = dropSqlFunction.getFuncName().getSimple();
            SqlFunction sqlFunction = db.getSqlFunction(funcName);
            if (sqlFunction == null) {
                if (dropSqlFunction.isIfExists()) {
                    return SqlProcessResult.msg("drop sql function success", "msg");
                }
                throw new RuntimeException("sql function not exists: " + funcName);
            }
            List<SqlApi> sqlApis = db.getSqlApiListByFunctionName(funcName);
            if (!sqlApis.isEmpty()) {
                List<String> usingApis = sqlApis.stream()
                        .map(SqlApi::getName)
                        .collect(Collectors.toList());
                throw new RuntimeException("sql function " + funcName + " is used by api: " + String.join(", ", usingApis));
            }
            db.deleteSqlFunction(funcName);
            return SqlProcessResult.msg("drop sql function success", "msg");
        }

        if (sqlNode instanceof SqlDropApi) {
            SqlDropApi dropApi = (SqlDropApi) sqlNode;
            String apiName = dropApi.getApiName().getSimple();
            SqlApi sqlApi = db.getSqlApi(apiName);
            if (sqlApi == null) {
                if (dropApi.isIfExists()) {
                    return SqlProcessResult.msg("drop api success", "msg");
                }
                throw new RuntimeException("api not exists: " + apiName);
            }
            db.deleteSqlApi(apiName);
            return SqlProcessResult.msg("drop api success", "msg");
        }

        return null;
    }

    private SqlProcessResult processResourceQuery(SqlNode sqlNode) throws Exception {
        MetadataAccess db = MetadataAccessFactory.getInstance();
        if (sqlNode instanceof SqlShowDatabases) {
            List<String> databases = db.getDatabases();
            return SqlProcessResult.stringList(databases, "database name");
        }

        if (sqlNode instanceof SqlShowTables) {
            String dbName = defaultSchema;
            String[] dbInSql = ((SqlShowTables) sqlNode).fullDatabaseName();
            if (dbInSql.length > 0) {
                dbName = dbInSql[0];
            }

            CalciteSchema subSchema = schema.getSubSchema(dbName, false);
            if (subSchema == null) {
                throw new RuntimeException("database not exists: " + dbName);
            }

            List<String> tableNames = MetadataAccessFactory.getInstance().getTables(dbName)
                    .stream().map(org.apache.hadoop.hive.metastore.api.Table::getTableName).collect(Collectors.toList());
            if (defaultSchema.equalsIgnoreCase(dbName)) {
                tableNames.addAll(schema.getTableNames());
            }
            tableNames = tableNames.stream().distinct().collect(Collectors.toList());
            return SqlProcessResult.stringList(tableNames, "table name");
        }

        if (sqlNode instanceof SqlRichDescribeTable) {
            String[] fullTableName = ((SqlRichDescribeTable) sqlNode).fullTableName();
            String dbName = defaultSchema;
            String table = fullTableName[fullTableName.length - 1];
            if (fullTableName.length > 1) {
                dbName = fullTableName[0];
            }

            Table tableObj = SchemaUtils.getTableObj(schema, dbName, table);
            if (tableObj != null) {
                RelDataType rowType = tableObj.getRowType(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT));
                return SqlProcessResult.tableTypeDesc(rowType.getFieldList());
            }
        }

        if (sqlNode instanceof SqlShowCreateTable) {
            ImmutableList<String> names = ((SqlShowCreateTable) sqlNode).getTableName().names;
            String dbName = defaultSchema;
            if (names.size() > 1) {
                dbName = names.get(0);
            }
            String table = names.get(names.size() - 1);

            Table tableObj = SchemaUtils.getTableObj(schema, dbName, table);
            if (tableObj != null) {
                if (tableObj instanceof SqlRecTable) {
                    SqlRecTable sqlRecTable = (SqlRecTable) tableObj;
                    if (StringUtils.isNotEmpty(sqlRecTable.getCreateSql())) {
                        return SqlProcessResult.msg(sqlRecTable.getCreateSql(), "create sql");
                    }
                }
                org.apache.hadoop.hive.metastore.api.Table hmsTable = db.getTable(dbName, table);
                return SqlProcessResult.msg(SchemaUtils.generateCreateSqlFromHmsTable(hmsTable), "create sql");
            }
        }

        if (sqlNode instanceof SqlShowSqlFunction) {
            List<SqlFunction> sqlFunctions = db.getSqlFunctionList();
            return SqlProcessResult.stringList(
                    sqlFunctions.stream().map(SqlFunction::getName).collect(Collectors.toList()),
                    "sql function"
            );
        }

        if (sqlNode instanceof SqlShowCreateSqlFunction) {
            SqlShowCreateSqlFunction showCreateSqlFunction = (SqlShowCreateSqlFunction) sqlNode;
            SqlFunction sqlFunction = db.getSqlFunction(showCreateSqlFunction.getFuncName().getSimple());
            if (sqlFunction == null) {
                throw new RuntimeException("sql function not exists: " + showCreateSqlFunction.getFuncName().getSimple());
            }
            List<String> sqlList = JsonUtils.parseStringList(sqlFunction.getSqlList());
            return SqlProcessResult.msg(String.join(";\n\n", sqlList) + ";", "create sql");
        }

        if (sqlNode instanceof SqlShowApi) {
            List<SqlApi> sqlApis = db.getSqlApiList();
            return SqlProcessResult.stringList(
                    sqlApis.stream().map(SqlApi::getName).collect(Collectors.toList()),
                    "api"
            );
        }

        if (sqlNode instanceof SqlShowCreateApi) {
            SqlShowCreateApi showCreateApi = (SqlShowCreateApi) sqlNode;
            SqlApi sqlApi = db.getSqlApi(showCreateApi.getApiName().getSimple());
            if (sqlApi == null) {
                throw new RuntimeException("api not exists: " + showCreateApi.getApiName());
            }
            String sql = "create api " + sqlApi.getName() + " with " + sqlApi.getFunctionName();
            return SqlProcessResult.msg(sql, "create sql");
        }

        if (sqlNode instanceof SqlShowModel) {
            List<Model> models = db.getModelList();
            return SqlProcessResult.stringList(
                    models.stream().map(Model::getName).collect(Collectors.toList()),
                    "model"
            );
        }

        if (sqlNode instanceof SqlShowCreateModel) {
            return processShowCreateModel((SqlShowCreateModel) sqlNode);
        }

        if (sqlNode instanceof SqlShowCheckpoint) {
            SqlShowCheckpoint showCheckpoint = (SqlShowCheckpoint) sqlNode;
            List<Checkpoint> checkpoints = db.getCheckpointListByModelName(showCheckpoint.getModelName().getSimple());
            return SqlProcessResult.stringList(
                    checkpoints.stream().map(Checkpoint::getCheckpointName).collect(Collectors.toList()),
                    "checkpoint"
            );
        }

        if (sqlNode instanceof SqlShowService) {
            List<Service> services = db.getServiceList();
            return SqlProcessResult.stringList(
                    services.stream().map(Service::getName).collect(Collectors.toList()),
                    "service"
            );
        }

        if (sqlNode instanceof SqlShowCreateService) {
            return processShowCreateService((SqlShowCreateService) sqlNode);
        }

        return null;
    }

    public static void saveSqlFunction(FunctionCompiler compiler) {
        MetadataAccess db = MetadataAccessFactory.getInstance();
        SqlFunction sqlFunction = new SqlFunction();
        sqlFunction.setName(compiler.getFunctionBindable().getFunName());
        sqlFunction.setSqlList(JsonUtils.toJson(compiler.getSqlList()));
        sqlFunction.setCreatedAt(System.currentTimeMillis());
        sqlFunction.setUpdatedAt(System.currentTimeMillis());
        if (compiler.isOrReplace()) {
            db.upsertSqlFunction(sqlFunction);
        } else {
            db.insertSqlFunction(sqlFunction);
        }
    }

    public static void saveSqlApi(SqlCreateApi api) {
        MetadataAccess db = MetadataAccessFactory.getInstance();
        SqlApi sqlApi = new SqlApi();
        sqlApi.setName(api.getApiName());
        sqlApi.setFunctionName(api.getFuncName());
        sqlApi.setCreatedAt(System.currentTimeMillis());
        sqlApi.setUpdatedAt(System.currentTimeMillis());
        if (api.isOrReplace()) {
            db.upsertSqlApi(sqlApi);
        } else {
            db.insertSqlApi(sqlApi);
        }
    }

    private SqlProcessResult processShowCreateModel(SqlShowCreateModel showCreateModel) throws Exception {
        MetadataAccess db = MetadataAccessFactory.getInstance();
        String modelName = showCreateModel.getModelName().getSimple();
        Model model = db.getModel(modelName);
        if (model == null) {
            throw new RuntimeException("model not exists: " + modelName);
        }

        Checkpoint checkpoint = null;
        if (showCreateModel.hasCheckpoint()) {
            String checkpointName = SchemaUtils.removeQuotes(showCreateModel.getCheckpoint().toString());
            checkpoint = db.getCheckpoint(modelName, checkpointName);
            if (checkpoint == null) {
                throw new RuntimeException("checkpoint not exists: " + checkpointName + " for model " + modelName);
            }
        }

        if (showCreateModel.isFormatted()) {
            java.util.List<java.util.List<String>> rows = new java.util.ArrayList<>();

            if (checkpoint != null) {
                ModelUtils.addModelInfo(rows, checkpoint.getModelDdl(), model);
                ModelUtils.addCheckpointInfo(rows, checkpoint);
            } else {
                ModelUtils.addModelInfo(rows, model);
            }

            Enumerable<Object[]> enumerable = com.sqlrec.common.utils.DataTransformUtils.convertListToArrayToEnumerable(rows);
            java.util.List<RelDataTypeField> fields = com.sqlrec.common.utils.DataTypeUtils.getStringTypeFieldList(
                    java.util.Arrays.asList("col_name", "data_type")
            );
            return SqlProcessResult.of(enumerable, fields);
        } else {
            if (checkpoint != null) {
                return SqlProcessResult.msg(checkpoint.getDdl(), "create sql");
            } else {
                return SqlProcessResult.msg(model.getDdl(), "create sql");
            }
        }
    }

    private SqlProcessResult processShowCreateService(SqlShowCreateService showCreateService) throws Exception {
        MetadataAccess db = MetadataAccessFactory.getInstance();
        String serviceName = showCreateService.getServiceName().getSimple();
        Service service = db.getService(serviceName);
        if (service == null) {
            throw new RuntimeException("service not exists: " + serviceName);
        }

        if (showCreateService.isFormatted()) {
            java.util.List<java.util.List<String>> rows = new java.util.ArrayList<>();
            ModelUtils.addServiceInfo(rows, service);

            Enumerable<Object[]> enumerable = com.sqlrec.common.utils.DataTransformUtils.convertListToArrayToEnumerable(rows);
            java.util.List<RelDataTypeField> fields = com.sqlrec.common.utils.DataTypeUtils.getStringTypeFieldList(
                    java.util.Arrays.asList("col_name", "data_type")
            );
            return SqlProcessResult.of(enumerable, fields);
        } else {
            return SqlProcessResult.msg(service.getDdl(), "create sql");
        }
    }
}
