package com.sqlrec.executor;

import com.sqlrec.common.model.CheckpointInfo;
import com.sqlrec.common.utils.JsonUtils;
import com.sqlrec.common.utils.ResourceNames;
import com.sqlrec.compiler.FunctionCompiler;
import com.sqlrec.compiler.SqlFunctionCache;
import com.sqlrec.db.MetadataAccess;
import com.sqlrec.entity.Checkpoint;
import com.sqlrec.entity.SqlApi;
import com.sqlrec.entity.SqlFunction;
import com.sqlrec.model.ModelManager;
import com.sqlrec.model.ServiceManager;
import com.sqlrec.sql.parser.*;
import com.sqlrec.utils.SchemaUtils;
import org.apache.calcite.sql.SqlNode;

import java.util.List;
import java.util.stream.Collectors;

/** Executes metadata-changing SQL commands. */
final class ResourceCommandExecutor {
    private static final String MESSAGE_FIELD = "msg";

    private final MetadataAccess metadata;

    ResourceCommandExecutor(MetadataAccess metadata) {
        this.metadata = metadata;
    }

    SqlProcessResult execute(SqlNode node, String defaultSchema) throws Exception {
        if (node instanceof SqlCreateApi command) {
            saveSqlApi(metadata, command);
            return message("create api success");
        }
        if (node instanceof SqlCreateModel command) {
            ModelManager.createModel(command);
            return message("create model success");
        }
        if (node instanceof SqlTrainModel command) {
            List<CheckpointInfo> checkpoints = ModelManager.trainModel(command, defaultSchema);
            return ModelSqlProcessResult.msg("train model success", MESSAGE_FIELD, checkpoints);
        }
        if (node instanceof SqlExportModel command) {
            List<CheckpointInfo> checkpoints = ModelManager.exportModel(command, defaultSchema);
            return ModelSqlProcessResult.msg("export model success", MESSAGE_FIELD, checkpoints);
        }
        if (node instanceof SqlDropModel command) {
            ModelManager.deleteModel(command.getModelName().getSimple());
            return message("drop model success");
        }
        if (node instanceof SqlAlterModelDropCheckpoint command) {
            return dropCheckpoint(command);
        }
        if (node instanceof SqlCreateService command) {
            String serviceName = ServiceManager.createService(command);
            return ServiceSqlProcessResult.msg(
                    "create service success", MESSAGE_FIELD, serviceName);
        }
        if (node instanceof SqlDropService command) {
            ServiceManager.deleteService(command.getServiceName().getSimple());
            return message("drop service success");
        }
        if (node instanceof SqlDropSqlFunction command) {
            return dropSqlFunction(command);
        }
        if (node instanceof SqlDropApi command) {
            return dropApi(command);
        }
        return null;
    }

    private SqlProcessResult dropCheckpoint(SqlAlterModelDropCheckpoint command) throws Exception {
        String modelName = command.getModelName().getSimple();
        String checkpointName = SchemaUtils.removeQuotes(command.getCheckpointName().toString());
        Checkpoint checkpoint = metadata.getCheckpoint(modelName, checkpointName);
        if (checkpoint == null) {
            if (command.isIfExists()) {
                return message("drop checkpoint success");
            }
            throw new RuntimeException(
                    "checkpoint not exists: " + checkpointName + " for model " + modelName);
        }
        ModelManager.deleteCheckpoint(modelName, checkpointName);
        return message("drop checkpoint success");
    }

    private SqlProcessResult dropSqlFunction(SqlDropSqlFunction command) {
        String name = command.getFuncName().getSimple();
        if (metadata.getSqlFunction(name) == null) {
            if (command.isIfExists()) {
                return message("drop sql function success");
            }
            throw new RuntimeException("sql function not exists: " + name);
        }
        List<String> usingApis = metadata.getSqlApiListByFunctionName(name).stream()
                .map(SqlApi::getName)
                .collect(Collectors.toList());
        if (!usingApis.isEmpty()) {
            throw new RuntimeException(
                    "sql function " + name + " is used by api: " + String.join(", ", usingApis));
        }
        metadata.deleteSqlFunction(name);
        return message("drop sql function success");
    }

    private SqlProcessResult dropApi(SqlDropApi command) {
        String name = command.getApiName().getSimple();
        if (metadata.getSqlApi(name) == null) {
            if (command.isIfExists()) {
                return message("drop api success");
            }
            throw new RuntimeException("api not exists: " + name);
        }
        metadata.deleteSqlApi(name);
        return message("drop api success");
    }

    static void saveSqlFunction(MetadataAccess metadata, FunctionCompiler compiler) {
        SqlFunction function = new SqlFunction();
        function.setName(compiler.getFunctionBindable().getFunName());
        function.setSqlList(JsonUtils.toJson(compiler.getSqlList()));
        function.setCreatedAt(System.currentTimeMillis());
        function.setUpdatedAt(System.currentTimeMillis());
        if (compiler.isOrReplace()) {
            metadata.upsertSqlFunction(function);
        } else {
            metadata.insertSqlFunction(function);
        }
        SqlFunctionCache.invalidateAll();
    }

    static void saveSqlApi(MetadataAccess metadata, SqlCreateApi command) {
        SqlApi api = new SqlApi();
        api.setName(ResourceNames.normalize(command.getApiName()));
        api.setFunctionName(ResourceNames.normalize(command.getFuncName()));
        api.setCreatedAt(System.currentTimeMillis());
        api.setUpdatedAt(System.currentTimeMillis());
        if (command.isOrReplace()) {
            metadata.upsertSqlApi(api);
        } else {
            metadata.insertSqlApi(api);
        }
    }

    private static SqlProcessResult message(String text) {
        return SqlProcessResult.msg(text, MESSAGE_FIELD);
    }
}
