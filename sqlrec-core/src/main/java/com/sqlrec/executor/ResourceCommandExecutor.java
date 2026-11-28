package com.sqlrec.executor;

import com.sqlrec.common.model.CheckpointInfo;
import com.sqlrec.compiler.FunctionCompiler;
import com.sqlrec.compiler.SqlApiCache;
import com.sqlrec.compiler.SqlFunctionCache;
import com.sqlrec.db.MetadataAccess;
import com.sqlrec.entity.SqlApi;
import com.sqlrec.entity.SqlFunction;
import com.sqlrec.model.ModelManager;
import com.sqlrec.model.ServiceManager;
import com.sqlrec.schema.CacheManager;
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
            saveSqlApi(command);
            SqlApiCache.invalidate(command.getApiName());
            return message("create api success");
        }
        if (node instanceof SqlDropApi command) {
            String apiName = command.getApiName().getSimple();
            dropApi(apiName, command.isIfExists());
            SqlApiCache.invalidate(apiName);
            return message("drop api success");
        }
        if (node instanceof SqlDropSqlFunction command) {
            dropSqlFunction(command);
            SqlFunctionCache.invalidateAll();
            return message("drop sql function success");
        }

        SqlProcessResult result = executeModelOrServiceCommand(node, defaultSchema);
        if (result != null) {
            CacheManager.invalidateAll();
        }
        return result;
    }

    private SqlProcessResult executeModelOrServiceCommand(SqlNode node, String defaultSchema) throws Exception {
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
        return null;
    }

    private SqlProcessResult dropCheckpoint(SqlAlterModelDropCheckpoint command) throws Exception {
        String modelName = command.getModelName().getSimple();
        String checkpointName = SchemaUtils.removeQuotes(command.getCheckpointName().toString());
        if (!ModelManager.deleteCheckpointIfPresent(metadata, modelName, checkpointName) && !command.isIfExists()) {
            throw new RuntimeException(
                    "checkpoint not exists: " + checkpointName + " for model " + modelName);
        }
        return message("drop checkpoint success");
    }

    private void dropSqlFunction(SqlDropSqlFunction command) {
        String name = command.getFuncName().getSimple();
        if (metadata.getSqlFunction(name) == null) {
            if (command.isIfExists()) {
                return;
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
    }

    private void dropApi(String name, boolean ifExists) {
        if (metadata.getSqlApi(name) == null) {
            if (ifExists) {
                return;
            }
            throw new RuntimeException("api not exists: " + name);
        }
        metadata.deleteSqlApi(name);
    }

    static void saveSqlFunction(MetadataAccess metadata, FunctionCompiler compiler) {
        SqlFunction function = SqlFunction.create(compiler.getFunctionBindable().getFunName(), compiler.getSqlList());
        if (compiler.isOrReplace()) {
            metadata.upsertSqlFunction(function);
        } else {
            metadata.insertSqlFunction(function);
        }
        SqlFunctionCache.invalidateAll();
    }

    private void saveSqlApi(SqlCreateApi command) {
        SqlApi api = SqlApi.create(command.getApiName(), command.getFuncName());
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
