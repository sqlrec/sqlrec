package com.sqlrec.executor;

import com.google.common.collect.ImmutableList;
import com.sqlrec.common.schema.SqlRecTable;
import com.sqlrec.common.utils.DataTransformUtils;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.common.utils.JsonUtils;
import com.sqlrec.db.MetadataAccess;
import com.sqlrec.entity.*;
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
import org.apache.flink.sql.parser.dql.*;

import com.sqlrec.sql.parser.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/** Executes metadata-reading SQL commands and formats their result tables. */
final class ResourceQueryExecutor {
    private final MetadataAccess metadata;
    private final CalciteSchema schema;

    ResourceQueryExecutor(MetadataAccess metadata, CalciteSchema schema) {
        this.metadata = metadata;
        this.schema = schema;
    }

    SqlProcessResult execute(SqlNode node, String defaultSchema) throws Exception {
        if (node instanceof SqlShowDatabases) {
            return SqlProcessResult.stringList(metadata.getDatabases(), "database name");
        }
        if (node instanceof SqlShowTables command) {
            return showTables(command, defaultSchema);
        }
        if (node instanceof SqlRichDescribeTable command) {
            return describeTable(command, defaultSchema);
        }
        if (node instanceof SqlShowCreateTable command) {
            return showCreateTable(command, defaultSchema);
        }
        if (node instanceof SqlShowSqlFunction) {
            return SqlProcessResult.stringList(
                    metadata.getSqlFunctionList().stream()
                            .map(SqlFunction::getName).collect(Collectors.toList()),
                    "sql function");
        }
        if (node instanceof SqlShowCreateSqlFunction command) {
            SqlFunction function = metadata.getSqlFunction(command.getFuncName().getSimple());
            if (function == null) {
                throw new RuntimeException(
                        "sql function not exists: " + command.getFuncName().getSimple());
            }
            return SqlProcessResult.msg(
                    String.join(";\n\n", JsonUtils.parseStringList(function.getSqlList())) + ";",
                    "create sql");
        }
        if (node instanceof SqlShowApi) {
            return SqlProcessResult.stringList(
                    metadata.getSqlApiList().stream()
                            .map(SqlApi::getName).collect(Collectors.toList()), "api");
        }
        if (node instanceof SqlShowCreateApi command) {
            SqlApi api = metadata.getSqlApi(command.getApiName().getSimple());
            if (api == null) {
                throw new RuntimeException("api not exists: " + command.getApiName());
            }
            return SqlProcessResult.msg(
                    "create api " + api.getName() + " with " + api.getFunctionName(), "create sql");
        }
        if (node instanceof SqlShowModel) {
            return SqlProcessResult.stringList(
                    metadata.getModelList().stream()
                            .map(Model::getName).collect(Collectors.toList()), "model");
        }
        if (node instanceof SqlShowCreateModel command) {
            return showCreateModel(command);
        }
        if (node instanceof SqlShowCheckpoint command) {
            return SqlProcessResult.stringList(
                    metadata.getCheckpointListByModelName(command.getModelName().getSimple()).stream()
                            .map(Checkpoint::getCheckpointName).collect(Collectors.toList()),
                    "checkpoint");
        }
        if (node instanceof SqlShowService) {
            return SqlProcessResult.stringList(
                    metadata.getServiceList().stream()
                            .map(Service::getName).collect(Collectors.toList()), "service");
        }
        if (node instanceof SqlShowCreateService command) {
            return showCreateService(command);
        }
        return null;
    }

    private SqlProcessResult showTables(SqlShowTables command, String defaultSchema) throws Exception {
        String database = command.fullDatabaseName().length == 0
                ? defaultSchema : command.fullDatabaseName()[0];
        if (schema.getSubSchema(database, false) == null) {
            throw new RuntimeException("database not exists: " + database);
        }
        List<String> names = metadata.getTables(database).stream()
                .map(org.apache.hadoop.hive.metastore.api.Table::getTableName)
                .collect(Collectors.toList());
        if (defaultSchema.equalsIgnoreCase(database)) {
            names.addAll(schema.getTableNames());
        }
        return SqlProcessResult.stringList(
                names.stream().distinct().collect(Collectors.toList()), "table name");
    }

    private SqlProcessResult describeTable(SqlRichDescribeTable command, String defaultSchema) {
        String[] name = command.fullTableName();
        String database = name.length > 1 ? name[0] : defaultSchema;
        Table table = SchemaUtils.getTableObj(schema, database, name[name.length - 1]);
        if (table == null) {
            return null;
        }
        RelDataType rowType = table.getRowType(
                new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT));
        return SqlProcessResult.tableTypeDesc(rowType.getFieldList());
    }

    private SqlProcessResult showCreateTable(SqlShowCreateTable command, String defaultSchema)
            throws Exception {
        ImmutableList<String> names = command.getTableName().names;
        String database = names.size() > 1 ? names.get(0) : defaultSchema;
        String tableName = names.get(names.size() - 1);
        Table table = SchemaUtils.getTableObj(schema, database, tableName);
        if (table == null) {
            return null;
        }
        if (table instanceof SqlRecTable sqlRecTable
                && StringUtils.isNotEmpty(sqlRecTable.getCreateSql())) {
            return SqlProcessResult.msg(sqlRecTable.getCreateSql(), "create sql");
        }
        return SqlProcessResult.msg(
                SchemaUtils.generateCreateSqlFromHmsTable(
                        metadata.getTable(database, tableName)),
                "create sql");
    }

    private SqlProcessResult showCreateModel(SqlShowCreateModel command) throws Exception {
        String modelName = command.getModelName().getSimple();
        Model model = metadata.getModel(modelName);
        if (model == null) {
            throw new RuntimeException("model not exists: " + modelName);
        }
        Checkpoint checkpoint = null;
        if (command.hasCheckpoint()) {
            String name = SchemaUtils.removeQuotes(command.getCheckpoint().toString());
            checkpoint = metadata.getCheckpoint(modelName, name);
            if (checkpoint == null) {
                throw new RuntimeException(
                        "checkpoint not exists: " + name + " for model " + modelName);
            }
        }
        if (!command.isFormatted()) {
            return SqlProcessResult.msg(
                    checkpoint == null ? model.getDdl() : checkpoint.getDdl(), "create sql");
        }
        List<List<String>> rows = new ArrayList<>();
        if (checkpoint == null) {
            ModelUtils.addModelInfo(rows, model);
        } else {
            ModelUtils.addModelInfo(rows, checkpoint.getModelDdl(), model);
            ModelUtils.addCheckpointInfo(rows, checkpoint);
        }
        return formatted(rows);
    }

    private SqlProcessResult showCreateService(SqlShowCreateService command) throws Exception {
        String name = command.getServiceName().getSimple();
        Service service = metadata.getService(name);
        if (service == null) {
            throw new RuntimeException("service not exists: " + name);
        }
        if (!command.isFormatted()) {
            return SqlProcessResult.msg(service.getDdl(), "create sql");
        }
        List<List<String>> rows = new ArrayList<>();
        ModelUtils.addServiceInfo(rows, service);
        return formatted(rows);
    }

    private static SqlProcessResult formatted(List<List<String>> rows) {
        Enumerable<Object[]> values = DataTransformUtils.convertListToArrayToEnumerable(rows);
        List<RelDataTypeField> fields = DataTypeUtils.getStringTypeFieldList(
                Arrays.asList("col_name", "data_type"));
        return SqlProcessResult.of(values, fields);
    }
}
