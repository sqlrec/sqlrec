package com.sqlrec.frontend.rest;

import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.common.utils.HiveTableUtils;
import com.sqlrec.common.utils.JsonUtils;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.db.MetadataAccess;
import com.sqlrec.db.MetadataAccessFactory;
import com.sqlrec.entity.Checkpoint;
import com.sqlrec.entity.Model;
import com.sqlrec.entity.Service;
import com.sqlrec.entity.SqlApi;
import com.sqlrec.entity.SqlFunction;
import com.sqlrec.runtime.SqlFunctionBindable;
import com.sqlrec.utils.ModelUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

final class UiApiService {
    private final MetadataAccess metadataAccess;

    UiApiService() {
        this(MetadataAccessFactory.getInstance());
    }

    UiApiService(MetadataAccess metadataAccess) {
        this.metadataAccess = Objects.requireNonNull(metadataAccess, "metadataAccess");
    }

    List<Map<String, Object>> listFunctions() {
        return toItems(metadataAccess.getSqlFunctionList(), SqlFunction::getName);
    }

    List<Map<String, String>> getFunction(String name) {
        SqlFunction function = metadataAccess.getSqlFunction(name);
        return function == null ? null : convertFunctionToTable(function);
    }

    Map<String, Object> getFunctionDag(String functionName) throws Exception {
        SqlFunctionBindable sqlFunctionBindable = new CompileManager().getSqlFunction(functionName);
        if (sqlFunctionBindable == null) {
            return null;
        }

        return FunctionDagBuilder.build(functionName, sqlFunctionBindable);
    }

    List<Map<String, Object>> listDatabases() throws Exception {
        return toItems(metadataAccess.getDatabases(), Function.identity());
    }

    List<Map<String, Object>> listTables(String database) throws Exception {
        return metadataAccess.getTables(database).stream()
                .map(table -> {
                    Map<String, Object> item = createNamedItem(table.getTableName());
                    item.put("database", table.getDbName());
                    item.put("owner", table.getOwner());
                    item.put("tableType", table.getTableType());
                    item.put("createTime", formatTimestamp(table.getCreateTime() * 1000L));
                    return item;
                })
                .collect(Collectors.toList());
    }

    Map<String, Object> getTable(String database, String tableName) throws Exception {
        org.apache.hadoop.hive.metastore.api.Table table = metadataAccess.getTable(database, tableName);
        if (table == null) {
            return null;
        }

        List<Map<String, String>> rows = new ArrayList<>();
        List<FieldSchema> columns = HiveTableUtils.parse(table);
        rows.add(createRow("# Columns", ""));
        rows.add(createRow("Name", "Type"));
        for (FieldSchema column : columns) {
            rows.add(createRow(column.getName(), column.getType()));
        }
        rows.add(createRow("", ""));

        if (table.getPartitionKeys() != null && !table.getPartitionKeys().isEmpty()) {
            rows.add(createRow("# Partition Keys", ""));
            rows.add(createRow("Name", "Type"));
            for (org.apache.hadoop.hive.metastore.api.FieldSchema partitionKey : table.getPartitionKeys()) {
                rows.add(createRow(partitionKey.getName(), partitionKey.getType()));
            }
            rows.add(createRow("", ""));
        }

        return tableData(rows);
    }

    List<Map<String, Object>> listApis() {
        return toItems(metadataAccess.getSqlApiList(), SqlApi::getName);
    }

    Map<String, Object> getApi(String name) {
        SqlApi api = metadataAccess.getSqlApi(name);
        if (api == null) {
            return null;
        }

        List<Map<String, String>> rows = new ArrayList<>();
        rows.add(createRow("# API Information", ""));
        rows.add(createRow("API Name:", api.getName()));
        rows.add(createRow("Function Name:", api.getFunctionName()));
        rows.add(createRow("Created At:", formatTimestamp(api.getCreatedAt())));
        rows.add(createRow("Updated At:", formatTimestamp(api.getUpdatedAt())));

        return tableData(rows);
    }

    List<Map<String, Object>> listModels() {
        return toItems(metadataAccess.getModelList(), Model::getName);
    }

    Map<String, Object> getModel(String name) throws Exception {
        Model model = metadataAccess.getModel(name);
        if (model == null) {
            return null;
        }

        List<List<String>> rows = new ArrayList<>();
        ModelUtils.addModelInfo(rows, model);
        Map<String, Object> result = tableData(convertRowsToMap(rows));
        if (model.getDdl() != null) {
            result.put("ddl", model.getDdl());
        }
        return result;
    }

    Map<String, Object> listCheckpoints(String modelName, int page, int pageSize) {
        int total = metadataAccess.getCheckpointCountByModelName(modelName);
        List<Map<String, Object>> items = metadataAccess
                .getCheckpointListByModelNamePaged(modelName, page, pageSize)
                .stream()
                .map(checkpoint -> {
                    Map<String, Object> item = new HashMap<>();
                    item.put("checkpointName", checkpoint.getCheckpointName());
                    item.put("checkpointType", checkpoint.getCheckpointType());
                    item.put("status", checkpoint.getStatus());
                    item.put("createdAt", formatTimestamp(checkpoint.getCreatedAt()));
                    item.put("updatedAt", formatTimestamp(checkpoint.getUpdatedAt()));
                    return item;
                })
                .collect(Collectors.toList());

        Map<String, Object> result = new HashMap<>();
        result.put("items", items);
        result.put("total", total);
        result.put("page", page);
        result.put("pageSize", pageSize);
        result.put("totalPages", (int) Math.ceil((double) total / pageSize));
        return result;
    }

    Map<String, Object> getCheckpoint(String modelName, String checkpointName) {
        Checkpoint checkpoint = metadataAccess.getCheckpoint(modelName, checkpointName);
        if (checkpoint == null) {
            return null;
        }

        Map<String, Object> result = new HashMap<>();
        result.put("modelName", checkpoint.getModelName());
        result.put("checkpointName", checkpoint.getCheckpointName());
        result.put("checkpointType", checkpoint.getCheckpointType());
        result.put("status", checkpoint.getStatus());
        result.put("ddl", checkpoint.getDdl());
        result.put("modelDdl", checkpoint.getModelDdl());
        result.put("yaml", checkpoint.getYaml());
        result.put("createdAt", formatTimestamp(checkpoint.getCreatedAt()));
        result.put("updatedAt", formatTimestamp(checkpoint.getUpdatedAt()));
        return result;
    }

    List<Map<String, Object>> listServices() {
        return toItems(metadataAccess.getServiceList(), Service::getName);
    }

    Map<String, Object> getService(String name) throws Exception {
        Service service = metadataAccess.getService(name);
        if (service == null) {
            return null;
        }

        List<List<String>> rows = new ArrayList<>();
        ModelUtils.addServiceInfo(rows, service);
        Map<String, Object> result = tableData(convertRowsToMap(rows));
        if (service.getYaml() != null) {
            result.put("yaml", service.getYaml());
        }
        if (service.getDdl() != null) {
            result.put("ddl", service.getDdl());
        }
        return result;
    }

    private static <T> List<Map<String, Object>> toItems(List<T> values, Function<T, String> nameFunction) {
        return values.stream()
                .map(value -> {
                    String name = nameFunction.apply(value);
                    return createNamedItem(name);
                })
                .collect(Collectors.toList());
    }

    private static Map<String, Object> createNamedItem(String name) {
        Map<String, Object> item = new HashMap<>();
        item.put("id", name);
        item.put("name", name);
        return item;
    }

    private static Map<String, Object> tableData(List<?> rows) {
        Map<String, Object> result = new HashMap<>();
        result.put("tableData", rows);
        return result;
    }

    private static List<Map<String, String>> convertFunctionToTable(SqlFunction function) {
        List<Map<String, String>> rows = new ArrayList<>();
        rows.add(createRow("# Function Information", ""));
        rows.add(createRow("Function Name:", function.getName()));
        rows.add(createRow("Created At:", formatTimestamp(function.getCreatedAt())));
        rows.add(createRow("Updated At:", formatTimestamp(function.getUpdatedAt())));
        rows.add(createRow("", ""));
        rows.add(createRow("# SQL Statements", ""));
        List<String> sqlStatements = function.getSqlList() == null
                ? null : JsonUtils.parseStringList(function.getSqlList());
        if (sqlStatements == null || sqlStatements.isEmpty()) {
            rows.add(createRow("(none)", ""));
            return rows;
        }

        int statementNumber = 1;
        for (String statement : sqlStatements) {
            if (statement != null && !statement.isBlank()) {
                rows.add(createRow("SQL " + statementNumber++ + ":", statement.trim()));
            }
        }
        return rows;
    }

    private static List<Map<String, String>> convertRowsToMap(List<List<String>> rows) {
        return rows.stream()
                .map(row -> createRow(row.get(0), row.size() > 1 ? row.get(1) : ""))
                .collect(Collectors.toList());
    }

    private static Map<String, String> createRow(String columnName, String dataType) {
        Map<String, String> row = new HashMap<>();
        row.put("col_name", columnName);
        row.put("data_type", dataType);
        return row;
    }

    private static String formatTimestamp(long timestamp) {
        return new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                .format(new java.util.Date(timestamp));
    }
}
